package upgrade

import (
	"archive/zip"
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// TestXrayDownloadEndToEnd 用模拟 GitHub 验证「下载→.dgst 校验→解包」全链路。
func TestXrayDownloadEndToEnd(t *testing.T) {
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	w, _ := zw.Create("xray")
	_, _ = w.Write([]byte("FAKE-XRAY-BIN"))
	gw, _ := zw.Create("geoip.dat")
	_, _ = gw.Write([]byte("FAKE-GEOIP"))
	sw, _ := zw.Create("geosite.dat")
	_, _ = sw.Write([]byte("FAKE-GEOSITE"))
	_ = zw.Close()
	zipData := buf.Bytes()
	sum := Sha256Hex(zipData)
	asset, err := XrayAssetName()
	if err != nil {
		t.Skip(err)
	}

	mux := http.NewServeMux()
	// XrayFetcher.RequireAtom=true：模拟 GitHub 的 releases.atom（含 prerelease）。
	mux.HandleFunc("/o/x/releases.atom", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/atom+xml")
		fmt.Fprint(w, `<?xml version="1.0" encoding="utf-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <entry><link rel="alternate" type="text/html" href="https://github.com/o/x/releases/tag/v26.10.21"/></entry>
</feed>`)
	})
	mux.HandleFunc("/o/x/releases/latest", func(w http.ResponseWriter, r *http.Request) {
		// 故意指向旧正式版：RequireAtom 下不应被采用
		w.Header().Set("Location", "/o/x/releases/tag/v26.3.27")
		w.WriteHeader(http.StatusFound)
	})
	mux.HandleFunc("/o/x/releases/download/v26.10.21/"+asset, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(zipData)
	})
	mux.HandleFunc("/o/x/releases/download/v26.10.21/"+asset+".dgst", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "MD5= abc\nSHA1= def\nSHA2-256= %s\nSHA2-512= fed\n", sum)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	f := NewXrayFetcher("", 0)
	f.Mirrors = []string{srv.URL}
	f.Repo = "o/x"

	tag, err := f.Latest()
	if err != nil || tag != "v26.10.21" {
		t.Fatalf("Latest = %q, %v", tag, err)
	}
	bundle, err := f.XrayDownloadBundle(tag)
	if err != nil {
		t.Fatalf("XrayDownloadBundle: %v", err)
	}
	if string(bundle.Bin) != "FAKE-XRAY-BIN" {
		t.Errorf("bin = %q", bundle.Bin)
	}
	if got := string(bundle.Geo["geoip.dat"]); got != "FAKE-GEOIP" {
		t.Errorf("geoip = %q", got)
	}
	if got := string(bundle.Geo["geosite.dat"]); got != "FAKE-GEOSITE" {
		t.Errorf("geosite = %q", got)
	}
	if !strings.EqualFold(bundle.SHA256, sum) {
		t.Errorf("sha = %s, want %s", bundle.SHA256, sum)
	}
}

// TestXrayDownloadRejectsBadDigest 摘要不符必须拒绝（不能装上一个来源不可信的二进制）。
func TestXrayDownloadRejectsBadDigest(t *testing.T) {
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	w, _ := zw.Create("xray")
	_, _ = w.Write([]byte("BIN"))
	_ = zw.Close()
	zipData := buf.Bytes()
	asset, err := XrayAssetName()
	if err != nil {
		t.Skip(err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/o/x/releases/download/v26.10.21/"+asset, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(zipData)
	})
	mux.HandleFunc("/o/x/releases/download/v26.10.21/"+asset+".dgst", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "SHA2-256= %s\n", strings.Repeat("a", 64))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	f := NewXrayFetcher("", 0)
	f.Mirrors = []string{srv.URL}
	f.Repo = "o/x"
	if _, err := f.XrayDownloadBundle("v26.10.21"); err == nil {
		t.Fatal("摘要不符应当拒绝")
	}
}

// TestXrayDownloadNoDigest 缺 .dgst 必须拒绝。
func TestXrayDownloadNoDigest(t *testing.T) {
	asset, err := XrayAssetName()
	if err != nil {
		t.Skip(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/o/x/releases/download/v26.10.21/"+asset, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("x"))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	f := NewXrayFetcher("", 0)
	f.Mirrors = []string{srv.URL}
	f.Repo = "o/x"
	if _, err := f.XrayDownloadBundle("v26.10.21"); err == nil {
		t.Fatal("缺 .dgst 应当拒绝")
	}
}

func TestReplaceXrayBinaryPreservesMode(t *testing.T) {
	if runtime.GOOS == "windows" {
		// Windows 没有 POSIX 权限位，chmod 不反映到 Mode().Perm()；该断言只在 Unix 有意义
		t.Skip("Windows 下不校验可执行权限位")
	}
	dir := t.TempDir()
	bin := filepath.Join(dir, "xray")
	_ = os.WriteFile(bin, []byte("OLD"), 0o755)
	if _, err := ReplaceXrayBinary(bin, []byte("NEW")); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(bin)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm()&0o111 == 0 {
		t.Errorf("替换后可执行位丢失: %v", info.Mode())
	}
}
