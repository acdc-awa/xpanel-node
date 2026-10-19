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
	_ = zw.Close()
	zipData := buf.Bytes()
	sum := Sha256Hex(zipData)
	asset, err := XrayAssetName()
	if err != nil {
		t.Skip(err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/o/x/releases/latest", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", "/o/x/releases/tag/v26.10.17")
		w.WriteHeader(http.StatusFound)
	})
	mux.HandleFunc("/o/x/releases/download/v26.10.17/"+asset, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(zipData)
	})
	mux.HandleFunc("/o/x/releases/download/v26.10.17/"+asset+".dgst", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "MD5= abc\nSHA1= def\nSHA2-256= %s\nSHA2-512= fed\n", sum)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	f := NewXrayFetcher("", 0)
	f.Mirrors = []string{srv.URL}
	f.Repo = "o/x"

	tag, err := f.Latest()
	if err != nil || tag != "v26.10.17" {
		t.Fatalf("Latest = %q, %v", tag, err)
	}
	bin, got, err := f.XrayDownload(tag)
	if err != nil {
		t.Fatalf("XrayDownload: %v", err)
	}
	if string(bin) != "FAKE-XRAY-BIN" {
		t.Errorf("bin = %q", bin)
	}
	if !strings.EqualFold(got, sum) {
		t.Errorf("sha = %s, want %s", got, sum)
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
	mux.HandleFunc("/o/x/releases/download/v26.10.17/"+asset, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(zipData)
	})
	mux.HandleFunc("/o/x/releases/download/v26.10.17/"+asset+".dgst", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "SHA2-256= %s\n", strings.Repeat("a", 64))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	f := NewXrayFetcher("", 0)
	f.Mirrors = []string{srv.URL}
	f.Repo = "o/x"
	if _, _, err := f.XrayDownload("v26.10.17"); err == nil {
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
	mux.HandleFunc("/o/x/releases/download/v26.10.17/"+asset, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("x"))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	f := NewXrayFetcher("", 0)
	f.Mirrors = []string{srv.URL}
	f.Repo = "o/x"
	if _, _, err := f.XrayDownload("v26.10.17"); err == nil {
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
