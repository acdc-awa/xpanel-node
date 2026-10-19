package upgrade

import (
	"archive/zip"
	"bytes"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestParseDigestSHA256(t *testing.T) {
	const hex = "ecdd54eccd24c4c21a8f00968e7dbf7f5996da92383d2321170b560dc4a95158"
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"v26 键值格式", "MD5= c6e0ddc8\nSHA1= 60fe60dd\nSHA2-256= " + hex + "\nSHA2-512= 2ce15a\n", hex},
		{"BSD 格式", "SHA256 (Xray-linux-64.zip) = " + hex + "\n", hex},
		{"sha256sum 格式", hex + "  Xray-linux-64.zip\n", hex},
		{"大写 hex", "SHA2-256= " + strings.ToUpper(hex) + "\n", hex},
		{"大写字样", "SHA2-256= " + hex + "\n", hex},
		{"只有 md5", "MD5= c6e0ddc88fd331872d8b8b7ac137cec1\n", ""},
		{"空", "", ""},
		{"垃圾", "not a digest at all", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := parseDigestSHA256([]byte(c.in)); got != c.want {
				t.Errorf("parseDigestSHA256() = %q, want %q", got, c.want)
			}
		})
	}
}

func TestXrayVersionOf(t *testing.T) {
	cases := []struct{ in, want string }{
		{"Xray 26.10.17 (Xray, Penetrates Everything.) 92ccc93 (go1.27.2 windows/amd64)", "26.10.17"},
		{"Xray 26.6.27 (Xray, Penetrates Everything.) 45cf289 (go1.26.4 linux/amd64)", "26.6.27"},
		{"garbage", ""},
		{"", ""},
	}
	for _, c := range cases {
		if got := XrayVersionOf(c.in); got != c.want {
			t.Errorf("XrayVersionOf(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestExtractFileFromZip(t *testing.T) {
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	w, err := zw.Create("xray")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("FAKEBIN")); err != nil {
		t.Fatal(err)
	}
	if _, err := zw.Create("geoip.dat"); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}

	got, err := extractFileFromZip(buf.Bytes(), "xray")
	if err != nil {
		t.Fatalf("extractFileFromZip: %v", err)
	}
	if string(got) != "FAKEBIN" {
		t.Errorf("got %q, want FAKEBIN", got)
	}

	if _, err := extractFileFromZip(buf.Bytes(), "missing"); err == nil {
		t.Error("期望未找到文件时报错")
	}
}

func TestExtractFileFromZipExe(t *testing.T) {
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	w, _ := zw.Create("xray.exe")
	_, _ = w.Write([]byte("WINBIN"))
	_ = zw.Close()

	got, err := extractFileFromZip(buf.Bytes(), "xray")
	if err != nil {
		t.Fatalf("extractFileFromZip: %v", err)
	}
	if string(got) != "WINBIN" {
		t.Errorf("got %q, want WINBIN", got)
	}
}

func TestReplaceXrayBinaryKeepsPrev(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(dir, "xray")
	if err := os.WriteFile(bin, []byte("OLD"), 0o755); err != nil {
		t.Fatal(err)
	}
	prev, err := ReplaceXrayBinary(bin, []byte("NEW"))
	if err != nil {
		t.Fatalf("ReplaceXrayBinary: %v", err)
	}
	if got, _ := os.ReadFile(bin); string(got) != "NEW" {
		t.Errorf("bin = %q, want NEW", got)
	}
	if got, _ := os.ReadFile(prev); string(got) != "OLD" {
		t.Errorf("prev = %q, want OLD", got)
	}

	if err := RollbackXrayBinary(bin, prev); err != nil {
		t.Fatalf("RollbackXrayBinary: %v", err)
	}
	if got, _ := os.ReadFile(bin); string(got) != "OLD" {
		t.Errorf("回滚后 bin = %q, want OLD", got)
	}
}

func TestReplaceXrayBinaryFirstInstall(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(dir, "xray") // 不存在
	prev, err := ReplaceXrayBinary(bin, []byte("NEW"))
	if err != nil {
		t.Fatalf("首次安装应当成功: %v", err)
	}
	if _, err := os.Stat(prev); !os.IsNotExist(err) {
		t.Error("首次安装不应产生 .prev 备份")
	}
	if got, _ := os.ReadFile(bin); string(got) != "NEW" {
		t.Errorf("bin = %q", got)
	}
}

// VerifyXrayBinary 需要一个真的可执行文件；用 shell 脚本模拟 `version` 输出。
func TestVerifyXrayBinaryRejectsBadBinary(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows 下无法用 shell 脚本模拟可执行文件")
	}
	err := VerifyXrayBinary([]byte("not an executable"), "26.10.17")
	if err == nil {
		t.Fatal("期望不可执行的二进制被拒绝")
	}
	if !IsProbeError(err) {
		t.Errorf("期望 probeError，实际 %T", err)
	}
}

func TestVerifyXrayBinaryVersionMismatch(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows 下无法用 shell 脚本模拟可执行文件")
	}
	script := "#!/bin/sh\nif [ \"$1\" = version ]; then echo 'Xray 26.9.8 (Xray, Penetrates Everything.) abc'; fi\n"
	if err := VerifyXrayBinary([]byte(script), "26.10.17"); err == nil {
		t.Fatal("版本不符应当被拒绝")
	}
	if err := VerifyXrayBinary([]byte(script), "26.9.8"); err != nil {
		t.Fatalf("版本相符应当通过: %v", err)
	}
}

func TestXrayAssetName(t *testing.T) {
	name, err := XrayAssetName()
	if err != nil {
		t.Skipf("当前平台 %s/%s 不受支持: %v", runtime.GOOS, runtime.GOARCH, err)
	}
	if !strings.HasPrefix(name, "Xray-") || !strings.HasSuffix(name, ".zip") {
		t.Errorf("资产名不符合预期: %s", name)
	}
	if runtime.GOARCH == "amd64" && runtime.GOOS == "linux" && name != "Xray-linux-64.zip" {
		t.Errorf("linux/amd64 资产名 = %s", name)
	}
}
