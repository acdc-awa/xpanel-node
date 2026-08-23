package upgrade

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestApplyUpgradesBinary(t *testing.T) {
	dir := t.TempDir()
	exePath := filepath.Join(dir, "xray-agent")
	if err := os.WriteFile(exePath, []byte("old-binary"), 0o755); err != nil {
		t.Fatal(err)
	}
	_, f := newFakeGitHub(t, fakeRelease{Tag: "v9.9.9", Data: []byte("new-binary")})

	restarted := false
	var out bytes.Buffer
	err := Apply(f, exePath, func() error { restarted = true; return nil }, &out)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := os.ReadFile(exePath)
	if string(got) != "new-binary" {
		t.Errorf("替换后内容 = %q", got)
	}
	if !restarted {
		t.Error("未调用 restart")
	}
	if !strings.Contains(out.String(), "v9.9.9") {
		t.Errorf("输出应含新版本号: %q", out.String())
	}
}

func TestApplySameVersionNoop(t *testing.T) {
	dir := t.TempDir()
	exePath := filepath.Join(dir, "xray-agent")
	if err := os.WriteFile(exePath, []byte("old"), 0o755); err != nil {
		t.Fatal(err)
	}
	// 与本地相同版本（dev）：不重下不重启
	_, f := newFakeGitHub(t, fakeRelease{Tag: CurrentVersion(), Data: []byte("x")})

	err := Apply(f, exePath, func() error { t.Error("不应重启"); return nil }, &bytes.Buffer{})
	if !errors.Is(err, ErrUpToDate) {
		t.Fatalf("err = %v, want ErrUpToDate", err)
	}
	got, _ := os.ReadFile(exePath)
	if string(got) != "old" {
		t.Error("版本相同不应替换")
	}
}

func TestApplyShaMismatchRejects(t *testing.T) {
	dir := t.TempDir()
	exePath := filepath.Join(dir, "xray-agent")
	if err := os.WriteFile(exePath, []byte("old"), 0o755); err != nil {
		t.Fatal(err)
	}
	_, f := newFakeGitHub(t, fakeRelease{Tag: "v2.0.0", Data: []byte("corrupt"), BadSums: true})

	err := Apply(f, exePath, func() error { t.Error("不应重启"); return nil }, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "sha256") {
		t.Fatalf("err = %v, want sha256 错误", err)
	}
	got, _ := os.ReadFile(exePath)
	if string(got) != "old" {
		t.Error("校验失败不应触碰现有二进制")
	}
	// 临时文件应被清理
	if _, err := os.Stat(filepath.Join(dir, "xray-agent.tmp")); err == nil {
		t.Error("临时文件未清理: xray-agent.tmp 仍存在")
	}
}
