package fsutil

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCopyFile(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src.bin")
	dst := filepath.Join(dir, "dst.bin")
	if err := os.WriteFile(src, []byte("v1"), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := CopyFile(src, dst, 0o644); err != nil {
		t.Fatalf("首次复制失败: %v", err)
	}
	if got, _ := os.ReadFile(dst); string(got) != "v1" {
		t.Fatalf("内容不符: %q", got)
	}
	if _, err := os.Stat(dst + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf("临时文件未清理: %v", err)
	}

	// 覆盖已有目标：内容整体替换，不残留旧内容。
	if err := os.WriteFile(src, []byte("v2-longer"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := CopyFile(src, dst, 0o644); err != nil {
		t.Fatalf("覆盖复制失败: %v", err)
	}
	if got, _ := os.ReadFile(dst); string(got) != "v2-longer" {
		t.Fatalf("覆盖后内容不符: %q", got)
	}
}

func TestCopyFileSourceMissing(t *testing.T) {
	dir := t.TempDir()
	dst := filepath.Join(dir, "dst.bin")
	if err := CopyFile(filepath.Join(dir, "nope"), dst, 0o644); err == nil {
		t.Fatal("源文件不存在时应返回错误")
	}
	if _, err := os.Stat(dst); !os.IsNotExist(err) {
		t.Fatalf("失败时不应创建目标文件: %v", err)
	}
}
