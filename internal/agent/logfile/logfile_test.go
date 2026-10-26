package logfile

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestSlidingKeepsTailAcrossLimit(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "xray.log")
	const limit = 64 << 10 // 64KB
	w, err := Open(path, limit)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	// 写入 300KB（远超上限）：每次都应当是「保留尾部一半 + 本次写入」，最终体积有界。
	line := strings.Repeat("x", 99) + "\n" // 100B
	for i := 0; i < 3000; i++ {
		if _, err := w.Write([]byte(line)); err != nil {
			t.Fatal(err)
		}
	}
	if size := Size(path); size > limit {
		t.Fatalf("滑动后文件体积应不超过上限 %d，实际 %d", limit, size)
	}
	if w.Size() != Size(path) {
		t.Fatalf("内部记录体积 %d 与磁盘 %d 不一致", w.Size(), Size(path))
	}
}

func TestSlidingOpenTrimsOversizeFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "xray.log")
	// 预置 1MB，Open 时应立即收口到上限的一半以内。
	if err := os.WriteFile(path, []byte(strings.Repeat("a\n", 512<<10)), 0o644); err != nil {
		t.Fatal(err)
	}
	w, err := Open(path, 64<<10)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if size := w.Size(); size > 64<<10 {
		t.Fatalf("Open 应裁剪存量文件，实际 %d", size)
	}
}

func TestVacuumFreesSpace(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "xray.log")
	content := strings.Repeat("line\n", 100000) // 500KB
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	freed, err := SlideNow(path, 64<<10)
	if err != nil {
		t.Fatal(err)
	}
	if freed <= 0 {
		t.Fatalf("应释放空间，实际 %d", freed)
	}
	if size := Size(path); size > 64<<10 {
		t.Fatalf("裁剪后应不超过上限，实际 %d", size)
	}
	// 行首对齐：保留部分的每一行都应是完整的 "line"。
	data, _ := os.ReadFile(path)
	for _, l := range strings.Split(strings.TrimRight(string(data), "\n"), "\n") {
		if l != "line" {
			t.Fatalf("保留内容应为整行，实际首行 %q", l)
		}
	}
}

func TestTailReadsLastLines(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "xray.log")
	var sb strings.Builder
	for i := 0; i < 2000; i++ {
		sb.WriteString("line-")
		sb.WriteString(strings.Repeat("0", 3))
		sb.WriteString("\n")
	}
	sb.WriteString("last-1\nlast-2\n")
	if err := os.WriteFile(path, []byte(sb.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	out, err := Tail(path, 2)
	if err != nil {
		t.Fatal(err)
	}
	if out != "last-1\nlast-2" {
		t.Fatalf("Tail(2) 应取最后两行，实际 %q", out)
	}
	out, err = Tail(path, 2000)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(out, "last-2") {
		t.Fatalf("大 n 时应回溯到文件头，实际尾部 %q", out)
	}
}

func TestSlideNowDisabled(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "xray.log")
	if err := os.WriteFile(path, []byte("x\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := SlideNow(path, 0); err == nil {
		t.Fatal("上限 0 应返回错误")
	}
	if size := Size(path); size != 2 {
		t.Fatalf("上限关闭时不应改动文件，实际 %d", size)
	}
}

// 外部把文件换掉（手工删除后重建 / 外部轮转）时，必须重新打开而不是继续写旧 inode。
// Windows 不允许删除仍被打开的文件，这条只在 POSIX 有意义。
func TestWriteReopensAfterExternalReplace(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows 无法删除被打开的文件：该场景只在 Linux 上成立")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "xray.log")
	w, err := Open(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if _, err := w.Write([]byte("before\n")); err != nil {
		t.Fatal(err)
	}
	// 外部删掉并用新文件顶替：inode 变了，继续写旧 fd 会让日志落进已删除的文件。
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("fresh\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	w.checkedAt = w.checkedAt.Add(-2 * externalCheckInterval) // 跳过探测间隔
	if _, err := w.Write([]byte("after\n")); err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(path)
	if string(data) != "fresh\nafter\n" {
		t.Fatalf("外部替换文件后应写到新文件，实际 %q", string(data))
	}
}
