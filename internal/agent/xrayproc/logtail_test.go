package xrayproc

import (
	"strings"
	"testing"
)

// 分片边界与行边界无关：残行必须留到下次补齐，不能把半行当一条日志。
func TestLogTailSplitsAcrossChunks(t *testing.T) {
	tail := newLogTail()
	_, _ = tail.Write([]byte("hello wo"))
	_, _ = tail.Write([]byte("rld\n第二行\n第三"))
	if got, ok := tail.Tail(10); !ok || !strings.Contains(got, "hello world") {
		t.Fatalf("跨分片残行未补齐: %q ok=%v", got, ok)
	}
	_, _ = tail.Write([]byte("行\n"))
	got, _ := tail.Tail(1)
	if got != "第三行" {
		t.Fatalf("最后一行应为 第三行，实际 %q", got)
	}
}

// 只保留最近 memTailLines 行（面板一次最多看 500 行）。
func TestLogTailCapsLines(t *testing.T) {
	tail := newLogTail()
	for i := 0; i < memTailLines+100; i++ {
		_, _ = tail.Write([]byte("line\n"))
	}
	got, ok := tail.Tail(memTailLines * 2)
	if !ok {
		t.Fatal("缓冲应有内容")
	}
	if n := len(strings.Split(got, "\n")); n != memTailLines {
		t.Fatalf("应保留 %d 行，实际 %d", memTailLines, n)
	}
}

// 重启后按磁盘历史播种：面板看到的仍是「最近 500 行」而不是从零开始。
func TestLogTailSeedOnlyWhenEmpty(t *testing.T) {
	tail := newLogTail()
	tail.Seed("old-1\nold-2")
	got, _ := tail.Tail(10)
	if got != "old-1\nold-2" {
		t.Fatalf("播种结果不符: %q", got)
	}
	tail.Seed("ignored")
	got, _ = tail.Tail(10)
	if strings.Contains(got, "ignored") {
		t.Fatalf("已有内容时不应再播种: %q", got)
	}
}
