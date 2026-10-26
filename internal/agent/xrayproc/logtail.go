package xrayproc

import (
	"bytes"
	"strings"
	"sync"
)

// 内存日志尾部（2026-10-26）：主控 get_logs 与本地排查直接从这里取，不再每次读盘。
// 只保留最近 memTailLines 行——日志时效性强，面板一次最多看几百行，留多了纯占内存。
const (
	memTailLines    = 500
	memTailMaxBytes = 1 << 20 // 字节兜底：短行再多也不让内存无界
	memTailMaxLine  = 8 << 10 // 单行上限（超长行截断，防一行打爆内存）
)

// logTail 行级环形缓冲，实现 io.Writer：接收 xray 的 stdout/stderr 原始分片
// （分片边界与行边界无关，跨分片的残行留到下次补齐）。
type logTail struct {
	mu      sync.Mutex
	lines   []string
	bytes   int
	partial []byte
}

func newLogTail() *logTail {
	return &logTail{lines: make([]string, 0, memTailLines)}
}

func (t *logTail) Write(p []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	data := p
	if len(t.partial) > 0 {
		merged := make([]byte, 0, len(t.partial)+len(p))
		merged = append(merged, t.partial...)
		data = append(merged, p...)
		t.partial = nil
	}
	cut := bytes.LastIndexByte(data, '\n')
	if cut < 0 {
		t.partial = append(t.partial, data...)
		if len(t.partial) > memTailMaxLine {
			t.partial = t.partial[len(t.partial)-memTailMaxLine:]
		}
		return len(p), nil
	}
	// 末尾残行留到下次；注意 t.partial 可能复用 data 的底层数组，需拷贝。
	t.partial = append(t.partial[:0], data[cut+1:]...)
	for _, line := range strings.Split(string(data[:cut]), "\n") {
		t.pushLocked(line)
	}
	return len(p), nil
}

// Seed 用磁盘上已有的日志填充缓冲（agent 重启后首次启动 xray 时，
// 让面板看到的仍是「最近的 500 行」而不是从零开始）。已有内容时忽略。
func (t *logTail) Seed(text string) {
	if strings.TrimSpace(text) == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.lines) > 0 {
		return
	}
	for _, line := range strings.Split(text, "\n") {
		t.pushLocked(line)
	}
}

// Tail 返回最近 n 行；ok=false 表示缓冲还是空的（调用方回退读文件）。
func (t *logTail) Tail(n int) (string, bool) {
	if n <= 0 {
		n = 100
	}
	if n > memTailLines {
		n = memTailLines
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	lines := t.lines
	if len(t.partial) > 0 {
		// 还没换行的尾巴（如进度输出）也算最后一行，否则面板会"少最后一条"。
		lines = append(append([]string(nil), lines...), string(t.partial))
	}
	if len(lines) == 0 {
		return "", false
	}
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n"), true
}

// Empty 缓冲是否为空（尚未喂入任何内容）。
func (t *logTail) Empty() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.lines) == 0 && len(t.partial) == 0
}

func (t *logTail) pushLocked(line string) {
	line = strings.TrimRight(line, "\r")
	if len(line) > memTailMaxLine {
		line = "…" + line[len(line)-memTailMaxLine:]
	}
	t.lines = append(t.lines, line)
	t.bytes += len(line)
	for len(t.lines) > memTailLines || (t.bytes > memTailMaxBytes && len(t.lines) > 1) {
		t.bytes -= len(t.lines[0])
		t.lines = t.lines[1:]
	}
}
