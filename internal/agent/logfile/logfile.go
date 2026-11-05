// Package logfile 提供节点侧的日志文件写入：按大小滑动的单文件窗口 + 有界尾部读取。
//
// 为什么不用 logrotate：节点（精简镜像）不保证装了 logrotate，而 xray.log 是 xray 输出的
// 唯一副本（agent 用管道接它的 stdout/stderr）。大小滑动只需要「读尾部 + 重写一次」，
// 且不改变运维心智——永远只有一个文件，grep 到的就是最近的日志。
//
// 滑动语义：写入使体积超过上限时，保留文件尾部一半（按行首对齐）再继续追加；
// 上限 0 = 不限制（回到旧行为）。日志时效性强，故不压缩、不留多份。
package logfile

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// externalCheckInterval 外部改动（手工 truncate / rm / 换文件）的探测间隔。
// 每秒最多一次 stat：既让外部裁剪能被 agent 感知，也不至于按行 stat。
const externalCheckInterval = time.Second

// Sliding 按大小滑动的日志 writer（并发安全，可直接塞进 io.MultiWriter）。
type Sliding struct {
	path     string
	maxBytes int64

	mu        sync.Mutex
	f         *os.File
	stat      os.FileInfo
	size      int64
	checkedAt time.Time
}

// Open 打开（必要时创建）日志文件；存量文件超过上限时先裁剪一次。
func Open(path string, maxBytes int64) (*Sliding, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	w := &Sliding{path: path, maxBytes: maxBytes}
	// 老节点升级上来第一次启动：把陈年大文件（可能几 GB）当场收口。
	if maxBytes > 0 {
		if st, err := os.Stat(path); err == nil && st.Size() > maxBytes {
			_, _ = SlideNow(path, maxBytes)
		}
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.reopenLocked(); err != nil {
		return nil, err
	}
	return w, nil
}

// Write 追加日志；超过上限先滑动。写失败不改变滑动状态（宁可超限也别丢日志）。
func (w *Sliding) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.checkExternalLocked()
	if w.f == nil {
		if err := w.reopenLocked(); err != nil {
			return 0, err
		}
	}
	if w.maxBytes > 0 && w.size > 0 && w.size+int64(len(p)) > w.maxBytes {
		_, _ = w.slideLocked()
		if w.f == nil {
			return 0, errors.New("日志文件不可写")
		}
	}
	n, err := w.f.Write(p)
	w.size += int64(n)
	return n, err
}

// Close 关闭文件（子进程退出后调用）。
func (w *Sliding) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return nil
	}
	err := w.f.Close()
	w.f = nil
	return err
}

// Size 当前文件体积（字节）。
func (w *Sliding) Size() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.size
}

// checkExternalLocked 发现文件被外部替换/删除/截断时重开，避免继续往旧 inode 里写。
func (w *Sliding) checkExternalLocked() {
	now := time.Now()
	if now.Sub(w.checkedAt) < externalCheckInterval {
		return
	}
	w.checkedAt = now
	st, err := os.Stat(w.path)
	if err != nil {
		if w.f != nil {
			_ = w.f.Close()
			w.f = nil
		}
		return
	}
	if w.stat == nil || !os.SameFile(st, w.stat) {
		if w.f != nil {
			_ = w.f.Close()
			w.f = nil
		}
		return
	}
	// 同 inode 但体积对不上：外部 truncate 过，按现况重算。
	if st.Size() != w.size {
		w.size = st.Size()
	}
}

func (w *Sliding) reopenLocked() error {
	f, err := os.OpenFile(w.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return err
	}
	w.f, w.stat, w.size = f, st, st.Size()
	return nil
}

func (w *Sliding) slideLocked() (int64, error) {
	if w.f != nil {
		_ = w.f.Close()
		w.f = nil
	}
	freed, err := SlideNow(w.path, w.maxBytes)
	if rerr := w.reopenLocked(); rerr != nil && err == nil {
		err = rerr
	}
	return freed, err
}

// SlideNow 把文件裁剪到 maxBytes 的一半（按行首对齐），返回释放字节数。
// 供 CLI（xray-agent logs --vacuum）与 agent 内部共用；文件小于保留量时不动。
func SlideNow(path string, maxBytes int64) (int64, error) {
	if maxBytes <= 0 {
		return 0, errors.New("未启用日志上限（log_max_size_mb = 0）")
	}
	st, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}
		return 0, err
	}
	size := st.Size()
	keep := maxBytes / 2
	if keep <= 0 {
		keep = 1
	}
	if size <= keep {
		return 0, nil
	}
	src, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	buf := make([]byte, keep)
	_, rerr := src.ReadAt(buf, size-keep)
	// 必须先关掉读句柄再 rename：Windows 上带打开句柄的文件无法被替换。
	_ = src.Close()
	if rerr != nil && !errors.Is(rerr, io.EOF) {
		return 0, rerr
	}
	// 对齐到行首：丢掉被截断的首行（找不到换行说明是超长单行，原样保留）。
	if i := bytes.IndexByte(buf, '\n'); i >= 0 {
		buf = buf[i+1:]
	}
	// 临时文件 + rename：外部任何时刻读到的都是完整文件。
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, buf, 0o644); err != nil {
		return 0, err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return 0, err
	}
	return size - int64(len(buf)), nil
}

// Size 返回文件体积（不存在按 0）。
func Size(path string) int64 {
	st, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return st.Size()
}

// Tail 读取文件末尾 n 行（有界：只从尾部按 1MB 窗口回溯，不整文件进内存）。
// 手动模式下没有常驻 agent 时也走这里。
func Tail(path string, n int) (string, error) {
	if n <= 0 {
		n = 100
	}
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", nil
		}
		return "", err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return "", err
	}
	size := st.Size()
	if size == 0 {
		return "", nil
	}
	window := int64(1 << 20)
	for {
		start := size - window
		if start < 0 {
			start = 0
		}
		buf := make([]byte, size-start)
		if _, err := f.ReadAt(buf, start); err != nil && !errors.Is(err, io.EOF) {
			return "", err
		}
		text := strings.TrimRight(string(buf), "\n")
		if start > 0 {
			// 窗口首行可能被截断，按整行对齐后再加入。
			if i := strings.IndexByte(text, '\n'); i >= 0 {
				text = text[i+1:]
			} else {
				text = ""
			}
		}
		lines := splitNonEmpty(text)
		if len(lines) >= n || start == 0 {
			if len(lines) > n {
				lines = lines[len(lines)-n:]
			}
			return strings.Join(lines, "\n"), nil
		}
		window *= 4
	}
}

func splitNonEmpty(text string) []string {
	if text == "" {
		return nil
	}
	return strings.Split(text, "\n")
}
