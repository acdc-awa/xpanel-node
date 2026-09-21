package xrayproc

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"sync"
	"time"
)

// 错误摘要上限：回传主控的错误文本按 rune 截断（模型列宽 512，留足前缀余量）。
const maxErrText = 300

// logSink 把日志文件包成"永不失败"的 writer：写不进去只丢这一行日志，绝不把错误往上抛。
//
// 为什么必须这样（2026-09-21 实机事故）：子进程的 stdout/stderr 经 os/exec 的管道 + 拷贝
// goroutine 落到这里（writer 不是 *os.File 时 os/exec 必建管道，见 exec.go writerDescriptor）。
// 拷贝 goroutine 一旦因写失败退出，os/exec 会顺手关掉管道读端（exec.go:603 `pr.Close()`），
// 而 Go 对 fd 1/2 的 SIGPIPE **不忽略**——子进程此后任何一次写 stdout/stderr 都会被 SIGPIPE
// 当场杀死（退出码 -1、无任何错误输出、日志停在窗口内最后一行）。
// 于是"日志文件不可写"（磁盘满/被删/权限）会升级成"节点 xray 每隔几秒无声死一次"，
// 而运维看到的只是"进程莫名其妙没了"。丢日志可以，丢进程不行。
type logSink struct{ w io.Writer }

func (s logSink) Write(p []byte) (int, error) {
	_, _ = s.w.Write(p)
	return len(p), nil
}

// ringBuffer 定长环形缓冲，保留最近写入的字节。
// 子进程的 stdout/stderr 同时写日志文件与这里：文件是运维现场（跨次累积、分不清属于哪一次启动），
// 缓冲用来把「本次运行」的致命错误摘要回传主控（2026-09-21：端口被占时 xray 秒死，
// 只有它自己的 stderr 说得清原因）。
type ringBuffer struct {
	mu    sync.Mutex
	buf   []byte
	pos   int
	total int
}

func newRingBuffer(n int) *ringBuffer {
	if n <= 0 {
		n = 4096
	}
	return &ringBuffer{buf: make([]byte, n)}
}

func (r *ringBuffer) Write(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := len(p)
	if n >= len(r.buf) {
		copy(r.buf, p[n-len(r.buf):])
		r.pos = 0
		r.total = len(r.buf)
		return n, nil
	}
	written := copy(r.buf[r.pos:], p)
	if written < n {
		copy(r.buf, p[written:])
	}
	r.pos = (r.pos + n) % len(r.buf)
	r.total += n
	return n, nil
}

// String 返回缓冲内容（按写入顺序，最多 cap 字节）。
func (r *ringBuffer) String() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.total < len(r.buf) {
		return string(r.buf[:r.pos])
	}
	return string(r.buf[r.pos:]) + string(r.buf[:r.pos])
}

// summarizeTail 从 stderr 尾部提取可回传的错误摘要。
// xray 的致命错误形态实测：`Failed to start: app/proxyman/inbound: failed to listen TCP on 443 > ... bind: ...`
// （启动失败 exit 255）、`Failed to start: main: failed to load config files: ...`（配置错 exit 23）、
// 以及 panic。优先取这类行，找不到就退回最后一条非空行。
func summarizeTail(tail string) string {
	lines := strings.Split(strings.TrimRight(tail, "\n"), "\n")
	pick := ""
	for i := len(lines) - 1; i >= 0; i-- {
		l := strings.TrimSpace(lines[i])
		if l == "" {
			continue
		}
		if pick == "" {
			pick = l
		}
		if strings.Contains(l, "Failed to start") || strings.Contains(l, "[Error]") ||
			strings.HasPrefix(l, "panic:") || strings.Contains(l, "bind:") {
			pick = l
			break
		}
	}
	return truncateRunes(pick, maxErrText)
}

func truncateRunes(s string, n int) string {
	rs := []rune(s)
	if len(rs) <= n {
		return s
	}
	return string(rs[:n]) + "…"
}

// exitCodeOf 取子进程退出码（Wait 返回 nil 表示 exit 0）。
func exitCodeOf(err error) int {
	if err == nil {
		return 0
	}
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		return ee.ExitCode()
	}
	return -1
}

// formatExit 把一次退出整理成一行可回传的原因文本。
func formatExit(code int, tail string) string {
	summary := summarizeTail(tail)
	switch {
	case summary != "":
		return fmt.Sprintf("退出码 %d: %s", code, summary)
	case code < 0:
		// ExitCode() 对"被信号终止"返回 -1：xray 没来得及打印任何东西（OOM/SIGKILL 常见）
		return "进程被信号终止（无 stderr 输出，常见于 OOM 或外部 SIGKILL）"
	default:
		return fmt.Sprintf("退出码 %d（xray 无 stderr 输出）", code)
	}
}

// backoffFor 第 n 次连续启动失败后的重试延迟：第 1 次不延迟（看门狗下一拍即重试，与旧行为一致），
// 此后 2s→4s→8s→…→backoffMax 递增，避免永久失败退化成 2s 热循环。
func backoffFor(failures int) time.Duration {
	if failures <= 1 {
		return 0
	}
	d := backoffBase << (failures - 2)
	if d <= 0 || d > backoffMax {
		return backoffMax
	}
	return d
}

// contentHash 配置内容指纹：判"主控这次推的是不是同一份东西"。主控按周期补推的是已保存的
// 同一份 config_json（不重新生成），所以同一份内容的字节稳定、指纹可比。
// 空内容返回空串（调用方据此跳过判等）。
func contentHash(configJSON string) string {
	if configJSON == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(configJSON))
	return hex.EncodeToString(sum[:])
}
