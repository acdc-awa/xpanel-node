//go:build !linux

package cli

// isAgentProcess 非 Linux 平台没有 /proc，退化为「pgrep 能匹配到就算」。
func isAgentProcess(pid int, exe string) bool { return pid > 0 }
