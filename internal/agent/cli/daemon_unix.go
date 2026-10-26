//go:build !windows

package cli

import (
	"os"
	"os/exec"
	"syscall"
	"time"
)

// spawnDetached 以新会话（脱离终端）拉起子进程，返回 pid。
// 手动安装没有服务管理器，start 需要自己把 agent 放到后台，否则 SSH 一断就跟着退。
func spawnDetached(exe string, args []string, env []string) (int, error) {
	devnull, err := os.OpenFile(os.DevNull, os.O_RDWR, 0)
	if err != nil {
		return 0, err
	}
	defer devnull.Close()
	cmd := exec.Command(exe, args...)
	cmd.Env = env
	cmd.Stdin = devnull
	cmd.Stdout = devnull
	cmd.Stderr = devnull
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		return 0, err
	}
	pid := cmd.Process.Pid
	_ = cmd.Process.Release()
	return pid, nil
}

// processAlive 进程是否还活着（signal 0 探测）。
func processAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	return syscall.Kill(pid, 0) == nil
}

// waitAlive 在 d 内确认进程还活着；用于识别「拉起后立刻退出」。
func waitAlive(pid int, d time.Duration) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if !processAlive(pid) {
			return false
		}
		time.Sleep(100 * time.Millisecond)
	}
	return processAlive(pid)
}
