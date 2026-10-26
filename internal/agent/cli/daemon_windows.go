//go:build windows

package cli

import (
	"errors"
	"os"
	"time"
)

// spawnDetached Windows 不支持脱离终端（agent 实际只部署在 Linux）。
func spawnDetached(exe string, args []string, env []string) (int, error) {
	return 0, errors.New("后台启动仅支持 Linux；Windows 请用 xray-agent run 前台运行")
}

// processAlive Windows 下用 FindProcess 探测（仅用于 CLI 状态显示）。
func processAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	p, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	_ = p
	return true
}

// waitAlive Windows 下不做存活校验（启动逻辑不走这条路径）。
func waitAlive(pid int, d time.Duration) bool { return processAlive(pid) }
