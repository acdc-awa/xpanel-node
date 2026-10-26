//go:build linux

package cli

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// isAgentProcess 校验 pid 确实是 xray-agent 本体，而不是「命令行里恰好含 xray-agent」的
// shell / 包装进程（pgrep -f 会把这些一并匹配出来，stop 误杀父 shell 的后果很严重）。
func isAgentProcess(pid int, exe string) bool {
	data, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/cmdline")
	if err != nil || len(data) == 0 {
		return false
	}
	argv0 := string(data)
	if i := strings.IndexByte(argv0, 0); i >= 0 {
		argv0 = argv0[:i]
	}
	// 常见形态：/usr/local/bin/xray-agent、./xray-agent
	if filepath.Base(argv0) == "xray-agent" {
		return true
	}
	// 二进制被升级替换后 argv0 可能指向旧路径，再按实际可执行文件核对一次。
	if exe != "" {
		if real, err := os.Readlink("/proc/" + strconv.Itoa(pid) + "/exe"); err == nil {
			real = strings.TrimSuffix(real, " (deleted)")
			if real == exe {
				return true
			}
			if base := filepath.Base(argv0); base == filepath.Base(exe) {
				return true
			}
		}
	}
	return false
}
