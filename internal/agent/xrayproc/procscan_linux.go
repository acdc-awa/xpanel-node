//go:build linux

package xrayproc

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// scanProcForConfig 在 /proc 里找出 cmdline 含指定配置路径的进程（pgrep 缺失时的兜底）。
// 与 pgrep -f 同语义：完整 cmdline 子串匹配，排除自身。
func scanProcForConfig(configPath string, self int) []int {
	if configPath == "" {
		return nil
	}
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil
	}
	var pids []int
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		pid, cerr := strconv.Atoi(e.Name())
		if cerr != nil || pid <= 0 || pid == self {
			continue
		}
		data, rerr := os.ReadFile(filepath.Join("/proc", e.Name(), "cmdline"))
		if rerr != nil || len(data) == 0 {
			continue
		}
		// cmdline 以 NUL 分隔，直接当普通文本做子串匹配即可
		if strings.Contains(string(data), configPath) {
			pids = append(pids, pid)
		}
	}
	return pids
}
