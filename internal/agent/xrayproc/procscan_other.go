//go:build !linux

package xrayproc

// scanProcForConfig 非 Linux 无 /proc，pgrep 缺失时不做兜底（生产只跑 Linux）。
func scanProcForConfig(configPath string, self int) []int { return nil }
