//go:build !linux

package collector

// connStats 非 Linux 环境安全回退：没有 /proc 连接表，ok=false 表示「不支持」，
// 心跳据此不上报连接数（区别于「节点确实 0 连接」）。
func connStats() (established, total, udp int, ok bool) {
	return 0, 0, 0, false
}
