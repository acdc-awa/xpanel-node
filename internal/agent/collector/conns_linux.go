//go:build linux

package collector

import "os"

// connStats 读取内核连接表，返回 TCP 已建立连接数、TCP 表项总数（不含 LISTEN）、
// UDP 绑定 socket 数。IPv4/IPv6 两张表各自独立，任一可读即认为支持。
//
// 口径是整机而非仅 xray 进程：代理节点上绝大多数 socket 都来自 xray，
// 整机口径还能顺带暴露「有别的进程在偷偷监听/建连」。
func connStats() (established, total, udp int, ok bool) {
	for _, path := range []string{"/proc/net/tcp", "/proc/net/tcp6"} {
		f, err := os.Open(path)
		if err != nil {
			continue
		}
		e, t := parseTCPTable(f)
		_ = f.Close()
		established += e
		total += t
		ok = true
	}
	for _, path := range []string{"/proc/net/udp", "/proc/net/udp6"} {
		f, err := os.Open(path)
		if err != nil {
			continue
		}
		udp += parseUDPTable(f)
		_ = f.Close()
		ok = true
	}
	return established, total, udp, ok
}
