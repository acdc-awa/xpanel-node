package collector

import (
	"bufio"
	"io"
	"strings"
)

// 内核连接表解析（/proc/net/{tcp,tcp6,udp,udp6}）。
//
// 解析与读取分开：解析部分不带构建约束，便于在任意平台用样例数据做单测；
// 只有「读 /proc」的部分按平台分文件（conns_linux.go / conns_other.go）。
//
// 用 bufio 逐行扫描而不是整文件读进内存：繁忙节点的连接可能有几万个，
// 整表读入 + strings.Split 会在每次采样时制造大量临时对象，把 GC 压力加到
// 正在跑代理的节点上。

const (
	tcpStateEstablished = "01"
	tcpStateListen      = "0A"
)

// scanLines 逐行扫描并回调；返回是否为合法数据行（首列形如 "0:"）。
func scanLines(r io.Reader, fn func(fields []string)) error {
	sc := bufio.NewScanner(r)
	// 单行最长约 200 字节，给一点余量；防止极端环境下超长行报错
	sc.Buffer(make([]byte, 0, 512), 8*1024)
	for sc.Scan() {
		line := sc.Text()
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		// 表头首列是 "sl"（不以冒号结尾），数据行首列是 "0:"、"1:" …
		if len(fields) < 4 || !strings.HasSuffix(fields[0], ":") {
			continue
		}
		fn(fields)
	}
	return sc.Err()
}

// parseTCPTable 统计一张 TCP 表：已建立连接数与连接表项总数。
//
// 口径：LISTEN 是监听项而不是连接，不计入两者；TIME_WAIT / CLOSE_WAIT 等
// 已断开但内核仍保留的表项计入 total（它们是短连接风暴与端口耗尽的前兆），
// 只有 ESTABLISHED 计入 established。
func parseTCPTable(r io.Reader) (established, total int) {
	_ = scanLines(r, func(fields []string) {
		switch fields[3] {
		case tcpStateListen:
			return
		case tcpStateEstablished:
			established++
		}
		total++
	})
	return established, total
}

// parseUDPTable 统计一张 UDP 表里的 socket 数。
//
// UDP 没有连接语义：这里数的是「绑定的 socket 数」，一条 QUIC/Hysteria 监听 socket
// 会承载成千上万个会话，所以该值通常是几个而不是几千。它用来发现「端口被别人占了」
// 或「协议栈 socket 泄漏」，不能当成 UDP 会话数看。
func parseUDPTable(r io.Reader) int {
	n := 0
	_ = scanLines(r, func([]string) { n++ })
	return n
}
