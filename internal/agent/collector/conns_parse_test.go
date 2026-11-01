package collector

import (
	"runtime"
	"strings"
	"testing"
	"time"
)

// 真实 /proc/net/tcp 的行形态（列数随内核版本变化，解析只依赖首列与第 4 列状态码）。
const tcpTableSample = `  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode
   0: 00000000:01F90 00000000:0000 0A 00000000:00000000 00:00000000 00000000     0        0 41234 1 0000000000000000 100 0 0 10 0
   1: 0100007F:1F90 0100007F:D2F1 01 00000000:00000000 00:00000000 00000000     0        0 41235 1 0000000000000000 20 4 30 10 -1
   2: 0100007F:1F90 0100007F:D2F2 01 00000000:00000000 00:00000000 00000000     0        0 41236 1 0000000000000000 20 4 30 10 -1
   3: 0100007F:1F90 0100007F:D2F3 06 00000000:00000000 00:00000000 00000000     0        0 0 1 0000000000000000 20 4 30 10 -1
   4: 0100007F:1F90 0100007F:D2F4 08 00000000:00000000 00:00000000 00000000     0        0 41237 1 0000000000000000 20 4 30 10 -1
   5: 0100007F:D2F5 0100007F:1F90 02 00000000:00000000 00:00000000 00000000     0        0 41238 1 0000000000000000 20 4 30 10 -1
`

const udpTableSample = `   sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode ref pointer drops
  100: 00000000:01F4 00000000:0000 07 00000000:00000000 00:00000000 00000000     0        0 55555 2 0000000000000000 0
  101: 0100007F:1F91 00000000:0000 07 00000000:00000000 00:00000000 00000000     0        0 55556 2 0000000000000000 0
`

func TestParseTCPTable(t *testing.T) {
	established, total := parseTCPTable(strings.NewReader(tcpTableSample))
	// LISTEN(0A) 不计入；ESTABLISHED 两行；TIME_WAIT / CLOSE_WAIT / SYN_SENT 只计入总数
	if established != 2 {
		t.Errorf("established = %d, want 2", established)
	}
	if total != 5 {
		t.Errorf("total = %d, want 5", total)
	}
}

func TestParseTCPTableHeaderOnly(t *testing.T) {
	established, total := parseTCPTable(strings.NewReader("  sl  local_address rem_address   st\n"))
	if established != 0 || total != 0 {
		t.Errorf("空表应计 0，实际 established=%d total=%d", established, total)
	}
}

func TestParseTCPTableEmpty(t *testing.T) {
	if e, tot := parseTCPTable(strings.NewReader("")); e != 0 || tot != 0 {
		t.Errorf("空数据应为 0/0，实际 %d/%d", e, tot)
	}
}

func TestParseUDPTable(t *testing.T) {
	if n := parseUDPTable(strings.NewReader(udpTableSample)); n != 2 {
		t.Errorf("UDP socket 数 = %d, want 2", n)
	}
}

// 非 Linux 平台 connStats 必须报「不支持」而不是报 0，否则面板会把
// 「拿不到数据」展示成「节点零连接」。
func TestConnStatsUnsupportedOnNonLinux(t *testing.T) {
	if runtime.GOOS == "linux" {
		t.Skip("Linux 平台支持采集连接数，跳过不支持性断言")
	}
	if _, _, _, ok := connStats(); ok {
		t.Error("非 Linux 平台 connStats 应返回 ok=false")
	}
}

// connSnapshot 必须按 connSampleInterval 节流：心跳可配到 3s，不能被每次心跳都触发整表扫描。
// 这里通过把缓存时间戳拨到「刚刚采样过」来断言第二次调用命中缓存（返回值不变且不刷新时间）。
func TestConnSnapshotThrottles(t *testing.T) {
	c := New()
	defer c.Close()

	now := time.Now()
	c.connMu.Lock()
	c.connAt = now
	c.connEst, c.connTotal, c.connUDP, c.connOK = 11, 22, 33, true
	c.connMu.Unlock()

	est, total, udp, ok := c.connSnapshot(now.Add(time.Second))
	if est != 11 || total != 22 || udp != 33 || !ok {
		t.Fatalf("采样间隔内应命中缓存，实际 %d/%d/%d/%v", est, total, udp, ok)
	}

	// 超过间隔后必须重新采样：这里只断言缓存时间被刷新（值取决于平台）
	c.connSnapshot(now.Add(connSampleInterval + time.Second))
	c.connMu.Lock()
	refreshed := c.connAt.After(now)
	c.connMu.Unlock()
	if !refreshed {
		t.Fatal("超过采样间隔后应刷新 connAt")
	}
}
