package collector

import (
	"math"
	"testing"
	"time"
)

func TestParseNetDevData(t *testing.T) {
	sample := `Inter-|   Receive                                                |  Transmit
 face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed
    lo: 12345678       0    0    0    0     0          0         0 12345678       0    0    0    0     0       0          0
  eth0: 10000000     500    0    0    0     0          0         0 20000000     600    0    0    0     0       0          0
docker0: 9999999       0    0    0    0     0          0         0  9999999       0    0    0    0     0       0          0
  ens3:  5000000     200    0    0    0     0          0         0  8000000     300    0    0    0     0       0          0
 veth1: 11111111       0    0    0    0     0          0         0 11111111       0    0    0    0     0       0          0
`
	rx, tx := parseNetDevData(sample)
	expectedRx := uint64(10000000 + 5000000)
	expectedTx := uint64(20000000 + 8000000)

	if rx != expectedRx {
		t.Errorf("expected rx %d, got %d", expectedRx, rx)
	}
	if tx != expectedTx {
		t.Errorf("expected tx %d, got %d", expectedTx, tx)
	}
}

// newTestCollector 构造不启动 1 秒采样循环的采集器：后台循环会读真实 /proc，
// 与被 stub 的接缝和数据序列冲突。
func newTestCollector() *Collector {
	return &Collector{stopCh: make(chan struct{})}
}

// stubProc 用固定序列替换 /proc 读取与时钟，测试结束自动还原。
func stubProc(t *testing.T, ticks [][2]float64, net [][2]uint64, clock []time.Time) {
	t.Helper()
	oldTicks, oldNet, oldNow := cpuTicksFn, netDevInfoFn, nowFn
	ti, ni, ci := 0, 0, 0
	cpuTicksFn = func() (float64, float64, bool) {
		if ti >= len(ticks) {
			return 0, 0, false
		}
		v := ticks[ti]
		ti++
		return v[0], v[1], true
	}
	netDevInfoFn = func() (uint64, uint64, bool) {
		if ni >= len(net) {
			return 0, 0, false
		}
		v := net[ni]
		ni++
		return v[0], v[1], true
	}
	nowFn = func() time.Time {
		if ci >= len(clock) {
			return clock[len(clock)-1]
		}
		v := clock[ci]
		ci++
		return v
	}
	t.Cleanup(func() { cpuTicksFn, netDevInfoFn, nowFn = oldTicks, oldNet, oldNow })
}

// TestCPUNoSpikeOnCollidingSamples 回归 2026-11-01：心跳（30s）与快车道（3s）撞在同一时刻时，
// 后到的一方只有亚毫秒窗口，jiffy 量化会给出 dTotal=1 而 dIdle=0 —— 旧实现据此算出
// (1-0/1)*100 = 100%，面板上表现为 CPU 周期性跳到 100%。修复后必须沿用上一帧的值，
// 且不能污染基线（下一个正常窗口仍要算对）。
func TestCPUNoSpikeOnCollidingSamples(t *testing.T) {
	base := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	stubProc(t,
		[][2]float64{
			{1000, 2000}, // t0        建立基线
			{1300, 2600}, // t0+3s     正常窗口 300/600 → 50%
			{1300, 2601}, // t0+3s+1ms 撞车窗口：只有 1 个忙 jiffy
			{1600, 3200}, // t0+6s     正常窗口 300/600 → 50%
		},
		[][2]uint64{{100, 100}, {200, 200}, {300, 300}, {400, 400}},
		[]time.Time{
			base,
			base.Add(3 * time.Second),
			base.Add(3*time.Second + time.Millisecond),
			base.Add(6 * time.Second),
		},
	)
	c := newTestCollector()

	if got := c.RealtimeSnapshot().CPU; got != 0 {
		t.Fatalf("首帧无基线，CPU 应为 0，得到 %v", got)
	}
	if got := c.RealtimeSnapshot().CPU; math.Abs(got-50) > 0.01 {
		t.Fatalf("正常 3s 窗口 CPU 应为 50%%，得到 %v", got)
	}
	if got := c.RealtimeSnapshot().CPU; math.Abs(got-50) > 0.01 {
		t.Fatalf("1ms 撞车窗口应沿用上一帧 50%%，得到 %v（旧实现报 100%%）", got)
	}
	if got := c.RealtimeSnapshot().CPU; math.Abs(got-50) > 0.01 {
		t.Fatalf("撞车不能污染基线，下一个窗口 CPU 仍应为 50%%，得到 %v", got)
	}
}

// TestNetRateNoSpikeOnCollidingSamples 同一根因的另一面：1ms 窗口内几个字节会被算成巨大速率，
// 且撞车帧不能把峰值跟踪清零。
func TestNetRateNoSpikeOnCollidingSamples(t *testing.T) {
	base := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	stubProc(t,
		[][2]float64{{0, 0}, {0, 0}, {0, 0}, {0, 0}},
		[][2]uint64{
			{1000, 1000},
			{4000, 1000},   // 3s 内 3000 字节 → 1000 B/s
			{504000, 1000}, // 1ms 内 +500KB → 旧实现算出 5×10^8 B/s
			{510000, 1000}, // 下一个正常窗口：3s 内 6000 字节
		},
		[]time.Time{
			base,
			base.Add(3 * time.Second),
			base.Add(3*time.Second + time.Millisecond),
			base.Add(6 * time.Second),
		},
	)
	c := newTestCollector()

	c.RealtimeSnapshot() // 建立基线
	if got := c.RealtimeSnapshot().RxRate; math.Abs(got-1000) > 0.01 {
		t.Fatalf("正常 3s 窗口速率应为 1000 B/s，得到 %v", got)
	}

	// 撞车帧之前注入一个峰值：撞车帧不能让它被复位（否则本轮峰值被截断）
	c.mu.Lock()
	c.peakRxRate = 9_000_000
	c.mu.Unlock()

	if got := c.RealtimeSnapshot().RxRate; math.Abs(got-1000) > 0.01 {
		t.Fatalf("1ms 撞车窗口应沿用上一帧 1000 B/s，得到 %v", got)
	}
	c.mu.Lock()
	peak := c.peakRxRate
	c.mu.Unlock()
	if peak != 9_000_000 {
		t.Fatalf("撞车帧不应复位峰值，得到 %v", peak)
	}

	// 下一个正常窗口照常重算、带上峰值，随后复位
	if got := c.RealtimeSnapshot().RxRate; got != 9_000_000 {
		t.Fatalf("正常窗口应取峰值 9000000 B/s，得到 %v", got)
	}
	c.mu.Lock()
	peak = c.peakRxRate
	c.mu.Unlock()
	if peak != 0 {
		t.Fatalf("正常窗口处理完应复位峰值，得到 %v", peak)
	}
}

// TestSystemSnapshotLeavesBaselineAlone 心跳走的 SystemSnapshot 不能写差分基线：
// 否则快车道下一次采样会拿到被心跳截断的窗口。
func TestSystemSnapshotLeavesBaselineAlone(t *testing.T) {
	base := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	stubProc(t,
		[][2]float64{
			{1000, 2000}, // t0     快车道建立基线
			{1300, 2600}, // t0+3s  正常窗口 300/600 → 50%
			{1900, 3800}, // 心跳不该消费这一帧，下面的快车道帧才用它
		},
		[][2]uint64{{0, 0}, {0, 0}, {0, 0}},
		[]time.Time{
			base,
			base.Add(3 * time.Second),
			base.Add(30 * time.Second),
		},
	)
	c := newTestCollector()

	c.RealtimeSnapshot()
	if got := c.RealtimeSnapshot().CPU; math.Abs(got-50) > 0.01 {
		t.Fatalf("正常窗口 CPU 应为 50%%，得到 %v", got)
	}

	// 心跳（SystemSnapshot）完全不碰 /proc/stat 与网速基线
	if got := c.SystemSnapshot().CPU; got != 0 {
		t.Fatalf("SystemSnapshot 不应产出 CPU，得到 %v", got)
	}

	// 快车道下一帧的窗口仍从 t0+3s 起算（900 idle / 1800 total → 50%），
	// 没有被心跳截断成 30s 窗口里的碎片
	if got := c.RealtimeSnapshot().CPU; math.Abs(got-50) > 0.01 {
		t.Fatalf("心跳不应截断快车道窗口，CPU 应为 50%%，得到 %v", got)
	}
}

func TestCollectorLifecycleIdempotentClose(t *testing.T) {
	c := New()
	c.Close()
	c.Close() // 幂等：重复关闭不得 panic
}
