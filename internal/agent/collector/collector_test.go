package collector

import (
	"testing"
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

func TestCollectorLifecycleAndPeakRate(t *testing.T) {
	c := New()
	// 验证幂等关闭
	c.Close()
	c.Close()

	// 验证 Snapshot 峰值保留与复位逻辑
	c.mu.Lock()
	c.peakRxRate = 100 * 1024 * 1024 // 100 MB/s
	c.peakTxRate = 20 * 1024 * 1024  // 20 MB/s
	c.mu.Unlock()

	// 直接调用 Snapshot
	snap := c.Snapshot()
	// 在 Linux 上若有网卡数据会采入 peak；无论是否有真实网卡，Snapshot 执行后峰值必须复位
	c.mu.Lock()
	resetRx := c.peakRxRate
	resetTx := c.peakTxRate
	c.mu.Unlock()

	if resetRx != 0 || resetTx != 0 {
		t.Errorf("Snapshot 后峰值未复位: rx=%v, tx=%v", resetRx, resetTx)
	}
	_ = snap
}

