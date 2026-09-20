package client

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/acdc-awa/xpanel-node/internal/agent/outbox"
	"github.com/acdc-awa/xpanel-node/internal/agent/stats"
	"github.com/acdc-awa/xpanel-node/pkg/protocol"
)

// newFlowClient 构造一个只做流量路径测试的客户端（无 WS：发送必然失败，用于验证
// 「落盘成功即持久化、发送失败不丢」）。
func newFlowClient(t *testing.T) *Client {
	t.Helper()
	c := &Client{
		Outbox: outbox.New(filepath.Join(t.TempDir(), "traffic_outbox.json")),
	}
	if err := c.Outbox.Load(); err != nil {
		t.Fatalf("outbox load: %v", err)
	}
	c.pending = make(map[trafficKey]*pendingEntry)
	c.cycles = make(map[string]uint64)
	c.sentThisConn = make(map[string]struct{})
	c.reportKick = make(chan struct{}, 1)
	return c
}

// TestAccumulateTagsCycleAtCollectTime 账期 ID 在采集时刻取值：
// 切换账期后，已在 pending 里的旧账期增量保留旧 ID，新采集的用新 ID（审计 F3 的核心）。
func TestAccumulateTagsCycleAtCollectTime(t *testing.T) {
	c := newFlowClient(t)
	const key = "u7.i3@panel.local"

	// 账期 1：采集 100 字节
	c.applyCycles(map[string][]protocol.User{"in-a": {{Email: key, CycleID: 1}}})
	c.accumulate([]stats.Entry{{Email: key, Up: 100}})

	// 主控切到账期 2（节点收到新名单）→ 封账后切换
	if changed := c.cycleChanges(map[string][]protocol.User{"in-a": {{Email: key, CycleID: 2}}}); len(changed) != 1 {
		t.Fatalf("应检测到 1 个账期变化，实际 %v", changed)
	}
	c.applyCycles(map[string][]protocol.User{"in-a": {{Email: key, CycleID: 2}}})
	// 账期 2：再采集 50 字节
	c.accumulate([]stats.Entry{{Email: key, Up: 50}})

	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()
	if len(c.pending) != 2 {
		t.Fatalf("两个账期应各占一个 pending 键，实际 %d 个", len(c.pending))
	}
	got := map[uint64]int64{}
	for k, p := range c.pending {
		got[k.cycleID] = p.Up
	}
	if got[1] != 100 || got[2] != 50 {
		t.Fatalf("账期归属错误: %v（want {1:100, 2:50}）", got)
	}
}

// TestCycleChangesIgnoresNewAndUnknownUsers 新用户（本地无记录）与旧主控（CycleID=0）
// 都不算「账期变化」——前者没有旧增量需要封账，后者根本没下发账期。
func TestCycleChangesIgnoresNewAndUnknownUsers(t *testing.T) {
	c := newFlowClient(t)
	users := map[string][]protocol.User{
		"in-a": {{Email: "new@panel.local", CycleID: 3}, {Email: "old-master@panel.local", CycleID: 0}},
	}
	if changed := c.cycleChanges(users); len(changed) != 0 {
		t.Fatalf("首次见到的用户不应算账期变化，实际 %v", changed)
	}
	c.applyCycles(users)
	// 旧主控不下发账期（0）：不写入映射，也不产生变化
	if got := c.cycleFor("old-master@panel.local"); got != 0 {
		t.Fatalf("CycleID=0 不应写入映射，实际 %d", got)
	}
	// 已记录的用户账期变化 → 检出
	if changed := c.cycleChanges(map[string][]protocol.User{"in-a": {{Email: "new@panel.local", CycleID: 4}}}); len(changed) != 1 {
		t.Fatalf("应检出账期变化，实际 %v", changed)
	}
}

// TestReportPendingPersistsAndSends 上报循环：内存 pending 落盘为批次并发送。
// 无连接时发送失败，但批次必须留在发件箱（下轮重发），不能丢。
func TestReportPendingPersistsAndSends(t *testing.T) {
	c := newFlowClient(t)
	c.applyCycles(map[string][]protocol.User{"in-a": {{Email: "u1.i2@panel.local", CycleID: 5}}})
	c.accumulate([]stats.Entry{{Email: "u1.i2@panel.local", Up: 100, Down: 200}})

	c.reportPending()

	batches, entries := c.Outbox.Pending()
	if batches != 1 || entries != 1 {
		t.Fatalf("应落盘 1 批 1 条，实际 %d 批 %d 条", batches, entries)
	}
	// 内存 pending 已交接给发件箱
	c.pendingMu.Lock()
	n := len(c.pending)
	c.pendingMu.Unlock()
	if n != 0 {
		t.Fatalf("落盘成功后 pending 应清空，实际 %d 条", n)
	}
	b, _ := c.Outbox.Oldest()
	if b.Entries[0].CycleID != 5 {
		t.Fatalf("批次应带采集时刻的账期 ID 5，实际 %d", b.Entries[0].CycleID)
	}
	if b.Up() != 100 || b.Down() != 200 {
		t.Fatalf("批次字节 = %d/%d, want 100/200", b.Up(), b.Down())
	}

	// 再跑一轮：新数据进新批次，旧批次仍在（等主控回执）
	c.accumulate([]stats.Entry{{Email: "u1.i2@panel.local", Up: 7}})
	c.reportPending()
	if batches, _ := c.Outbox.Pending(); batches != 2 {
		t.Fatalf("未确认批次应累积为 2 批，实际 %d", batches)
	}
}

// TestAckRemovesBatch 主控回执 ok=true → 删批；ok=false → 保留重发。
func TestAckRemovesBatch(t *testing.T) {
	c := newFlowClient(t)
	c.accumulate([]stats.Entry{{Email: "a", Up: 1}})
	c.reportPending()
	b, _ := c.Outbox.Oldest()

	// 主控回绝（写库失败）：保留
	if res := c.dispatch(mustMsg(t, protocol.MsgTrafficAck,
		protocol.TrafficAckPayload{BatchID: b.ID, OK: false, Error: "流量落库失败"})); res != nil {
		t.Fatalf("回执不是指令，不应回 result 帧，实际 %+v", res)
	}
	if batches, _ := c.Outbox.Pending(); batches != 1 {
		t.Fatalf("回执 ok=false 时批次必须保留，实际 %d 批", batches)
	}

	// 主控确认：删除
	if res := c.dispatch(mustMsg(t, protocol.MsgTrafficAck,
		protocol.TrafficAckPayload{BatchID: b.ID, OK: true})); res != nil {
		t.Fatalf("回执不是指令，不应回 result 帧，实际 %+v", res)
	}
	if batches, _ := c.Outbox.Pending(); batches != 0 {
		t.Fatalf("回执 ok=true 后应删批，实际 %d 批", batches)
	}
}

// TestReportPendingKeepsPendingWhenOutboxFails 落盘失败（发件箱路径不可写）时数据放回内存，
// 下轮重试——不得静默丢弃。
func TestReportPendingKeepsPendingWhenOutboxFails(t *testing.T) {
	c := newFlowClient(t)
	// 把发件箱路径指到一个不可能写入的位置（父路径是文件而非目录）
	blocker := filepath.Join(t.TempDir(), "blocker")
	if err := writeFile(blocker, "x"); err != nil {
		t.Fatal(err)
	}
	c.Outbox = outbox.New(filepath.Join(blocker, "sub", "outbox.json"))

	c.accumulate([]stats.Entry{{Email: "a", Up: 100}})
	c.reportPending()

	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()
	if len(c.pending) != 1 {
		t.Fatalf("落盘失败时数据必须放回 pending，实际 %d 条", len(c.pending))
	}
	for _, p := range c.pending {
		if p.Up != 100 {
			t.Fatalf("放回的字节数 = %d, want 100", p.Up)
		}
	}
}

// TestCycleForInboundDimensionIsZero 入站维度条目（Email 恒空）无用户账期 → 0。
func TestCycleForInboundDimensionIsZero(t *testing.T) {
	c := newFlowClient(t)
	if got := c.cycleFor(""); got != 0 {
		t.Fatalf("入站维度条目的账期应为 0，实际 %d", got)
	}
	c.applyCycles(map[string][]protocol.User{"in-a": {{Email: "u1.i2@panel.local", CycleID: 9}}})
	c.accumulate([]stats.Entry{{Inbound: "in-a", Up: 5}})
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()
	for k := range c.pending {
		if k.cycleID != 0 || k.inbound != "in-a" {
			t.Fatalf("入站维度键错误: %+v", k)
		}
	}
}

func writeFile(path, content string) error {
	return os.WriteFile(path, []byte(content), 0o600)
}
