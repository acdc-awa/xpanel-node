package client

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"

	statsService "github.com/xtls/xray-core/app/stats/command"
	"google.golang.org/grpc"

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

type fakeStatsServer struct {
	statsService.UnimplementedStatsServiceServer
	mu    sync.Mutex
	stats []*statsService.Stat
	err   error
}

func (f *fakeStatsServer) QueryStats(_ context.Context, _ *statsService.QueryStatsRequest) (*statsService.QueryStatsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	return &statsService.QueryStatsResponse{Stat: f.stats}, nil
}

func (f *fakeStatsServer) GetUsersStats(_ context.Context, _ *statsService.GetUsersStatsRequest) (*statsService.GetUsersStatsResponse, error) {
	return &statsService.GetUsersStatsResponse{}, nil
}

func startFakeStatsServer(t *testing.T, f *fakeStatsServer) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	statsService.RegisterStatsServiceServer(srv, f)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

// TestSealCycle 验证账期切换封账：
// 1. Stats 为 nil 时安全无操作
// 2. 正常情况下，在 applyCycles 之前调用 sealCycle 会立即采集，增量打上旧 CycleID 标签；
//    applyCycles 之后采集的增量打上新 CycleID 标签
// 3. Stats 采集失败时，优雅忽略且不影响状态
func TestSealCycle(t *testing.T) {
	// 1. Stats 为 nil 时调用不 panic
	cNil := newFlowClient(t)
	cNil.sealCycle([]string{"u1.i1@panel.local"})

	// 2. 真实采集封账测试
	fake := &fakeStatsServer{}
	addr := startFakeStatsServer(t, fake)

	c := newFlowClient(t)
	c.Stats = stats.New(addr)
	t.Cleanup(func() {
		c.Stats.Close()
	})

	const email = "u1.i1@panel.local"
	// 设置用户初始账期 100
	c.applyCycles(map[string][]protocol.User{"in-a": {{Email: email, CycleID: 100}}})

	// 初始基线采集：Counter 为 1000（建立 baseline，不产出 delta）
	fake.mu.Lock()
	fake.stats = []*statsService.Stat{
		{Name: "user>>>" + email + ">>>traffic>>>uplink", Value: 1000},
	}
	fake.mu.Unlock()

	entries, err := c.Stats.Collect(context.Background())
	if err != nil {
		t.Fatalf("建立基线失败: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("基线建立时不应产出增量，实际 %d 条", len(entries))
	}

	// 产生流量（Counter 升至 1500，增量 500），此时收到账期切换（主控下发新账期 200）
	fake.mu.Lock()
	fake.stats = []*statsService.Stat{
		{Name: "user>>>" + email + ">>>traffic>>>uplink", Value: 1500},
	}
	fake.mu.Unlock()

	newUsers := map[string][]protocol.User{"in-a": {{Email: email, CycleID: 200}}}
	changed := c.cycleChanges(newUsers)
	if len(changed) != 1 {
		t.Fatalf("应检出 1 个账期变化，实际 %v", changed)
	}

	// 封账：applyCycles 之前调用 sealCycle
	c.sealCycle(changed)

	// 验证：此时 pending 中必须有旧账期 100 的增量 500
	c.pendingMu.Lock()
	kOld := trafficKey{email: email, cycleID: 100}
	if p, ok := c.pending[kOld]; !ok || p.Up != 500 {
		t.Fatalf("旧账期封账数据不正确: ok=%v, pending=%+v", ok, c.pending)
	}
	c.pendingMu.Unlock()

	// 封账后应用新账期
	c.applyCycles(newUsers)
	if got := c.cycleFor(email); got != 200 {
		t.Fatalf("applyCycles 后账期应为 200，实际 %d", got)
	}

	// 新账期下再次产生流量（Counter 升至 1800，增量 300）
	fake.mu.Lock()
	fake.stats = []*statsService.Stat{
		{Name: "user>>>" + email + ">>>traffic>>>uplink", Value: 1800},
	}
	fake.mu.Unlock()

	entries2, err := c.Stats.Collect(context.Background())
	if err != nil {
		t.Fatalf("新账期采集失败: %v", err)
	}
	c.accumulate(entries2)

	// 验证：pending 中同时存在两个账期的条目，分别对应 500 和 300
	c.pendingMu.Lock()
	kNew := trafficKey{email: email, cycleID: 200}
	if p, ok := c.pending[kNew]; !ok || p.Up != 300 {
		t.Fatalf("新账期数据不正确: ok=%v, pending=%+v", ok, c.pending)
	}
	if p, ok := c.pending[kOld]; !ok || p.Up != 500 {
		t.Fatalf("旧账期数据丢失或被覆盖: ok=%v, pending=%+v", ok, c.pending)
	}
	c.pendingMu.Unlock()

	// 3. 测试采集失败时的健壮性
	fake.mu.Lock()
	fake.err = errors.New("模拟 gRPC 采集错误")
	fake.mu.Unlock()

	// 不应 panic，直接返回
	c.sealCycle([]string{email})
}

