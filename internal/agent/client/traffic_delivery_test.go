package client

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/acdc-awa/xpanel-node/internal/agent/stats"
	"github.com/acdc-awa/xpanel-node/pkg/protocol"
	"github.com/gorilla/websocket"
)

// fakeMaster 最小 WS 主控：完成认证握手后按配置回 auth_ok / traffic_ack，并记录收到的上报。
// caps=nil 复刻旧主控（回执只有 ok，无能力声明）；ackOK=nil 表示从不回执（旧主控行为）。
type fakeMaster struct {
	srv    *httptest.Server
	caps   []string
	ackOK  *bool
	mu     sync.Mutex
	report []protocol.TrafficReportPayload
}

func newFakeMaster(t *testing.T, caps []string, ackOK *bool) *fakeMaster {
	t.Helper()
	f := &fakeMaster{caps: caps, ackOK: ackOK}
	up := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer ws.Close()
		_, data, err := ws.ReadMessage()
		if err != nil {
			return
		}
		msg, err := protocol.Decode(data)
		if err != nil || msg.Type != protocol.MsgAuth {
			return
		}
		// AuthOKPayload{OK:true, Caps:nil} 序列化为 {"ok":true}，与旧主控的
		// ResultPayload{OK:true} 逐字节一致——测试因此同时覆盖了新旧两种对端。
		raw, err := protocol.Encode(protocol.MsgAuthOK, msg.ID, protocol.AuthOKPayload{OK: true, Caps: f.caps})
		if err != nil {
			return
		}
		if err := ws.WriteMessage(websocket.TextMessage, raw); err != nil {
			return
		}
		for {
			_, data, err := ws.ReadMessage()
			if err != nil {
				return
			}
			m, err := protocol.Decode(data)
			if err != nil || m.Type != protocol.MsgTrafficReport {
				continue
			}
			var p protocol.TrafficReportPayload
			if err := m.PayloadTo(&p); err != nil {
				continue
			}
			f.mu.Lock()
			f.report = append(f.report, p)
			f.mu.Unlock()
			if f.ackOK != nil {
				ack, err := protocol.Encode(protocol.MsgTrafficAck, "",
					protocol.TrafficAckPayload{BatchID: p.BatchID, OK: *f.ackOK})
				if err != nil {
					return
				}
				if err := ws.WriteMessage(websocket.TextMessage, ack); err != nil {
					return
				}
			}
		}
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeMaster) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.report)
}

// waitReports 等到收到 n 条上报（超时报错）。上报是异步落地的，断言前必须等。
func (f *fakeMaster) waitReports(t *testing.T, n int) []protocol.TrafficReportPayload {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		f.mu.Lock()
		got := append([]protocol.TrafficReportPayload(nil), f.report...)
		f.mu.Unlock()
		if len(got) >= n {
			return got
		}
		if time.Now().After(deadline) {
			t.Fatalf("等待 %d 条 traffic_report 超时（实际 %d 条）", n, len(got))
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// assertStaysAt 断言上报数稳定在 n（用于「不得重发」这类反向断言）。
func (f *fakeMaster) assertStaysAt(t *testing.T, n int) {
	t.Helper()
	time.Sleep(200 * time.Millisecond)
	if got := f.count(); got != n {
		t.Fatalf("上报数应稳定在 %d，实际 %d（多发即重复投递）", n, got)
	}
}

// connectClient 让客户端走生产路径 connectAndServe 完成真实认证握手，并停在消息循环上。
// auth_ok 的能力解析、ackMode 置位、resetSent 都是被测代码本身，测试不复制这段逻辑。
func connectClient(t *testing.T, f *fakeMaster) *Client {
	t.Helper()
	c := newFlowClient(t)
	// 心跳周期拉长到不会触发：connectAndServe 会起心跳循环，而本测试不构造 Collector
	// （ticker 一旦触发会空指针）。测试耗时远小于 1 小时。
	c.Heartbeat = time.Hour
	c.BaseURL = "ws" + strings.TrimPrefix(f.srv.URL, "http")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		backoff := time.Second
		_ = c.connectAndServe(ctx, &backoff)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	// 握手完成的确定性同步点：connectAndServe 在 ackMode 置位与 resetSent 之后才
	// kickReport。测试不跑 reportLoop，直接取走这个信号（缓冲为 1，不会丢）。
	select {
	case <-c.reportKick:
	case <-time.After(3 * time.Second):
		t.Fatal("等待认证握手超时")
	}
	return c
}

// waitAwaiting 等某批次的「已发出待回执」标记变为 want。
// 回执由读循环异步处理，断言前必须等它落地，否则测到的是时序而不是语义。
func waitAwaiting(t *testing.T, c *Client, batchID string, want bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		if c.awaitingAck(batchID) == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("等待批次 %s 的「已发出待回执」标记变为 %v 超时", batchID, want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func seedBatch(t *testing.T, c *Client) {
	t.Helper()
	c.accumulate([]stats.Entry{{Email: "u1.i2@panel.local", Up: 100, Down: 200}})
	c.reportPending()
}

// TestLegacyMasterDeletesBatchAfterSend 旧主控（auth_ok 无能力声明）：发完即删。
// 这是防重复计量的关键——旧主控小时桶 upsert 是加法且无批次去重，留下的批次每轮重发
// 会把同一批流量反复累加到用户账上。
func TestLegacyMasterDeletesBatchAfterSend(t *testing.T) {
	f := newFakeMaster(t, nil, nil) // 无 caps、从不回执 = 旧主控
	c := connectClient(t, f)

	seedBatch(t, c)
	f.waitReports(t, 1)

	if batches, _ := c.Outbox.Pending(); batches != 0 {
		t.Fatalf("旧主控下批次应发完即删，实际残留 %d 批", batches)
	}

	// 再跑两轮：队列已空，不得有任何重发
	c.reportPending()
	c.reportPending()
	f.assertStaysAt(t, 1)
}

// TestAckMasterKeepsBatchAndDoesNotResendInConnection 新主控但回执未达：
// 批次必须保留（不能丢数据），但同一条连接上不得重发。
func TestAckMasterKeepsBatchAndDoesNotResendInConnection(t *testing.T) {
	f := newFakeMaster(t, []string{protocol.CapTrafficAck}, nil) // 声明能力但本次不回执
	c := connectClient(t, f)

	seedBatch(t, c)
	f.waitReports(t, 1)
	if batches, _ := c.Outbox.Pending(); batches != 1 {
		t.Fatalf("未收到回执时批次必须保留，实际 %d 批", batches)
	}

	c.reportPending()
	c.reportPending()
	f.assertStaysAt(t, 1)
}

// TestAckMasterResendsAfterReconnect 重连后清空「已发出」标记：回执可能随旧连接丢失，
// 未确认批次必须在新连接上重发，重复由主控 BatchID 去重吸收。
func TestAckMasterResendsAfterReconnect(t *testing.T) {
	f := newFakeMaster(t, []string{protocol.CapTrafficAck}, nil)
	c := connectClient(t, f)

	seedBatch(t, c)
	f.waitReports(t, 1)
	c.reportPending()
	f.assertStaysAt(t, 1)

	c.resetSent() // 等价于一次重连（connectAndServe 认证后即调用它）
	c.reportPending()
	got := f.waitReports(t, 2)
	if got[0].BatchID != got[1].BatchID {
		t.Fatalf("重发必须复用同一 BatchID（主控靠它去重）: %s vs %s", got[0].BatchID, got[1].BatchID)
	}
}

// TestNegativeAckUnlocksRetry 主控明确回绝（写库失败）→ 解锁重试，下轮重发。
func TestNegativeAckUnlocksRetry(t *testing.T) {
	no := false
	f := newFakeMaster(t, []string{protocol.CapTrafficAck}, &no)
	c := connectClient(t, f)

	seedBatch(t, c)
	first := f.waitReports(t, 1)
	batchID := first[0].BatchID

	// 先等回执被读循环处理完（标记解除），再触发下一轮——否则会在回执到达前抢跑，
	// 测到的是时序而非「回绝即解锁重试」这条语义。
	waitAwaiting(t, c, batchID, false)
	c.reportPending()
	got := f.waitReports(t, 2)
	if got[0].BatchID != got[1].BatchID {
		t.Fatalf("重试必须复用同一 BatchID: %s vs %s", got[0].BatchID, got[1].BatchID)
	}
	if batches, _ := c.Outbox.Pending(); batches != 1 {
		t.Fatalf("回绝后批次仍应保留，实际 %d 批", batches)
	}
}

// TestPositiveAckDeletesBatch 主控确认落库 → 删批，发件箱清空。
func TestPositiveAckDeletesBatch(t *testing.T) {
	yes := true
	f := newFakeMaster(t, []string{protocol.CapTrafficAck}, &yes)
	c := connectClient(t, f)

	seedBatch(t, c)
	f.waitReports(t, 1)

	deadline := time.Now().Add(3 * time.Second)
	for {
		if batches, _ := c.Outbox.Pending(); batches == 0 {
			return
		}
		if time.Now().After(deadline) {
			b, _ := c.Outbox.Pending()
			t.Fatalf("回执 ok=true 后发件箱应清空，实际残留 %d 批", b)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestAuthOKCapabilityWireCompat 能力声明的线格式兼容：旧主控的 {"ok":true} 解析为
// 「无能力」（降级发完即删），新主控才带 caps。协议只增不删，旧节点忽略 caps 不受影响。
func TestAuthOKCapabilityWireCompat(t *testing.T) {
	var oldMaster protocol.AuthOKPayload
	if err := mustMsg(t, protocol.MsgAuthOK, map[string]any{"ok": true}).PayloadTo(&oldMaster); err != nil {
		t.Fatalf("旧主控回执应可解析: %v", err)
	}
	if !oldMaster.OK {
		t.Fatal("旧主控回执 ok 应为 true")
	}
	if oldMaster.HasCap(protocol.CapTrafficAck) {
		t.Fatal("旧主控无 caps，不得判定为支持回执")
	}

	var newMaster protocol.AuthOKPayload
	if err := mustMsg(t, protocol.MsgAuthOK,
		protocol.AuthOKPayload{OK: true, Caps: []string{protocol.CapTrafficAck}}).PayloadTo(&newMaster); err != nil {
		t.Fatalf("新主控回执应可解析: %v", err)
	}
	if !newMaster.HasCap(protocol.CapTrafficAck) {
		t.Fatal("新主控应声明 traffic_ack 能力")
	}
	if newMaster.HasCap("unknown_cap") {
		t.Fatal("未声明的能力不得判定为支持")
	}
}
