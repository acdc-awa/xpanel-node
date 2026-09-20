// Package client 实现 Agent 到主控的 WebSocket 客户端：
// 认证、心跳、断线指数退避重连、指令处理与回执、流量采集上报。
package client

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"github.com/acdc-awa/xpanel-node/internal/agent/accounts"
	"github.com/acdc-awa/xpanel-node/internal/agent/certs"
	"github.com/acdc-awa/xpanel-node/internal/agent/collector"
	"github.com/acdc-awa/xpanel-node/internal/agent/outbox"
	"github.com/acdc-awa/xpanel-node/internal/agent/stats"
	"github.com/acdc-awa/xpanel-node/internal/agent/upgrade"
	"github.com/acdc-awa/xpanel-node/internal/agent/xrayproc"
	"github.com/acdc-awa/xpanel-node/pkg/protocol"
)

// pendingEntry 待上报的累积流量。
type pendingEntry struct {
	Up   int64
	Down int64
}

// trafficKey pending 聚合键：用户维度 (email, "")，入站维度 ("", tag)——两个维度
// 来自 xray 不同的计数器族，互不合并。
//
// cycleID 是**采集时刻**该用户的账期 ID（审计 F3）：账期切换后旧键与新键并存，切换前
// 采集到的增量留在旧键上，上报时仍带旧账期，不会被算进新套餐。
type trafficKey struct {
	email   string
	inbound string
	cycleID uint64
}

// Client 节点端客户端。
type Client struct {
	BaseURL         string // ws://<主控地址>/node/ws（连接时追加 ?node_id=）
	NodeID          string
	Secret          string
	Heartbeat       time.Duration
	ReconnectMax    time.Duration
	Xray            *xrayproc.Proc
	Collector       *collector.Collector
	Stats           *stats.Collector
	CollectInterval time.Duration
	ReportInterval  time.Duration
	// Phase T：内部账户存储 + 证书落盘目录
	Accounts *accounts.Store
	CertsDir string
	// Upgrade 升级源（repo/mirror 来自节点配置 update 段）；nil = 面板触发升级不可用。
	// SelfRestart 升级完成后的重启回调（systemd 服务内由 main 注入 systemctl restart）；
	// nil = 手动运行模式，替换后仅提示手动重启。
	Upgrade     *upgrade.Fetcher
	SelfRestart func() error
	// Outbox 持久化发件箱（审计 F1）：采集到的增量先落盘成不可变批次，收到主控
	// traffic_ack(ok) 才删批。nil 时 Run 内建纯内存发件箱（等价旧行为，仅供测试）。
	Outbox *outbox.Outbox

	ws        *websocket.Conn
	writeMu   sync.Mutex // 保护 ws 写（心跳/上报/回执并发）
	pendingMu sync.Mutex
	pending   map[trafficKey]*pendingEntry // by (email, inboundTag, cycleID) 维度键
	upgrading atomic.Bool                  // 面板触发升级进行中（防并发重复触发）

	// ackMode 主控是否支持流量批次落库回执（auth_ok 的 caps 声明，每次建连刷新）。
	// false = 旧主控：无回执可等，发完即删（等价旧 agent 行为）。
	// true  = 新主控：批次落库后才回执，收到 ok=true 才删批（至少一次投递）。
	ackMode atomic.Bool

	// sentThisConn 本连接上已发出、等待主控回执的批次 ID。
	// 同一条连接上每批只发一次：主控未回执时若每轮无脑重发，一旦对端是「不回执的主控」
	// （旧版本，或写库持续失败），同一批流量会被反复累加（旧主控小时桶 upsert 是加法
	// 且无批次去重），重复计量会直接算到用户账上。收到 ok=false 立即解锁重试，
	// 连接重建后整体清空（回执可能随旧连接一起丢失）。
	sentMu       sync.Mutex
	sentThisConn map[string]struct{}

	// cycles 每用户当前账期 ID（统计键 email → cycle_id），由主控 sync_users 下发（审计 F3）。
	// 主控切换周期（购买/续费/重置）后递增并立即推送；本端发现值变化即视为发生切换，
	// 立刻采集一次把切换前的增量按旧账期封账（见 flushOnCycleChange）。
	cycleMu sync.RWMutex
	cycles  map[string]uint64

	// 运行时设置：主控 agent_settings 下发（仅当前会话生效，agent.yaml 为兜底）。
	// 三循环各自持有 ticker，经 reset channel 动态重建。
	settingsMu     sync.Mutex
	rtReport       time.Duration
	rtHeartbeat    time.Duration
	reportReset    chan time.Duration
	heartbeatReset chan time.Duration
	collectReset   chan time.Duration
	// reportKick 立即触发一次上报（建连成功后补发积压、账期切换后尽快送达封账批次）
	reportKick chan struct{}
}

// Run 常驻运行：流量采集上报 + 连接/服务/重连。
func (c *Client) Run(ctx context.Context) {
	c.pending = make(map[trafficKey]*pendingEntry)
	c.cycles = make(map[string]uint64)
	c.sentThisConn = make(map[string]struct{})
	if c.Outbox == nil {
		// 未注入发件箱（测试/未配置路径）：退化为纯内存，行为与旧版一致（不落盘、不跨重启）
		c.Outbox = outbox.New("")
	}
	if err := c.Outbox.Load(); err != nil {
		// 损坏文件已在 Load 内隔离；此处仅告警，不阻断启动（拒绝启动会让节点彻底停报）
		log.Printf("agent: 流量发件箱加载告警: %v", err)
	}
	if b, e := c.Outbox.Pending(); b > 0 {
		log.Printf("agent: 从本地发件箱恢复 %d 个未确认流量批次（%d 条），待主控确认", b, e)
	}
	if !c.Outbox.Persistent() {
		log.Printf("agent: 警告——未配置发件箱路径，待上报流量不会落盘（重启即丢）")
	}
	c.reportReset = make(chan time.Duration, 1)
	c.heartbeatReset = make(chan time.Duration, 1)
	c.collectReset = make(chan time.Duration, 1)
	c.reportKick = make(chan struct{}, 1)
	go c.collectLoop(ctx)
	go c.reportLoop(ctx)

	backoff := time.Second
	for {
		err := c.connectAndServe(ctx, &backoff)
		if err != nil {
			log.Printf("agent: 连接断开: %v（%s 后重连）", err, backoff)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		// 指数退避 (带抖动)
		backoff = time.Duration(float64(backoff) * 1.5)
		jitter := time.Duration((float64(backoff) * 0.2) * (float64(time.Now().UnixNano()%100) / 100.0))
		backoff += jitter
		if backoff > c.ReconnectMax {
			backoff = c.ReconnectMax
		}
	}
}

// collectLoop 周期性采集 xray stats 并累积到 pending。
func (c *Client) collectLoop(ctx context.Context) {
	ticker := time.NewTicker(c.effectiveCollect())
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case d := <-c.collectReset:
			ticker.Reset(d)
			continue
		case <-ticker.C:
			entries, err := c.Stats.Collect(ctx)
			if err != nil {
				log.Printf("agent: stats 采集失败: %v", err)
				c.Stats.Close()
				continue
			}
			c.accumulate(entries)
		}
	}
}

// accumulate 把采集到的增量按 (email, inbound, 采集时刻账期) 累积到 pending。
// 账期 ID 在此刻取值（审计 F3）：这是「这笔字节属于哪个账期」的唯一判定点——切换周期后
// 主控推来的新 ID 只影响此后采集的增量，已经在 pending 里的旧账期增量不受影响。
func (c *Client) accumulate(entries []stats.Entry) {
	if len(entries) == 0 {
		return
	}
	total := int64(0)
	for _, e := range entries {
		total += e.Up + e.Down
	}
	if total > 0 {
		log.Printf("agent: 采集到流量 delta=%d 字节（%d 条）", total, len(entries))
	}
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()
	for _, e := range entries {
		k := trafficKey{email: e.Email, inbound: e.Inbound, cycleID: c.cycleFor(e.Email)}
		p, ok := c.pending[k]
		if !ok {
			p = &pendingEntry{}
			c.pending[k] = p
		}
		p.Up += e.Up
		p.Down += e.Down
	}
}

// cycleFor 取某统计键（email）当前账期 ID（未下发/入站维度条目返回 0）。
func (c *Client) cycleFor(email string) uint64 {
	if email == "" {
		return 0 // 入站维度条目无用户账期
	}
	c.cycleMu.RLock()
	defer c.cycleMu.RUnlock()
	return c.cycles[email]
}

// cycleChanges 只读比对：返回主控名单里「账期与本地记录不同」的用户统计键。
// 新出现的用户（本地无记录）不算变化——它没有需要封账的旧增量。
func (c *Client) cycleChanges(users map[string][]protocol.User) []string {
	c.cycleMu.RLock()
	defer c.cycleMu.RUnlock()
	var changed []string
	for _, list := range users {
		for _, u := range list {
			if u.Email == "" || u.CycleID == 0 {
				continue // 旧主控不下发账期（0），不做切换处理
			}
			if old, ok := c.cycles[u.Email]; ok && old != u.CycleID {
				changed = append(changed, u.Email)
			}
		}
	}
	return changed
}

// applyCycles 用主控名单刷新账期映射（必须在封账采集**之后**调用，否则封账增量会被打上新账期）。
func (c *Client) applyCycles(users map[string][]protocol.User) {
	c.cycleMu.Lock()
	defer c.cycleMu.Unlock()
	for _, list := range users {
		for _, u := range list {
			if u.Email == "" || u.CycleID == 0 {
				continue
			}
			c.cycles[u.Email] = u.CycleID
		}
	}
}

// sealCycle 账期切换封账（审计 F3）：立即采集一次并累积到 pending。
//
// 调用时机在 applyCycles **之前**，因此这次采集出来的增量带的是**旧账期** ID——正是我们要
// 的语义：切换前已经产生、但还没被上报的流量归旧账期，此后采集的才归新账期。分割点在
// 收到新名单的瞬间（≈主控提交切换的时刻 + 推送延迟），比只按上报周期（默认 60s）切分精确得多。
//
// 同步执行（在消息循环 goroutine 上）：账期映射的更新必须晚于封账采集，异步会引入竞态。
// 采集内部超时 5s，主控 SyncUsers 的 Ask 超时 30s，不会撞上。
func (c *Client) sealCycle(changed []string) {
	if c.Stats == nil {
		return
	}
	entries, err := c.Stats.Collect(context.Background())
	if err != nil {
		// 采集失败：无法精确封账，但仍要更新账期映射（否则后续增量继续按旧账期上报）。
		// 此时切换点前后的增量会一起落到旧账期——少计新套餐，方向安全（不会超收）。
		log.Printf("agent: 账期切换封账采集失败（%d 个用户），切换点前后的增量将按旧账期结算: %v", len(changed), err)
		return
	}
	c.accumulate(entries)
	if len(entries) > 0 {
		log.Printf("agent: 账期切换封账：%d 个用户账期变更，已按旧账期采集 %d 条增量", len(changed), len(entries))
	}
}

// reportLoop 周期上报：把 pending 封成持久化批次并发送未确认批次；失败保留（重连后补报）。
func (c *Client) reportLoop(ctx context.Context) {
	ticker := time.NewTicker(c.effectiveReport())
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case d := <-c.reportReset:
			ticker.Reset(d)
			continue
		case <-c.reportKick:
			// 建连成功/账期切换后的即时补发：不重置 ticker，只多做一轮
			c.reportPending()
		case <-ticker.C:
			c.reportPending()
		}
	}
}

// kickReport 非阻塞触发一次立即上报。
func (c *Client) kickReport() {
	select {
	case c.reportKick <- struct{}{}:
	default: // 已有待处理的 kick，无需重复
	}
}

// maxBatchesPerTick 单轮最多发送的批次数：长时间离线后积压可能有上千批，一次性全推会在
// 单个连接上形成突发；每轮（默认 60s）推 20 批，积压 1 天约 24 分钟排空，且下一轮 tick
// 会继续推进。不设上限的话，突发反而更容易触发主控侧写库竞争。
const maxBatchesPerTick = 20

// reportPending 把内存 pending 封成持久化批次，然后按 FIFO 发送未确认批次。
//
// 关键不变量：**批次的 Entries 一旦创建不可再变**——主控按 BatchID 去重，若同一 ID 两次
// 投递内容不同，后加的部分会被去重丢掉。因此新采集的数据一律进新批次，绝不并入已建批次。
// 这也是「删批只认主控 ACK」的前提：没收到 ACK 就重发，重复由主控去重吸收。
func (c *Client) reportPending() {
	c.flushPendingToOutbox()
	c.drainOutbox()
}

// flushPendingToOutbox 把内存 pending 落盘为一批（落盘失败则原样放回，下轮重试）。
func (c *Client) flushPendingToOutbox() {
	c.pendingMu.Lock()
	if len(c.pending) == 0 {
		c.pendingMu.Unlock()
		return
	}
	entries := make([]outbox.Entry, 0, len(c.pending))
	for k, p := range c.pending {
		entries = append(entries, outbox.Entry{
			Email:   k.email,
			Inbound: k.inbound,
			Up:      p.Up,
			Down:    p.Down,
			CycleID: k.cycleID,
		})
	}
	// 先清空再落盘：落盘失败要把这批放回（与旧 restorePending 语义一致）
	c.pending = make(map[trafficKey]*pendingEntry)
	c.pendingMu.Unlock()

	if _, ok, err := c.Outbox.Append(entries); err != nil {
		log.Printf("agent: 流量批次落盘失败，保留待下轮重试: %v", err)
		c.restorePending(entries)
	} else if !ok {
		c.restorePending(entries) // 空批次（理论不可达）：放回避免丢数据
	}
}

// drainOutbox 发送未确认批次（FIFO，每轮上限 maxBatchesPerTick）。
// 发送失败不删批、不重排：留在队首下轮重发。连接已坏时主动断开触发重连。
func (c *Client) drainOutbox() {
	if dropped := c.Outbox.DropExpired(time.Now()); dropped > 0 {
		log.Printf("agent: 警告——发件箱丢弃 %d 个超龄/超量批次（超出保留期，主控不会收到这批流量）", dropped)
	}
	if !c.connected() {
		// 未连接：批次留在发件箱即可（这是它存在的意义），不必每轮刷失败日志；
		// 重连后 connectAndServe 会 kickReport 立即补发。
		return
	}
	ackMode := c.ackMode.Load()
	sent := 0
	for i := 0; i < maxBatchesPerTick; i++ {
		// 等回执模式跳过本连接已发出的批次（拿下一个未发的），避免无谓重发；
		// 降级模式批次发完即删，队列里不会留下已发未删的项，跳过集合恒为空。
		b, ok := c.Outbox.OldestExcept(c.awaitingAck)
		if !ok {
			break
		}
		if err := c.sendBatch(b); err != nil {
			log.Printf("agent: traffic_report 发送失败，批次保留待补报 (batch=%s seq=%d): %v", b.ID, b.Seq, err)
			c.TriggerReconnect() // 写失败说明连接已坏：关闭唤醒读循环，避免僵尸连接
			return
		}
		sent++
		if !ackMode {
			// 旧主控没有落库回执：发完即删，与旧 agent 行为一致。绝不能留着重发——
			// 旧主控小时桶 upsert 是加法且无批次去重，重发会把同一批流量重复计到用户账上。
			if _, err := c.Outbox.Ack(b.ID); err != nil {
				log.Printf("agent: 删除已发送批次失败 (batch=%s): %v（下轮会重发，旧主控无去重）", b.ID, err)
			}
			continue
		}
		c.markSent(b.ID)
		log.Printf("agent: 已上报流量批次 %s（seq=%d, %d 条, %d 字节），等待主控回执",
			b.ID, b.Seq, len(b.Entries), b.Up()+b.Down())
	}
	if batches, _ := c.Outbox.Pending(); batches > 0 {
		if ackMode {
			log.Printf("agent: 发件箱仍有 %d 个批次未确认（本轮发出 %d 个，其余已发出待回执）", batches, sent)
		} else {
			log.Printf("agent: 发件箱仍有 %d 个批次待发送，下轮继续", batches)
		}
	}
}

// markSent 记录本连接已发出某批次（同连接内不再重发，等主控回执）。
func (c *Client) markSent(batchID string) {
	c.sentMu.Lock()
	c.sentThisConn[batchID] = struct{}{}
	c.sentMu.Unlock()
}

// clearSent 解除「已发出待回执」标记，允许下轮重发：主控明确回绝（ok=false）或已确认删除时调用。
func (c *Client) clearSent(batchID string) {
	c.sentMu.Lock()
	delete(c.sentThisConn, batchID)
	c.sentMu.Unlock()
}

// resetSent 清空标记（每次建连后调用）：旧连接上未收到回执的批次需要在新连接上重发，
// 重复由主控去重吸收。
func (c *Client) resetSent() {
	c.sentMu.Lock()
	c.sentThisConn = make(map[string]struct{})
	c.sentMu.Unlock()
}

// awaitingAck 该批次是否已在本连接发出、正在等主控回执。
func (c *Client) awaitingAck(batchID string) bool {
	c.sentMu.Lock()
	defer c.sentMu.Unlock()
	_, ok := c.sentThisConn[batchID]
	return ok
}

// connected 当前是否持有可用连接。
func (c *Client) connected() bool {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.ws != nil
}

// sendBatch 发送单个批次（不等待回执；回执由读循环的 traffic_ack 处理）。
func (c *Client) sendBatch(b outbox.Batch) error {
	entries := make([]protocol.TrafficEntry, 0, len(b.Entries))
	for _, e := range b.Entries {
		entries = append(entries, protocol.TrafficEntry{
			UserID:    0, // 用户维度主控按 email 匹配；入站维度（Inbound 非空）主控只累计入站计数
			Email:     e.Email,
			Inbound:   e.Inbound,
			UpBytes:   e.Up,
			DownBytes: e.Down,
			CycleID:   e.CycleID,
		})
	}
	return c.send(protocol.MsgTrafficReport, "", protocol.TrafficReportPayload{
		Entries: entries,
		Period:  time.Now().UTC().Format(time.RFC3339),
		BatchID: b.ID,
		Seq:     b.Seq,
		BootID:  b.BootID,
	})
}

// restorePending 落盘失败后把数据放回 pending（键含账期，放回不改变归属）。
func (c *Client) restorePending(entries []outbox.Entry) {
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()
	for _, e := range entries {
		k := trafficKey{email: e.Email, inbound: e.Inbound, cycleID: e.CycleID}
		p, ok := c.pending[k]
		if !ok {
			p = &pendingEntry{}
			c.pending[k] = p
		}
		p.Up += e.Up
		p.Down += e.Down
	}
}

func (c *Client) wsURL() string {
	u, _ := url.Parse(c.BaseURL)
	q := u.Query()
	q.Set("node_id", c.NodeID)
	u.RawQuery = q.Encode()
	return u.String()
}

// connectAndServe 建立连接并进入消息循环（返回时连接已关闭）。
func (c *Client) connectAndServe(ctx context.Context, backoff *time.Duration) error {
	ws, _, err := websocket.DefaultDialer.Dial(c.wsURL(), nil)
	if err != nil {
		return err
	}
	c.writeMu.Lock()
	c.ws = ws
	c.writeMu.Unlock()
	defer func() {
		c.writeMu.Lock()
		c.ws = nil
		c.writeMu.Unlock()
		_ = ws.Close()
	}()
	ws.SetReadLimit(1024 * 1024)

	// 认证
	if err := c.send(protocol.MsgAuth, "", protocol.AuthPayload{NodeID: c.NodeID, Secret: c.Secret}); err != nil {
		return err
	}
	_ = ws.SetReadDeadline(time.Now().Add(10 * time.Second))
	_, data, err := ws.ReadMessage()
	if err != nil {
		return err
	}
	msg, err := protocol.Decode(data)
	if err != nil || msg.Type != protocol.MsgAuthOK {
		return errAuthRejected
	}
	// 能力协商：主控声明 CapTrafficAck 才等回执，否则降级发完即删（旧主控没有回执，
	// 等下去只会让批次永远留在发件箱里反复重发，反而被旧主控重复计量）。
	var authOK protocol.AuthOKPayload
	if err := msg.PayloadTo(&authOK); err != nil {
		log.Printf("agent: 认证回执载荷解析失败（按旧主控处理，流量发完即删）: %v", err)
	}
	c.ackMode.Store(authOK.HasCap(protocol.CapTrafficAck))
	c.resetSent()
	const (
		// 主控 ping 间隔 54s（PongWait*9/10），90s 只靠 ping 续命、留足网络抖动余量
		pongWait = 90 * time.Second
	)
	_ = ws.SetReadDeadline(time.Now().Add(pongWait))
	ws.SetPingHandler(func(appData string) error {
		_ = ws.SetReadDeadline(time.Now().Add(pongWait))
		c.writeMu.Lock()
		defer c.writeMu.Unlock()
		if c.ws != nil {
			_ = c.ws.SetWriteDeadline(time.Now().Add(writeTimeout))
			return c.ws.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(writeTimeout))
		}
		return nil
	})
	ws.SetPongHandler(func(string) error {
		_ = ws.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})
	log.Printf("agent: 已连上主控（node=%s）", c.NodeID)

	// 连接成功，重置退避时间
	*backoff = time.Second

	// ctx 取消时关闭连接，中断阻塞中的 ReadMessage（否则 SIGTERM 无法退出）
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			_ = ws.Close()
		case <-done:
		}
	}()

	// 心跳
	hbCtx, hbCancel := context.WithCancel(ctx)
	defer hbCancel()
	go c.heartbeatLoop(hbCtx, ws)

	// 建连即补发积压（离线期间落盘的批次 + 上次未确认的批次），不必等下一个上报周期
	c.kickReport()

	// 消息循环
	for {
		_, data, err := ws.ReadMessage()
		if err != nil {
			return err
		}
		m, err := protocol.Decode(data)
		if err != nil {
			continue
		}
		c.handle(m)
	}
}

var errAuthRejected = &wsError{msg: "认证被拒绝"}

// writeTimeout 单次 WebSocket 写超时（心跳/上报/回执/pong 统一使用）。
const writeTimeout = 10 * time.Second

type wsError struct{ msg string }

func (e *wsError) Error() string { return e.msg }

// send 写消息（并发安全）。
func (c *Client) send(typ, id string, payload any) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.ws == nil {
		return errNoConn
	}
	return c.sendLocked(typ, id, payload)
}

var errNoConn = &wsError{msg: "未连接"}

// sendLocked 写消息（调用方需持有 writeMu）。
// gorilla 的写 deadline 是连接级持久状态（pong 回写的 WriteControl 设定后不复原），
// 数据写必须每次显式重设，否则会继承 pong 留下的 10 秒绝对时间点——过期后所有
// 心跳/上报瞬间失败且心跳循环静默退出，连接退化为只剩控制帧的僵尸并被中间层回收。
func (c *Client) sendLocked(typ, id string, payload any) error {
	data, err := protocol.Encode(typ, id, payload)
	if err != nil {
		return err
	}
	_ = c.ws.SetWriteDeadline(time.Now().Add(writeTimeout))
	return c.ws.WriteMessage(websocket.TextMessage, data)
}

func (c *Client) heartbeatLoop(ctx context.Context, ws *websocket.Conn) {
	ticker := time.NewTicker(c.effectiveHeartbeat())
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case d := <-c.heartbeatReset:
			ticker.Reset(d)
			continue
		case <-ticker.C:
			snap := c.Collector.Snapshot()
			onlineUsers := 0
			var onlineIPs []protocol.OnlineUserIPs
			if c.Stats != nil {
				onlineUsers = c.Stats.OnlineUsers()
				for _, u := range c.Stats.OnlineSnapshot() {
					onlineIPs = append(onlineIPs, protocol.OnlineUserIPs{Email: u.Email, IPs: u.IPs})
				}
			}
			hb := protocol.HeartbeatPayload{
				CPU:         snap.CPU,
				Mem:         snap.Mem,
				MemTotal:    snap.MemTotal,
				Disk:        snap.Disk,
				DiskTotal:   snap.DiskTotal,
				XrayRunning: c.Xray.IsRunning(),
				OnlineUsers: onlineUsers,
				OnlineIPs:   onlineIPs,
				RxRate:      snap.RxRate,
				TxRate:      snap.TxRate,
				RxBytes:     snap.RxBytes,
				TxBytes:     snap.TxBytes,
				Version:     upgrade.Version,
				TS:          time.Now().Unix(),
			}
			if err := c.send(protocol.MsgHeartbeat, "", hb); err != nil {
				// 写失败说明连接已坏：主动关闭唤醒阻塞中的读循环，让主循环立即重连，
				// 而不是留下一条只进不出的僵尸连接（读侧要等 ping 超时才会发现）
				log.Printf("agent: 心跳发送失败: %v（主动断开触发重连）", err)
				_ = ws.Close()
				return
			}
		}
	}
}

// 运行时设置的合法区间：过小会打爆 WS 与主控落库，过大失去近实时语义。
const (
	minSettingsInterval = 5 * time.Second
	maxSettingsInterval = 30 * time.Minute
)

func clampInterval(d time.Duration) time.Duration {
	if d < minSettingsInterval {
		return minSettingsInterval
	}
	if d > maxSettingsInterval {
		return maxSettingsInterval
	}
	return d
}

// effectiveReport 当前生效的上报周期（远程下发优先，agent.yaml 兜底）。
func (c *Client) effectiveReport() time.Duration {
	c.settingsMu.Lock()
	defer c.settingsMu.Unlock()
	if c.rtReport > 0 {
		return c.rtReport
	}
	return c.ReportInterval
}

// effectiveHeartbeat 当前生效的心跳周期（远程下发优先，agent.yaml 兜底）。
func (c *Client) effectiveHeartbeat() time.Duration {
	c.settingsMu.Lock()
	defer c.settingsMu.Unlock()
	if c.rtHeartbeat > 0 {
		return c.rtHeartbeat
	}
	return c.Heartbeat
}

// effectiveCollect 生效采集周期 = min(yaml 配置, 生效上报周期)，
// 防上报快于采集造成的增量粒度倒挂。
func (c *Client) effectiveCollect() time.Duration {
	cd := c.CollectInterval
	if r := c.effectiveReport(); r < cd {
		cd = r
	}
	return cd
}

// applyAgentSettings 应用主控下发的运行时设置并通知三个循环重建 ticker
// （远程设置仅当前会话生效，不写回 agent.yaml）。返回变更摘要（回执 data）。
func (c *Client) applyAgentSettings(p protocol.AgentSettingsPayload) string {
	c.settingsMu.Lock()
	var changed []string
	if p.ReportIntervalSec > 0 {
		d := clampInterval(time.Duration(p.ReportIntervalSec) * time.Second)
		if d != c.rtReport {
			changed = append(changed, fmt.Sprintf("上报周期→%s", d))
		}
		c.rtReport = d
	}
	if p.HeartbeatIntervalSec > 0 {
		d := clampInterval(time.Duration(p.HeartbeatIntervalSec) * time.Second)
		if d != c.rtHeartbeat {
			changed = append(changed, fmt.Sprintf("心跳周期→%s", d))
		}
		c.rtHeartbeat = d
	}
	c.settingsMu.Unlock()
	if len(changed) == 0 {
		return "设置无变更"
	}
	log.Printf("agent: 应用主控运行时设置: %s", strings.Join(changed, "，"))
	// 非阻塞通知对应循环重建 ticker（缓冲 1；循环忙时丢弃，下次下发兜底）
	select {
	case c.reportReset <- c.effectiveReport():
	default:
	}
	select {
	case c.collectReset <- c.effectiveCollect():
	default:
	}
	select {
	case c.heartbeatReset <- c.effectiveHeartbeat():
	default:
	}
	return strings.Join(changed, "，")
}

// handle 处理主控指令并回执。
func (c *Client) handle(m *protocol.Message) {
	res := c.dispatch(m)
	if res == nil {
		return // 不认识的类型不回应
	}
	_ = c.send(protocol.MsgResult, m.ID, *res)
}

// dispatch 按消息类型处理指令，返回回执负载；未知类型返回 nil。
func (c *Client) dispatch(m *protocol.Message) *protocol.ResultPayload {
	switch m.Type {
	case protocol.MsgPushConfig:
		var p protocol.PushConfigPayload
		if err := m.PayloadTo(&p); err != nil {
			return &protocol.ResultPayload{OK: false, Error: "解析 push_config 失败"}
		}
		if err := c.Xray.RestartWithConfig(p.ConfigJSON); err != nil {
			return &protocol.ResultPayload{OK: false, Error: err.Error()}
		}
		if c.Stats != nil {
			c.Stats.ResetUsers()
		}
		return &protocol.ResultPayload{OK: true, Data: "xray 已按新配置重启"}
	case protocol.MsgSyncUsers:
		var p protocol.SyncUsersPayload
		if err := m.PayloadTo(&p); err != nil {
			return &protocol.ResultPayload{OK: false, Error: "解析 sync_users 失败: " + err.Error()}
		}
		if c.Stats == nil {
			return &protocol.ResultPayload{OK: false, Error: "统计采集器未初始化"}
		}
		// 账期切换封账（审计 F3）：顺序不可颠倒——先按旧账期采集封账，再更新本地账期映射。
		// 反过来的话这次采集的增量会被打上新账期，切换前的消费就落到新套餐头上了。
		if changed := c.cycleChanges(p.Users); len(changed) > 0 {
			c.sealCycle(changed)
			c.kickReport() // 封账批次尽快送达（不必等下一个上报周期）
		}
		c.applyCycles(p.Users)
		if err := c.Stats.SyncUsers(context.Background(), p.Users); err != nil {
			return &protocol.ResultPayload{OK: false, Error: err.Error()}
		}
		return &protocol.ResultPayload{OK: true, Data: "用户列表同步成功（gRPC 动态调整）"}
	case protocol.MsgTrafficAck:
		// 主控流量批次落库回执（审计 F1）：只有 ok=true 才删本地批次。
		// 返回 nil 表示这不是「指令」，不回 result 帧。
		var p protocol.TrafficAckPayload
		if err := m.PayloadTo(&p); err != nil {
			return nil
		}
		if !p.OK {
			// 主控明确回绝（写库失败等）：解除「已发出」标记让下轮重试，错误文本仅供日志定位
			c.clearSent(p.BatchID)
			log.Printf("agent: 主控未确认流量批次 %s: %s（保留待重发）", p.BatchID, p.Error)
			return nil
		}
		hit, err := c.Outbox.Ack(p.BatchID)
		c.clearSent(p.BatchID)
		if err != nil {
			log.Printf("agent: 删除已确认批次失败 (batch=%s): %v（将重发，由主控去重吸收）", p.BatchID, err)
			return nil
		}
		if hit {
			if batches, entries := c.Outbox.Pending(); batches > 0 {
				log.Printf("agent: 流量批次 %s 已确认，发件箱剩余 %d 批（%d 条）", p.BatchID, batches, entries)
			} else {
				log.Printf("agent: 流量批次 %s 已确认，发件箱已清空", p.BatchID)
			}
		}
		return nil
	case protocol.MsgRestartXray:
		if err := c.Xray.Stop(); err != nil {
			return &protocol.ResultPayload{OK: false, Error: err.Error()}
		}
		if err := c.Xray.Start(); err != nil {
			return &protocol.ResultPayload{OK: false, Error: err.Error()}
		}
		if c.Stats != nil {
			c.Stats.ResetUsers()
		}
		return &protocol.ResultPayload{OK: true, Data: "xray 已重启"}
	case protocol.MsgGetStatus:
		running, pid, startedAt, uptime := c.Xray.Status()
		return &protocol.ResultPayload{OK: true, Data: protocol.StatusData{
			XrayRunning: running,
			Pid:         pid,
			UptimeSec:   uptime,
			ConfigPath:  c.Xray.ConfigPath,
			StartedAt:   startedAt,
		}}
	case protocol.MsgGetLogs:
		var p protocol.GetLogsPayload
		_ = m.PayloadTo(&p)
		logs, err := c.Xray.Logs(p.Lines)
		if err != nil {
			return &protocol.ResultPayload{OK: false, Error: "读取日志失败: " + err.Error()}
		}
		return &protocol.ResultPayload{OK: true, Data: logs}
	case protocol.MsgSetupInternalAccount, protocol.MsgRotateInternalAccount:
		return c.handleInternalAccount(m, m.Type == protocol.MsgRotateInternalAccount)
	case protocol.MsgPushCert:
		return c.handlePushCert(m)
	case protocol.MsgUpgradeAgent:
		return c.handleUpgradeAgent(m)
	case protocol.MsgAgentSettings:
		var p protocol.AgentSettingsPayload
		if err := m.PayloadTo(&p); err != nil {
			return &protocol.ResultPayload{OK: false, Error: "解析 agent_settings 失败: " + err.Error()}
		}
		return &protocol.ResultPayload{OK: true, Data: c.applyAgentSettings(p)}
	default:
		return nil
	}
}

// handleUpgradeAgent 面板触发的自升级：版本检查与下载/替换全部移入后台 goroutine
// 执行（外部网络请求可达数十秒至数分钟，绝不能阻塞 WebSocket 读循环导致心跳/pong
// 停摆被主控回收）。成功路径在回执刷出后延时触发重启。
func (c *Client) handleUpgradeAgent(m *protocol.Message) *protocol.ResultPayload {
	if c.Upgrade == nil {
		return &protocol.ResultPayload{OK: false, Error: "服务器未配置升级源（update.repo/mirror）"}
	}
	var p protocol.UpgradeAgentPayload
	_ = m.PayloadTo(&p)
	if !c.upgrading.CompareAndSwap(false, true) {
		return &protocol.ResultPayload{OK: false, Error: "升级正在进行中，请稍候"}
	}
	go c.runUpgrade(m.ID, p.Target)
	return nil // 回执由 runUpgrade 在版本检查/下载/替换完成后发送
}

// runUpgrade 后台执行查询 → 下载 → sha256 校验 → 原子替换，随后先发回执再延时触发重启。
func (c *Client) runUpgrade(reqID, target string) {
	defer c.upgrading.Store(false)
	from := upgrade.CurrentVersion()

	report := func(phase, msg, errStr string) {
		p := protocol.UpgradeProgressPayload{
			Phase:   phase,
			Target:  target,
			Message: msg,
			Error:   errStr,
			TS:      time.Now().Unix(),
		}
		_ = c.send(protocol.MsgUpgradeProgress, "", p)
	}

	reply := func(res protocol.ResultPayload) {
		if err := c.send(protocol.MsgResult, reqID, res); err != nil {
			log.Printf("agent: upgrade_agent 回执发送失败: %v", err)
		}
	}

	report("checking", "正在查询最新版本...", "")
	if target == "" {
		latest, err := c.Upgrade.Latest()
		if err != nil {
			errText := "查询最新版本失败: " + err.Error()
			report("failed", "查询最新版本失败", errText)
			reply(protocol.ResultPayload{OK: false, Error: errText})
			return
		}
		target = latest
	}
	if upgrade.Compare(from, target) >= 0 {
		report("success", fmt.Sprintf("已是最新版本 %s（远端 %s），无需升级", from, target), "")
		reply(protocol.ResultPayload{OK: true, Data: fmt.Sprintf("已是最新版本 %s（远端 %s）", from, target)})
		return
	}

	exe, err := os.Executable()
	if err != nil {
		errText := "获取自身路径失败: " + err.Error()
		report("failed", "获取自身路径失败", errText)
		reply(protocol.ResultPayload{OK: false, Error: errText})
		return
	}

	report("downloading", fmt.Sprintf("正在从 GitHub 下载版本 %s 二进制与校验和...", target), "")
	data, wantSum, err := c.Upgrade.Download(target)
	if err != nil {
		errText := "下载安装包失败: " + err.Error()
		report("failed", "下载安装包失败", errText)
		reply(protocol.ResultPayload{OK: false, Error: errText})
		return
	}

	report("verifying", "正在校验 sha256 完整性...", "")
	if !strings.EqualFold(wantSum, upgrade.Sha256Hex(data)) {
		errText := fmt.Sprintf("sha256 校验失败: 声明 %s 实际 %s", wantSum, upgrade.Sha256Hex(data))
		report("failed", "sha256 校验失败", errText)
		reply(protocol.ResultPayload{OK: false, Error: errText})
		return
	}

	report("replacing", "正在替换二进制文件...", "")
	if err := upgrade.ReplaceBinary(exe, data); err != nil {
		errText := "替换二进制失败: " + err.Error()
		report("failed", "替换失败", errText)
		reply(protocol.ResultPayload{OK: false, Error: errText})
		return
	}
	log.Printf("agent: 二进制已升级 %s → %s（%s）", from, target, exe)

	if c.SelfRestart == nil {
		report("success", fmt.Sprintf("已升级 %s → %s（手动模式，请手动重启 agent）", from, target), "")
		reply(protocol.ResultPayload{OK: true, Data: fmt.Sprintf("已升级 %s → %s；当前为手动运行模式，请手动重启 agent 进程加载新版本", from, target)})
		return
	}

	report("restarting", fmt.Sprintf("已升级 %s → %s，服务器正在重启…", from, target), "")
	reply(protocol.ResultPayload{OK: true, Data: fmt.Sprintf("已升级 %s → %s，服务器正在重启", from, target)})
	// 回执已写出：延时 1 秒确保 TCP 缓冲区将回执刷至主控，再触发 systemctl restart 杀掉自己
	time.Sleep(1 * time.Second)
	if err := c.SelfRestart(); err != nil {
		log.Printf("agent: 自重启失败（新二进制已就位，可手动 systemctl restart xray-agent）: %v", err)
	}
}

// TriggerReconnect 主动断开当前长连接以触发指数退避外的立即重连（如 Xray 重启后向主控重新对齐用户）。
func (c *Client) TriggerReconnect() {
	c.writeMu.Lock()
	ws := c.ws
	c.writeMu.Unlock()
	if ws != nil {
		_ = ws.Close()
	}
}

// handleInternalAccount setup/rotate：节点生成 UUID → 原子持久化 → 回执。
// setup 幂等（已有则复用）；rotate 强制重新生成。
func (c *Client) handleInternalAccount(m *protocol.Message, force bool) *protocol.ResultPayload {
	if c.Accounts == nil {
		return &protocol.ResultPayload{OK: false, Error: "内部账户存储未初始化"}
	}
	var p protocol.SetupInternalAccountPayload
	if err := m.PayloadTo(&p); err != nil {
		return &protocol.ResultPayload{OK: false, Error: "解析 internal_account 失败: " + err.Error()}
	}
	if p.Tag == "" {
		return &protocol.ResultPayload{OK: false, Error: "缺少 tag"}
	}
	if !force {
		if m, err := c.Accounts.Load(); err == nil {
			if uuid, ok := m[p.Tag]; ok && uuid != "" {
				return &protocol.ResultPayload{OK: true, Data: protocol.SetupInternalResult{Tag: p.Tag, UUID: uuid}}
			}
		}
	}
	uuid, err := accounts.NewUUID()
	if err != nil {
		return &protocol.ResultPayload{OK: false, Error: "生成 UUID 失败: " + err.Error()}
	}
	if err := c.Accounts.Set(p.Tag, uuid); err != nil {
		return &protocol.ResultPayload{OK: false, Error: "持久化失败: " + err.Error()}
	}
	return &protocol.ResultPayload{OK: true, Data: protocol.SetupInternalResult{Tag: p.Tag, UUID: uuid}}
}

// handlePushCert 证书下发：校验 PEM 匹配 → 原子落盘 → 回执。
func (c *Client) handlePushCert(m *protocol.Message) *protocol.ResultPayload {
	if c.CertsDir == "" {
		return &protocol.ResultPayload{OK: false, Error: "证书目录未配置"}
	}
	var p protocol.PushCertPayload
	if err := m.PayloadTo(&p); err != nil {
		return &protocol.ResultPayload{OK: false, Error: "解析 push_cert 失败: " + err.Error()}
	}
	if p.Domain == "" {
		return &protocol.ResultPayload{OK: false, Error: "缺少 domain"}
	}
	if err := certs.Write(c.CertsDir, p.Domain, p.CertPEM, p.KeyPEM); err != nil {
		return &protocol.ResultPayload{OK: false, Error: err.Error()}
	}
	return &protocol.ResultPayload{OK: true, Data: "证书已落盘 " + certs.SanitizeDomain(p.Domain)}
}
