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
type trafficKey struct {
	email   string
	inbound string
}

// Client 节点端客户端。
type Client struct {
	BaseURL         string // ws://host/api/v1/node/ws
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

	ws        *websocket.Conn
	writeMu   sync.Mutex // 保护 ws 写（心跳/上报/回执并发）
	pendingMu sync.Mutex
	pending   map[trafficKey]*pendingEntry // by (email, inboundTag) 维度键
	upgrading atomic.Bool                  // 面板触发升级进行中（防并发重复触发）

	// 运行时设置：主控 agent_settings 下发（仅当前会话生效，agent.yaml 为兜底）。
	// 三循环各自持有 ticker，经 reset channel 动态重建。
	settingsMu     sync.Mutex
	rtReport       time.Duration
	rtHeartbeat    time.Duration
	reportReset    chan time.Duration
	heartbeatReset chan time.Duration
	collectReset   chan time.Duration
}

// Run 常驻运行：流量采集上报 + 连接/服务/重连。
func (c *Client) Run(ctx context.Context) {
	c.pending = make(map[trafficKey]*pendingEntry)
	c.reportReset = make(chan time.Duration, 1)
	c.heartbeatReset = make(chan time.Duration, 1)
	c.collectReset = make(chan time.Duration, 1)
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
			if len(entries) == 0 {
				continue
			}
			total := int64(0)
			for _, e := range entries {
				total += e.Up + e.Down
			}
			if total > 0 {
				log.Printf("agent: 采集到流量 delta=%d 字节（%d 条）", total, len(entries))
			}
			c.pendingMu.Lock()
			for _, e := range entries {
				k := trafficKey{email: e.Email, inbound: e.Inbound}
				p, ok := c.pending[k]
				if !ok {
					p = &pendingEntry{}
					c.pending[k] = p
				}
				p.Up += e.Up
				p.Down += e.Down
			}
			c.pendingMu.Unlock()
		}
	}
}

// reportLoop 周期上报 pending；失败保留（重连后补报，不丢数据）。
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
		case <-ticker.C:
			c.reportPending()
		}
	}
}

func (c *Client) reportPending() {
	c.pendingMu.Lock()
	if len(c.pending) == 0 {
		c.pendingMu.Unlock()
		return
	}
	entries := make([]protocol.TrafficEntry, 0, len(c.pending))
	for k, p := range c.pending {
		entries = append(entries, protocol.TrafficEntry{
			UserID:    0, // 用户维度主控按 email 匹配；入站维度（Inbound 非空）主控只累计入站计数
			Email:     k.email,
			Inbound:   k.inbound,
			UpBytes:   p.Up,
			DownBytes: p.Down,
		})
	}
	period := time.Now().UTC().Format(time.RFC3339)
	// 清空 pending（发送失败再放回）
	c.pending = make(map[trafficKey]*pendingEntry)
	c.pendingMu.Unlock()

	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.ws == nil {
		c.restorePending(entries)
		return
	}
	ws := c.ws
	err := c.sendLocked(protocol.MsgTrafficReport, "", protocol.TrafficReportPayload{
		Entries: entries,
		Period:  period,
	})
	if err != nil {
		log.Printf("agent: traffic_report 发送失败，保留待补报: %v（主动断开触发重连）", err)
		c.restorePending(entries)
		_ = ws.Close() // 写失败说明连接已坏：关闭唤醒读循环，避免僵尸连接
	} else {
		log.Printf("agent: 已上报流量 %d 条（period=%s）", len(entries), period)
	}
}

// restorePending 上报失败后把数据放回 pending。
func (c *Client) restorePending(entries []protocol.TrafficEntry) {
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()
	for _, e := range entries {
		k := trafficKey{email: e.Email, inbound: e.Inbound}
		p, ok := c.pending[k]
		if !ok {
			p = &pendingEntry{}
			c.pending[k] = p
		}
		p.Up += e.UpBytes
		p.Down += e.DownBytes
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
		if err := c.Stats.SyncUsers(context.Background(), p.Users); err != nil {
			return &protocol.ResultPayload{OK: false, Error: err.Error()}
		}
		return &protocol.ResultPayload{OK: true, Data: "用户列表同步成功（gRPC 动态调整）"}
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
