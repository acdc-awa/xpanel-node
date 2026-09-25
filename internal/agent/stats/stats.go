// Package stats 通过 Xray gRPC StatsService 采集用户流量，并支持 HandlerService 动态更新用户。
// 实现模型对照《xray-api-探索.md》§4（3x-ui GetTraffic 轮询模式）：
// QueryStats 全量拉取 → 维护 last 基线 → 差值 = 本周期增量（xray 重启归零视为 0）。
package stats

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	handlerService "github.com/xtls/xray-core/app/proxyman/command"
	statsService "github.com/xtls/xray-core/app/stats/command"
	xrayProtocol "github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/proxy/vless"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/acdc-awa/xpanel-node/pkg/protocol"
)

// Entry 单条流量增量（用户维度 Email / 入站维度 Inbound 二选一）。
type Entry struct {
	Email   string
	Inbound string
	Up      int64
	Down    int64
}

// user 计数器命名：user>>>email>>>traffic>>>(uplink|downlink)
var userRe = regexp.MustCompile(`^user>>>(.+?)>>>traffic>>>(uplink|downlink)$`)

// inbound 计数器命名：inbound>>>tag>>>traffic>>>(uplink|downlink)
// （主控模板 policy.system.statsInboundUplink/Downlink 开启后产出）。
var inboundRe = regexp.MustCompile(`^inbound>>>(.+?)>>>traffic>>>(uplink|downlink)$`)

// panelUserEmailRe 面板下发用户邮箱格式（主控 xray.UserEmailFor：u<uid>.i<iid>@panel.local）。
// 只认这一种：relay 入站的内部账户（relay-<tag>@panel.local）不是「用户」，一旦进入缓存，
// SyncUsers 末尾的 tag 清理会把内部账户从运行中的 xray 里摘掉，转发鉴权当场失效。
var panelUserEmailRe = regexp.MustCompile(`(?i)^u\d+\.i\d+@panel\.local$`)

// OnlineUser 单个用户的在线快照：当前活跃连接的去重源 IP 列表。
type OnlineUser struct {
	Email string
	IPs   []string
	// LastSeen 每个 IP 最近一次建连时刻（unix 秒，透传 xray OnlineMap 的 lastSeen）。
	LastSeen map[string]int64
}

// maxOnlineStale 在线快照的最大可信年龄：距上次成功刷新超过该时长（xray 卡死但进程
// 还在、API 持续无响应）就不再上报残影，宁可显示空也不显示冻结的旧名单。
const maxOnlineStale = 2 * time.Minute

// onlineLogIntervalSec 在线拉取失败/超龄日志的限频间隔：xray 停机期间拉取循环按心跳
// 节拍空转，逐条记日志会刷屏（默认 5s 心跳 × 1h ≈ 720 行），限到 1 分钟一条。
const onlineLogIntervalSec = 60

// onlineSnapshot 不可变在线快照（写时复制）：后台拉取成功后原子整体发布，一经发布
// 永不被修改，读者（心跳）可安全持有其切片，免拷贝免锁。与计费大锁 mu 完全解耦——
// 拉取 RPC 不持任何锁，慢 xray 不再拖住计费采集与用户同步（2026-09-25 重构）。
type onlineSnapshot struct {
	users []OnlineUser
	at    time.Time // 成功拉取时刻；零值 = 从未成功拉取
}

// Collector 采集 Xray stats 并通过 HandlerService 动态同步用户。
type Collector struct {
	apiAddr string
	conn    *grpc.ClientConn
	client  statsService.StatsServiceClient
	handler handlerService.HandlerServiceClient

	mu           sync.Mutex
	last         map[string]int64                    // 计数器名 → 上次累计值
	baseline     bool                                // 是否已建立基线
	currentUsers map[string]map[string]protocol.User // inboundTag -> email -> protocol.User

	// online 在线快照的原子发布点；nil 或 at 零值 = 尚无成功快照。
	online            atomic.Pointer[onlineSnapshot]
	lastOnlineFailLog atomic.Int64 // 上次「拉取失败」日志时刻（unix 秒，限频）
	lastStaleLog      atomic.Int64 // 上次「快照超龄」日志时刻（unix 秒，限频）

	// 在线拉取专用 gRPC 连接：与计费/用户同步的共享连接分离，collectLoop 失败时的
	// Close() 不影响在线拉取自愈；仅供后台拉取 goroutine 使用，无并发。
	onlineConn *grpc.ClientConn
	onlineCli  statsService.StatsServiceClient
}

// New 构造采集器（apiAddr 如 127.0.0.1:10085）。
func New(apiAddr string) *Collector {
	return &Collector{
		apiAddr:      apiAddr,
		last:         make(map[string]int64),
		currentUsers: make(map[string]map[string]protocol.User),
	}
}

// OnlineForHeartbeat 心跳帧的在线快照读取：只读内存，不做任何 gRPC。
// 快照由后台拉取循环（RefreshOnlineOnce，随心跳周期节拍）持续刷新并原子发布；
// 心跳读内存 = 零 RPC、零阻塞，xray API 慢不再拖住心跳发送，也不会与计费采集/
// 用户同步抢 Collector.mu（锁内 gRPC 曾把消息循环最长卡住 8–13s）。
//
// 容错链：xray 未运行 → OnlineMap 随进程消失必为空，发布空快照并直接返回；
// 无任何成功快照 → 返回空；快照距上次成功刷新超过 maxOnlineStale（xray 卡死、
// 拉取持续失败）→ 返回空，防止心跳无限携带冻结的旧名单。
// 返回的切片是已发布不可变快照的一部分，调用方只读、不得修改。
func (c *Collector) OnlineForHeartbeat(xrayRunning bool) (int, []OnlineUser) {
	if !xrayRunning {
		c.online.Store(&onlineSnapshot{})
		return 0, nil
	}
	snap := c.online.Load()
	if snap == nil || snap.at.IsZero() {
		return 0, nil
	}
	if age := time.Since(snap.at); age > maxOnlineStale {
		if now := time.Now().Unix(); now-c.lastStaleLog.Load() >= onlineLogIntervalSec {
			c.lastStaleLog.Store(now)
			log.Printf("agent: 在线快照已 %s 未成功刷新，本帧不上报（防冻结残影）", age.Round(time.Second))
		}
		return 0, nil
	}
	return len(snap.users), snap.users
}

// RefreshOnlineOnce 拉取一次 GetUsersStats 并原子发布新快照（由后台拉取循环按心跳周期调用）。
// 成功 → 发布新快照；失败 → 沿用旧快照（瞬时抖动不让在线数闪跳为 0），限频记日志。
// 只允许单个后台 goroutine 串行调用（专用连接无并发保护）。
func (c *Collector) RefreshOnlineOnce() {
	if c.onlineCli == nil {
		conn, err := grpc.NewClient(c.apiAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			c.logOnlineFail(fmt.Errorf("连接 xray api 失败: %w", err))
			return
		}
		c.onlineConn = conn
		c.onlineCli = statsService.NewStatsServiceClient(conn)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	resp, err := c.onlineCli.GetUsersStats(ctx, &statsService.GetUsersStatsRequest{})
	if err != nil {
		c.logOnlineFail(err)
		return
	}
	c.online.Store(&onlineSnapshot{users: onlineUsersFromResp(resp), at: time.Now()})
}

// logOnlineFail 拉取失败日志（onlineLogIntervalSec 限频防刷屏）。
func (c *Collector) logOnlineFail(err error) {
	now := time.Now().Unix()
	if now-c.lastOnlineFailLog.Load() < onlineLogIntervalSec {
		return
	}
	c.lastOnlineFailLog.Store(now)
	log.Printf("agent: 在线快照拉取失败（沿用上次快照）: %v", err)
}

// CloseOnline 关闭在线拉取专用连接（拉取循环退出时调用）。
func (c *Collector) CloseOnline() {
	if c.onlineConn != nil {
		_ = c.onlineConn.Close()
		c.onlineConn = nil
		c.onlineCli = nil
	}
}

// onlineUsersFromResp 把 GetUsersStats 回复规整为在线快照（过滤空 email/空 IP）。
func onlineUsersFromResp(resp *statsService.GetUsersStatsResponse) []OnlineUser {
	users := make([]OnlineUser, 0, len(resp.GetUsers()))
	for _, u := range resp.GetUsers() {
		if u == nil || u.GetEmail() == "" {
			continue
		}
		ips := make([]string, 0, len(u.GetIps()))
		lastSeen := make(map[string]int64, len(u.GetIps()))
		for _, e := range u.GetIps() {
			if e == nil || e.GetIp() == "" {
				continue
			}
			ips = append(ips, e.GetIp())
			lastSeen[e.GetIp()] = e.GetLastSeen()
		}
		if len(ips) == 0 {
			continue // 服务端已滤掉 Count()==0 的用户，此处防御性再滤
		}
		users = append(users, OnlineUser{Email: u.GetEmail(), IPs: ips, LastSeen: lastSeen})
	}
	return users
}

// connectLocked 建立 gRPC 连接（调用方须持有 mu）。
func (c *Collector) connectLocked() error {
	if c.client != nil && c.handler != nil {
		return nil
	}
	conn, err := grpc.NewClient(c.apiAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("连接 xray api 失败: %w", err)
	}
	c.conn = conn
	c.client = statsService.NewStatsServiceClient(conn)
	c.handler = handlerService.NewHandlerServiceClient(conn)
	return nil
}

// Close 关闭连接。
func (c *Collector) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
		c.client = nil
		c.handler = nil
	}
}

// SeedUsers 按一份 xray 配置重建内存用户缓存（不变量 I5：缓存恒等于运行中 xray 的用户集）。
//
// 为什么是「重建」而不是「清空」：缓存是移除操作的唯一依据（SyncUsers 的移除分支只遍历它），
// 而 xray 每次从磁盘配置启动（冷更 / 回滚 / 崩溃自愈 / 慢探底 / 开机 / 手动重启）都会换掉
// 自己的用户集。旧实现只清空缓存，于是紧随其后的第一次同步一个都摘不掉；更糟的是添加分支
// 会把 payload 回填进缓存，分歧从此对后续每一次同步都不可见（被删 / 被封 / 超量 / 过期的
// 用户能一直用到下一次冷更，最长 1h）。见 docs/architecture/配置下发-热更冷更与状态机.md §1.2。
//
// 解析失败时保留原缓存并返回错误：清空正是要修的那个 bug，宁可沿用旧视图也不能留空。
func (c *Collector) SeedUsers(configJSON []byte) error {
	users, err := parseConfigUsers(configJSON)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.currentUsers = users
	// 旧进程的连接随重启全部消失，在线快照一并清零，避免心跳沿用旧进程的残影。
	c.online.Store(&onlineSnapshot{})
	return nil
}

// SeedUsersFromFile 读取配置并重建缓存（xray 已按该文件启动时使用）。
func (c *Collector) SeedUsersFromFile(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("读取配置失败: %w", err)
	}
	return c.SeedUsers(data)
}

// configClientsDoc 重建缓存所需的字段子集（配置里其余字段一律忽略）。
type configClientsDoc struct {
	Inbounds []struct {
		Tag      string `json:"tag"`
		Settings struct {
			Clients []struct {
				ID    string `json:"id"`
				Email string `json:"email"`
				Flow  string `json:"flow"`
				Level uint32 `json:"level"`
			} `json:"clients"`
		} `json:"settings"`
	} `json:"inbounds"`
}

// parseConfigUsers 从配置内容解析出 inboundTag -> email -> 用户。
// 只收 panelUserEmailRe 命中的邮箱（relay 内部账户等一律不入缓存）。
func parseConfigUsers(configJSON []byte) (map[string]map[string]protocol.User, error) {
	var doc configClientsDoc
	if err := json.Unmarshal(configJSON, &doc); err != nil {
		return nil, fmt.Errorf("解析配置失败: %w", err)
	}
	users := make(map[string]map[string]protocol.User, len(doc.Inbounds))
	for _, inb := range doc.Inbounds {
		if inb.Tag == "" {
			continue
		}
		for _, cl := range inb.Settings.Clients {
			if !panelUserEmailRe.MatchString(cl.Email) {
				continue
			}
			byEmail := users[inb.Tag]
			if byEmail == nil {
				byEmail = make(map[string]protocol.User)
				users[inb.Tag] = byEmail
			}
			byEmail[cl.Email] = protocol.User{UUID: cl.ID, Email: cl.Email, Flow: cl.Flow, Level: cl.Level}
		}
	}
	return users, nil
}

// SyncUsers 通过 gRPC HandlerService 增量调整 Xray 中的用户。
func (c *Collector) SyncUsers(ctx context.Context, targetUsers map[string][]protocol.User) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.handler == nil {
		if err := c.connectLocked(); err != nil {
			return err
		}
	}

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	if c.currentUsers == nil {
		c.currentUsers = make(map[string]map[string]protocol.User)
	}

	// 1. 遍历 targetUsers 中的每个 Inbound
	for tag, users := range targetUsers {
		existing, ok := c.currentUsers[tag]
		if !ok {
			existing = make(map[string]protocol.User)
			c.currentUsers[tag] = existing
		}

		targetMap := make(map[string]protocol.User, len(users))
		for _, u := range users {
			targetMap[u.Email] = u
		}

		// 移除不在 targetMap 中的用户
		for email := range existing {
			if _, exists := targetMap[email]; !exists {
				remOp := &handlerService.RemoveUserOperation{Email: email}
				req := &handlerService.AlterInboundRequest{
					Tag:       tag,
					Operation: serial.ToTypedMessage(remOp),
				}
				_, err := c.handler.AlterInbound(ctx, req)
				if err != nil {
					if strings.Contains(err.Error(), "not found") {
						log.Printf("agent: gRPC 移除用户 %s (inbound=%s): 用户不存在（跳过）", email, tag)
					} else {
						log.Printf("agent: gRPC 移除用户 %s (inbound=%s) 警告: %v", email, tag, err)
					}
				} else {
					log.Printf("agent: gRPC 动态移除用户 %s (inbound=%s)", email, tag)
				}
				delete(existing, email)
			}
		}

		// 添加或更新 targetMap 中的用户
		for email, newUser := range targetMap {
			oldUser, exists := existing[email]
			if !exists || oldUser.UUID != newUser.UUID || oldUser.Flow != newUser.Flow || oldUser.Level != newUser.Level {
				if exists {
					remOp := &handlerService.RemoveUserOperation{Email: email}
					_, _ = c.handler.AlterInbound(ctx, &handlerService.AlterInboundRequest{
						Tag:       tag,
						Operation: serial.ToTypedMessage(remOp),
					})
				}

				account := &vless.Account{
					Id:   newUser.UUID,
					Flow: newUser.Flow,
				}
				accountTyped := serial.ToTypedMessage(account)

				protoUser := &xrayProtocol.User{
					Email:   newUser.Email,
					Level:   newUser.Level,
					Account: accountTyped,
				}

				addOp := &handlerService.AddUserOperation{User: protoUser}
				req := &handlerService.AlterInboundRequest{
					Tag:       tag,
					Operation: serial.ToTypedMessage(addOp),
				}
				_, err := c.handler.AlterInbound(ctx, req)
				if err != nil {
					if strings.Contains(err.Error(), "already exists") {
						log.Printf("agent: gRPC 添加用户 %s (inbound=%s): 用户已存在（同步状态）", email, tag)
						existing[email] = newUser
					} else {
						log.Printf("agent: gRPC 添加用户 %s (inbound=%s) 失败: %v", email, tag, err)
						return fmt.Errorf("gRPC 添加用户 %s 失败: %w", email, err)
					}
				} else {
					log.Printf("agent: gRPC 动态添加用户 %s (inbound=%s)", email, tag)
					existing[email] = newUser
				}
			}
		}
	}

	// 2. 清理已不在 targetUsers 中的 Inbound 标签
	for tag, existing := range c.currentUsers {
		if _, ok := targetUsers[tag]; !ok {
			for email := range existing {
				remOp := &handlerService.RemoveUserOperation{Email: email}
				_, _ = c.handler.AlterInbound(ctx, &handlerService.AlterInboundRequest{
					Tag:       tag,
					Operation: serial.ToTypedMessage(remOp),
				})
			}
			delete(c.currentUsers, tag)
		}
	}

	return nil
}

// Collect 拉取全量计数器并返回自上次以来的流量增量（用户维度 + 入站维度两个计数器族）。
// 若 xray 未运行/连接失败返回 error，由调用方决定重连。
func (c *Collector) Collect(ctx context.Context) ([]Entry, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.client == nil {
		if err := c.connectLocked(); err != nil {
			return nil, err
		}
	}

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	resp, err := c.client.QueryStats(ctx, &statsService.QueryStatsRequest{Pattern: "", Reset_: false})
	if err != nil {
		return nil, err
	}

	// 先建基线：首次调用只记录当前值，不产出增量（user/inbound 两个计数器族都必须
	// 入基线，否则首个周期会把节点历史总流量误报为本周期增量）
	if !c.baseline {
		for _, st := range resp.Stat {
			if userRe.MatchString(st.Name) || inboundRe.MatchString(st.Name) {
				c.last[st.Name] = st.Value
			}
		}
		c.baseline = true
		return nil, nil
	}

	up, down := make(map[string]int64), make(map[string]int64)     // 用户维度：email → 字节
	inUp, inDown := make(map[string]int64), make(map[string]int64) // 入站维度：tag → 字节
	seen := make(map[string]bool)
	accumulate := func(re *regexp.Regexp, upMap, downMap map[string]int64) {
		for _, st := range resp.Stat {
			m := re.FindStringSubmatch(st.Name)
			if m == nil {
				continue
			}
			seen[st.Name] = true
			cur := st.Value
			prev, ok := c.last[st.Name]
			delta := cur
			if ok && cur >= prev {
				delta = cur - prev // 正常增量
			}
			// ok && cur < prev：xray 重启计数器归零，delta=cur 视为从 0 开始
			c.last[st.Name] = cur
			if m[2] == "uplink" {
				upMap[m[1]] += delta
			} else {
				downMap[m[1]] += delta
			}
		}
	}
	accumulate(userRe, up, down)
	accumulate(inboundRe, inUp, inDown)
	// 清理已消失的计数器（删除了用户/入站），防止泄漏
	for name := range c.last {
		if !seen[name] {
			delete(c.last, name)
		}
	}

	entries := make([]Entry, 0, len(up)+len(inUp))
	appendDelta := func(dst []Entry, upMap, downMap map[string]int64, inbound bool) []Entry {
		for key, u := range upMap {
			d := downMap[key]
			if u == 0 && d == 0 {
				continue // 过滤无流量增量条目，避免海量全 0 条目挤占网络带宽与序列化开销
			}
			e := Entry{Up: u, Down: d}
			if inbound {
				e.Inbound = key
			} else {
				e.Email = key
			}
			dst = append(dst, e)
		}
		// 仅上报有流量的（down 单独有流量而 upMap 中无该 key 的情况）
		for key, d := range downMap {
			if _, ok := upMap[key]; !ok && d > 0 {
				e := Entry{Up: 0, Down: d}
				if inbound {
					e.Inbound = key
				} else {
					e.Email = key
				}
				dst = append(dst, e)
			}
		}
		return dst
	}
	entries = appendDelta(entries, up, down, false)
	entries = appendDelta(entries, inUp, inDown, true)
	return entries, nil
}
