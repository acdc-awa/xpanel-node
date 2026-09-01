// Package stats 通过 Xray gRPC StatsService 采集用户流量，并支持 HandlerService 动态更新用户。
// 实现模型对照《xray-api-探索.md》§4（3x-ui GetTraffic 轮询模式）：
// QueryStats 全量拉取 → 维护 last 基线 → 差值 = 本周期增量（xray 重启归零视为 0）。
package stats

import (
	"context"
	"fmt"
	"log"
	"regexp"
	"strings"
	"sync"
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

// OnlineUser 单个用户的在线快照：当前活跃连接的去重源 IP 列表。
type OnlineUser struct {
	Email string
	IPs   []string
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
	online       int                                 // 最近一次 Collect 观测到的在线用户数
	onlineUsers  []OnlineUser                        // 最近一次 Collect 的在线用户 IP 快照
}

// New 构造采集器（apiAddr 如 127.0.0.1:10085）。
func New(apiAddr string) *Collector {
	return &Collector{
		apiAddr:      apiAddr,
		last:         make(map[string]int64),
		currentUsers: make(map[string]map[string]protocol.User),
	}
}

// OnlineUsers 返回最近一次 Collect 观测到的在线用户数（未采集过则 0）。
func (c *Collector) OnlineUsers() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.online
}

// OnlineSnapshot 返回最近一次 Collect 的在线用户 IP 快照（拷贝；未采集过则 nil）。
func (c *Collector) OnlineSnapshot() []OnlineUser {
	c.mu.Lock()
	defer c.mu.Unlock()
	return cloneOnlineUsers(c.onlineUsers)
}

// onlineUsersFromResp 把 GetUsersStats 回复规整为在线快照（过滤空 email/空 IP）。
func onlineUsersFromResp(resp *statsService.GetUsersStatsResponse) []OnlineUser {
	users := make([]OnlineUser, 0, len(resp.GetUsers()))
	for _, u := range resp.GetUsers() {
		if u == nil || u.GetEmail() == "" {
			continue
		}
		ips := make([]string, 0, len(u.GetIps()))
		for _, e := range u.GetIps() {
			if e != nil && e.GetIp() != "" {
				ips = append(ips, e.GetIp())
			}
		}
		if len(ips) == 0 {
			continue // 服务端已滤掉 Count()==0 的用户，此处防御性再滤
		}
		users = append(users, OnlineUser{Email: u.GetEmail(), IPs: ips})
	}
	return users
}

func cloneOnlineUsers(src []OnlineUser) []OnlineUser {
	if len(src) == 0 {
		return nil
	}
	out := make([]OnlineUser, len(src))
	for i, u := range src {
		out[i] = OnlineUser{Email: u.Email, IPs: append([]string(nil), u.IPs...)}
	}
	return out
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

// ResetUsers 重置内存中的用户缓存（在 Xray 重启后使用）。
// 旧进程的连接随重启全部消失，在线快照一并清零，避免心跳沿用旧进程的残影。
func (c *Collector) ResetUsers() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.currentUsers = make(map[string]map[string]protocol.User)
	c.online = 0
	c.onlineUsers = nil
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

	// 在线快照：OnlineMap（user>>><email>>>online）不在 counters 命名空间，
	// QueryStats 永远不返回它，必须走专用 RPC GetUsersStats。
	// 失败时沿用上次快照（瞬时抖动不该让心跳在线数闪跳为 0）；xray 进程整体
	// 掉线时 QueryStats 会先行失败，本函数根本走不到这里。
	if err := c.collectOnlineLocked(ctx); err != nil {
		log.Printf("agent: 在线用户采集失败（沿用上次快照）: %v", err)
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
			e := Entry{Up: u, Down: downMap[key]}
			if inbound {
				e.Inbound = key
			} else {
				e.Email = key
			}
			dst = append(dst, e)
		}
		// 仅上报有流量的（down 单独有流量而 up 为 0 的情况）
		for key, d := range downMap {
			if upMap[key] == 0 && d > 0 {
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

// collectOnlineLocked 通过 GetUsersStats 拉取在线用户快照（调用方须持有 mu）。
func (c *Collector) collectOnlineLocked(ctx context.Context) error {
	resp, err := c.client.GetUsersStats(ctx, &statsService.GetUsersStatsRequest{})
	if err != nil {
		return err
	}
	c.onlineUsers = onlineUsersFromResp(resp)
	c.online = len(c.onlineUsers)
	return nil
}
