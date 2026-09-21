package protocol

import "time"

// AuthPayload 节点认证（node_secret 不放入 URL，避免日志泄露）。
type AuthPayload struct {
	NodeID string `json:"node_id"`
	Secret string `json:"secret"`
}

// HeartbeatPayload 心跳与节点状态。
type HeartbeatPayload struct {
	CPU         float64         `json:"cpu"`                  // 百分比
	Mem         float64         `json:"mem"`                  // 已用内存（字节）
	MemTotal    float64         `json:"mem_total"`            // 总内存（字节）
	Disk        float64         `json:"disk"`                 // 已用磁盘（字节）
	DiskTotal   float64         `json:"disk_total"`           // 总磁盘（字节）
	XrayRunning bool            `json:"xray_running"`         // xray 是否在运行
	OnlineUsers int             `json:"online_users"`         // 在线用户数（当前活跃连接的去重用户）
	OnlineIPs   []OnlineUserIPs `json:"online_ips,omitempty"` // 每用户在线连接源 IP 快照（xray OnlineMap）
	RxRate      float64         `json:"rx_rate"`              // 实时速率（字节/秒）
	TxRate      float64         `json:"tx_rate"`
	RxBytes     uint64          `json:"rx_bytes"`          // 累计物理网卡接收字节
	TxBytes     uint64          `json:"tx_bytes"`          // 累计物理网卡发送字节
	Version     string          `json:"version,omitempty"` // agent 版本（旧 agent 不上报）
	TS          int64           `json:"ts"`                // unix 秒
	// 启动失败可观测性（2026-09-21）：xray 起不来时把状态与原因带回主控，
	// 面板不再只看到"未运行"却不知道为什么。旧主控忽略未知字段、旧 agent 不发（字段为空）。
	XrayState     string `json:"xray_state,omitempty"`            // running / restarting / failed / stopped
	XrayLastError string `json:"xray_last_error,omitempty"`       // 最近一次启动失败原因（已截断）
	XrayErrorAt   int64  `json:"xray_error_at,omitempty"`         // 该原因的观测时刻（unix 秒）
	XrayFailures  int    `json:"xray_restart_failures,omitempty"` // 连续启动失败次数
}

// OnlineUserIPs 单个用户当前活跃连接的去重源 IP（refcount 快照，连接断开即移除；
// 127.0.0.1/::1 不计入）。
type OnlineUserIPs struct {
	Email string   `json:"email"`
	IPs   []string `json:"ips,omitempty"`
}

// TrafficEntry 单条流量记录。两个维度互斥：
// 用户维度——Email 填邮箱（UserID=0 时主控按 Email 匹配用户），落 traffic_logs；
// 入站维度——Inbound 填入站 tag 且 Email 留空，主控仅累计 inbounds.up/down 冗余计数器
// （dashboard 节点流量占比/入站限额消费），不落用户流水防 KPI 双计（2026-09-01 接通）。
type TrafficEntry struct {
	UserID    uint64 `json:"user_id"`
	Email     string `json:"email,omitempty"`
	Inbound   string `json:"inbound,omitempty"`
	UpBytes   int64  `json:"up_bytes"`
	DownBytes int64  `json:"down_bytes"`
	// CycleID 账期 ID：本条增量所属的用户计费周期（主控经 sync_users 下发，节点在
	// **采集时刻**打标）。切换周期（购买/续费/重置）时主控递增该用户的 cycle_id 并立即
	// 推送新名单，节点据此把切换前已采集的增量按旧账期结算、切换后的按新账期——
	// 消费归属因此不再依赖上报时刻（Period），旧套餐消费不会落到新套餐头上（审计 F3）。
	// 0 = 未知（旧 agent 未打标 / 入站维度条目无用户账期）：主控回退按 period_start 归属。
	CycleID uint64 `json:"cycle_id,omitempty"`
}

// TrafficReportPayload 流量批量上报（P2 使用）。
type TrafficReportPayload struct {
	Entries []TrafficEntry `json:"entries"`
	Period  string         `json:"period"` // 上报周期起始，RFC3339
	// BatchID 批次唯一标识（UUID）：同一批数据重发必须复用同一 ID，主控据此去重
	// （审计 F2：小时桶 upsert 是加法，无批次去重时重发会翻倍计量）。
	// 空 = 旧 agent：主控不做去重、不回 ACK（维持旧行为）。
	BatchID string `json:"batch_id,omitempty"`
	// Seq 批次序号（跨重启单调递增）：与 BootID 合用于运维判缺口——同一 BootID 内 Seq
	// 跳号即节点侧丢了批次。不参与去重（去重键只有 BatchID）。
	Seq uint64 `json:"seq,omitempty"`
	// BootID 本次 agent 进程启动的随机标识：Seq 只在同一 BootID 内有缺口语义。
	BootID string `json:"boot_id,omitempty"`
}

// TrafficAckPayload 主控→节点：流量批次落库回执（审计 F1）。
// 节点只在收到 ok=true 后删除本地 outbox 中的该批次；未收到回执（主控写库失败、
// 主控崩溃、回执丢失、连接断开）时保留并重发，由主控侧 BatchID 去重兜住重复投递。
type TrafficAckPayload struct {
	BatchID string `json:"batch_id"`
	OK      bool   `json:"ok"`
	Error   string `json:"error,omitempty"`
}

// CapTrafficAck 主控能力：流量批次落库回执 + BatchID 去重（见 AuthOKPayload.Caps）。
const CapTrafficAck = "traffic_ack"

// AuthOKPayload 主控→节点：认证成功回执。
//
// Caps 声明主控支持的可选能力，是**双向兼容的关键**：节点只有在 Caps 里看到
// CapTrafficAck 时才进入「等回执才删批」模式。旧主控（回执只有 ok，无 caps）下节点
// 降级为发完即删——否则未确认批次会每轮重发，而旧主控小时桶 upsert 是加法且无批次去重，
// 同一批流量会被反复累加到用户账上。
type AuthOKPayload struct {
	OK   bool     `json:"ok"`
	Caps []string `json:"caps,omitempty"`
}

// HasCap 主控是否声明支持某能力（nil Caps = 旧主控）。
func (p AuthOKPayload) HasCap(cap string) bool {
	for _, c := range p.Caps {
		if c == cap {
			return true
		}
	}
	return false
}

// User 节点同步的用户信息。
type User struct {
	UUID  string `json:"uuid"`
	Email string `json:"email"`
	Flow  string `json:"flow,omitempty"`
	Level uint32 `json:"level,omitempty"`
	Limit int    `json:"limit,omitempty"` // 最大在线设备数限制
	// CycleID 该用户当前账期 ID（≥1）：节点收到与本地记录不同的值即视为发生周期切换，
	// 立刻做一次采集把切换前的增量按旧账期封账（见 TrafficEntry.CycleID）。
	// 0 = 旧主控未下发（节点不做切换处理，按未知账期上报）。
	CycleID uint64 `json:"cycle_id,omitempty"`
}

// SyncUsersPayload 全量用户同步负载（InboundTag -> []User）。
type SyncUsersPayload struct {
	Users map[string][]User `json:"users"`
}

// PushConfigPayload 下发 Xray 配置（P1 为完整 config JSON 透传，
// 模板生成器放 P3/P5）。
type PushConfigPayload struct {
	ConfigJSON string `json:"config_json"`
}

// GetLogsPayload 请求最近日志。
type GetLogsPayload struct {
	Lines int `json:"lines"`
}

// SetupInternalAccountPayload 主控→节点：为 relay 入站生成（或轮换）内部 UUID。
type SetupInternalAccountPayload struct {
	Tag string `json:"tag"`
}

// SetupInternalResult setup/rotate 回执 data（uuid 由节点生成，主控以此覆盖 DB）。
type SetupInternalResult struct {
	Tag  string `json:"tag"`
	UUID string `json:"uuid"`
}

// PushCertPayload 主控→节点：TLS 证书下发（agent 校验 PEM 匹配后落盘）。
type PushCertPayload struct {
	Domain  string `json:"domain"`
	CertPEM string `json:"cert_pem"`
	KeyPEM  string `json:"key_pem"`
}

// InternalUUIDReportPayload 节点→主控：内部 UUID 变更主动上报（如 CLI 轮换）。
// 主控侧已实现接收；agent 当前不发（内部 UUID 经 SetupInternalResult 回执承载），保留备用。
type InternalUUIDReportPayload struct {
	Tag  string `json:"tag"`
	UUID string `json:"uuid"`
}

// UpgradeAgentPayload 主控→节点：升级 agent 二进制。Target 为空 = 拉取最新 release。
type UpgradeAgentPayload struct {
	Target string `json:"target,omitempty"`
}

// UpgradeProgressPayload 节点→主控：升级进度上报。
type UpgradeProgressPayload struct {
	Phase   string `json:"phase"` // starting | checking | downloading | verifying | replacing | restarting | failed | success
	Target  string `json:"target,omitempty"`
	Message string `json:"message"`
	Error   string `json:"error,omitempty"`
	TS      int64  `json:"ts"`
}

// AgentSettingsPayload 主控→节点：运行时设置（连接建立与设置保存时下发，仅当前会话生效，
// 不写回 agent.yaml；字段为 0 表示保持现状）。agent.yaml 仍是主控未下发时的兜底。
type AgentSettingsPayload struct {
	ReportIntervalSec    int `json:"report_interval_sec,omitempty"`    // 流量上报周期（秒）
	HeartbeatIntervalSec int `json:"heartbeat_interval_sec,omitempty"` // 状态心跳周期（秒）
}

// ResultPayload 指令回执（id 回填请求 ID）。
type ResultPayload struct {
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
	Data  any    `json:"data,omitempty"`
}

// GetStatusPayload 查询完整状态。
type GetStatusPayload struct{}

// StatusData Agent 返回的完整状态。
type StatusData struct {
	XrayRunning bool       `json:"xray_running"`
	Pid         int        `json:"pid,omitempty"`
	UptimeSec   int64      `json:"uptime_sec,omitempty"`
	ConfigPath  string     `json:"config_path,omitempty"`
	StartedAt   *time.Time `json:"started_at,omitempty"` // 未托管实例（上一轮 agent 遗留）无此值
	// 启动失败可观测性（2026-09-21，旧主控忽略未知字段）
	XrayState     string `json:"xray_state,omitempty"`
	XrayLastError string `json:"xray_last_error,omitempty"`
	XrayFailures  int    `json:"xray_restart_failures,omitempty"`
}
