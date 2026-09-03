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
}

// TrafficReportPayload 流量批量上报（P2 使用）。
type TrafficReportPayload struct {
	Entries []TrafficEntry `json:"entries"`
	Period  string         `json:"period"` // 上报周期起始，RFC3339
}

// User 节点同步的用户信息。
type User struct {
	UUID  string `json:"uuid"`
	Email string `json:"email"`
	Flow  string `json:"flow,omitempty"`
	Level uint32 `json:"level,omitempty"`
	Limit int    `json:"limit,omitempty"` // 最大在线设备数限制
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
	Phase   string `json:"phase"`   // starting | checking | downloading | verifying | replacing | restarting | failed | success
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
	XrayRunning bool      `json:"xray_running"`
	Pid         int       `json:"pid,omitempty"`
	UptimeSec   int64     `json:"uptime_sec,omitempty"`
	ConfigPath  string    `json:"config_path,omitempty"`
	StartedAt   time.Time `json:"started_at,omitempty"`
}
