// Package protocol 定义主控-节点通信协议（主控与 Agent 共用）。
//
// 传输：WebSocket（生产 wss 由 Caddy 终止 TLS 后以明文 ws 转发给主控，主控恒监听 ws）。
// 消息：JSON 文本帧，{type, id, payload}，请求-响应用 id 配对。
package protocol

import (
	"encoding/json"
	"errors"
)

// 消息类型常量（与《系统设计方案》§4.3 对齐）。
const (
	// 节点 → 主控
	MsgAuth          = "auth"
	MsgHeartbeat     = "heartbeat"
	MsgTrafficReport = "traffic_report"
	MsgResult        = "result"

	// 主控 → 节点
	MsgPushConfig  = "push_config"
	MsgSyncUsers   = "sync_users"
	MsgRestartXray = "restart_xray"
	MsgGetStatus   = "get_status"
	MsgGetLogs     = "get_logs"

	// Phase T：内部账户与证书
	MsgSetupInternalAccount  = "setup_internal_account"  // 主控→节点：为 relay 入站生成内部 UUID
	MsgRotateInternalAccount = "rotate_internal_account" // 主控→节点：重新生成内部 UUID
	MsgPushCert              = "push_cert"               // 主控→节点：TLS 证书下发落盘
	MsgInternalUUIDReport    = "internal_uuid_report"    // 节点→主控：内部 UUID 变更主动上报

	// 运维指令
	MsgUpgradeAgent    = "upgrade_agent"    // 主控→节点：升级 agent 二进制（自升级，见 internal/agent/upgrade）
	MsgUpgradeProgress = "upgrade_progress" // 节点→主控：agent 升级进度状态上报
	MsgAgentSettings   = "agent_settings"   // 主控→节点：下发上报/心跳等运行时设置（0=保持不变）

	// 认证握手结果（主控→节点，首条 auth 消息的应答帧类型）
	MsgAuthOK  = "auth_ok"
	MsgAuthBad = "bad_auth"
)

// Message 为统一消息帧。
type Message struct {
	Type    string          `json:"type"`
	ID      string          `json:"id,omitempty"` // 请求 ID，result 回执时回填
	Payload json.RawMessage `json:"payload,omitempty"`
}

// Encode 编码消息帧。
func Encode(typ, id string, payload any) ([]byte, error) {
	var raw json.RawMessage
	if payload != nil {
		b, err := json.Marshal(payload)
		if err != nil {
			return nil, err
		}
		raw = b
	}
	return json.Marshal(Message{Type: typ, ID: id, Payload: raw})
}

// Decode 解码消息帧。
func Decode(data []byte) (*Message, error) {
	var m Message
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	if m.Type == "" {
		return nil, errors.New("消息缺少 type 字段")
	}
	return &m, nil
}

// Payload 解析消息负载到 v。
func (m *Message) PayloadTo(v any) error {
	if len(m.Payload) == 0 {
		return nil
	}
	return json.Unmarshal(m.Payload, v)
}
