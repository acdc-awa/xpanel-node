# XPanel-Node 节点通信协议

> 主控（XPanel）与节点 Agent 之间的 WSS 长连接协议说明书。
> **代码是强契约**：消息常量与载荷结构定义于 `pkg/protocol/`（本仓库，master 经 go.mod 引入）。
> 本文档是人类可读参考；演进规则：**只增不删、新字段 omitempty、先发 agent 再发 master**。

## 1. 传输与帧格式

- 传输：WebSocket。生产由主控侧 Caddy 终止 TLS（节点连 `wss://`，主控 WS 网关端口恒监听明文 `ws://`）。
- 端点：对外路径为面板域名 `/node/ws`（四端口模型，2026-08-24 起；Caddy `@ws` 规则分流到 WS 端口）。
- 帧：JSON 文本帧，统一信封：

```json
{ "type": "<消息类型>", "id": "<请求ID，可空>", "payload": { ... } }
```

- 请求-响应通过 `id` 配对：主控下发的指令带 `id`，节点回 `result` 帧回填同一 `id`。

## 2. 认证握手

1. 节点连接后**首条消息必须是 `auth`**，payload：`{"node_id": "...", "secret": "..."}`。
2. 主控应答帧类型为 `auth_ok`（成功）或 `bad_auth`（失败，payload 为 result 结构，`error` 含原因）。
3. 认证失败主控立即关闭连接。`secret` 不放 URL/query，避免日志泄露。

## 3. 消息类型

### 节点 → 主控

| type | payload | 说明 |
|---|---|---|
| `auth` | AuthPayload | 首条认证消息（见 §2） |
| `heartbeat` | HeartbeatPayload | 周期心跳（默认 30s），携带系统指标与 agent 版本 |
| `traffic_report` | TrafficReportPayload | 流量批量上报（默认 60s） |
| `result` | ResultPayload | 指令回执，`id` 回填请求 ID |
| `internal_uuid_report` | InternalUUIDReportPayload | relay 内部 UUID 变更主动上报（如 CLI 轮换） |

### 主控 → 节点

| type | payload | 说明 |
|---|---|---|
| `push_config` | PushConfigPayload | 下发完整 Xray 配置 JSON（先 `xray -test` 后落盘，失败自动回滚） |
| `sync_users` | SyncUsersPayload | 全量用户同步（gRPC AlterInbound 热更新，不重启 xray） |
| `restart_xray` | — | 重启 xray 进程 |
| `get_status` | — | 查询完整状态，回 `result`（data 为 StatusData） |
| `get_logs` | GetLogsPayload | 拉取最近日志，回 `result`（data 为日志文本） |
| `setup_internal_account` | SetupInternalAccountPayload | 为 relay 入站生成内部 UUID，回 `result`（data 为 SetupInternalResult） |
| `rotate_internal_account` | SetupInternalAccountPayload | 重新生成内部 UUID |
| `push_cert` | PushCertPayload | TLS 证书下发（agent 校验 PEM 匹配后落盘） |

## 4. 载荷结构

```go
AuthPayload        { node_id, secret }
HeartbeatPayload   { cpu, mem, mem_total, disk, disk_total, xray_running,
                     online_users, online_ips?,  // 在线用户数 + 每用户在线 IP 快照
                     rx_rate, tx_rate, rx_bytes, tx_bytes,
                     version?,  // agent 版本（v0.1.0+；旧 agent 不上报）
                     ts }       // unix 秒
// online_ips: OnlineUserIPs { email, ips }[] —— xray GetUsersStats 快照，
// 仅含当前有活跃连接的用户；连接断开即移除（refcount，无宽限期）。
// 前提：主控下发配置的 policy.levels.0.statsUserOnline = true（模板已默认开启）。
TrafficReportPayload { entries: TrafficEntry[], period }  // period: RFC3339 周期起始
TrafficEntry       { user_id, email?, inbound?, up_bytes, down_bytes }
User               { uuid, email, flow?, level?, limit? }  // limit: 最大在线设备数
SyncUsersPayload   { users: { "<inbound_tag>": User[] } }
PushConfigPayload  { config_json }                 // 完整 xray 配置 JSON 字符串
GetLogsPayload     { lines }
SetupInternalAccountPayload { tag }
SetupInternalResult         { tag, uuid }          // uuid 由节点生成，主控以此覆盖 DB
PushCertPayload    { domain, cert_pem, key_pem }
InternalUUIDReportPayload { tag, uuid }
ResultPayload      { ok, error?, data? }
StatusData         { xray_running, pid?, uptime_sec?, config_path?, started_at? }
```

## 5. 时序

- **心跳**：默认 30s 一次。主控落 `last_seen_at` + `status=1` + `agent_version`（有上报时），并写 `node_reports` 供仪表盘趋势。
- **流量上报**：默认 60s 一次，主控落 `traffic_logs` 并按周期聚合。
- **配置下发**：`push_config` → 节点 `xray -test` 校验 → 落盘 → 重启 xray → `result` 回执（失败自动回滚旧配置）。
- **用户热更新**：`sync_users` → gRPC AlterInbound 增删用户（不重启 xray）→ `result` 回执。
- **断线重连**：节点指数退避重连（上限默认 60s）；主控侧离线期间的配置变更在重连后自动补推。

## 6. 版本兼容

- 主控与 agent 各自独立发版（agent tag 即发布版本，如 `v0.1.0`）。
- 兼容策略：协议**只增不删**；新增字段必须 `omitempty`；旧 agent 可连新主控（缺省字段走主控默认值/保留旧值）。
- `upgrade_agent`（主控推送升级指令）为预留消息类型，尚未实现；当前升级为节点侧 CLI `xray-agent upgrade` 主动拉取 GitHub Releases。
