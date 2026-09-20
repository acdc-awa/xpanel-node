# XPanel-Node 节点通信协议

> 主控（XPanel）与节点 Agent 之间的 WSS 长连接协议说明书。
> **代码是强契约**：消息常量与载荷结构定义于 `pkg/protocol/`（本仓库，master 经 go.mod 引入）。
> 本文档是人类可读参考；演进规则：**只增不删、新字段 omitempty、先发 agent 再发 master**。

## 1. 传输与帧格式

- 传输：WebSocket。生产由**用户自备的反代**终止 TLS（节点连 `wss://`，主控 WS 网关端口恒监听明文 `ws://`）。
  主控的 compose 不含反代，只提供 `Caddyfile` 参考模板。
- 端点：对外路径为 `<面板域名>/node/ws`（三监听模型，2026-08-25 起；反代把 `/node/ws` 分流到主控的独立 WS 端口，
  默认 18082）。主控侧该端口内**任意路径**都交给 WS 网关，路径由反代裁决。
- 帧：JSON 文本帧，统一信封：

```json
{ "type": "<消息类型>", "id": "<请求ID，可空>", "payload": { ... } }
```

- 请求-响应通过 `id` 配对：主控下发的指令带 `id`，节点回 `result` 帧回填同一 `id`。

## 2. 认证握手

1. 节点连接后**首条消息必须是 `auth`**，payload：`{"node_id": "...", "secret": "..."}`。
   连接 URL 会附带 `?node_id=<id>`（便于反代/主控侧观测与路由），身份校验仍只认首帧 `auth` 的载荷。
2. 主控应答帧类型为 `auth_ok`（成功）或 `bad_auth`（失败，payload 为 result 结构，`error` 含原因）。
   节点等待 `auth_ok` 有 10s 读超时，超时即视为认证失败并断开重连。
3. `auth_ok` 载荷为 `{"ok": true, "caps": [...]}`，`caps` 声明主控支持的可选能力
   （当前仅 `traffic_ack`）。**旧主控只有 `{"ok": true}`**，节点据此把流量投递降级为
   「发完即删」（见 §5）；新节点忽略不了的只有这个字段，旧节点读 `ok` 即可，不受影响。
4. 认证失败主控立即关闭连接。`secret` 不放 URL/query，避免日志泄露。

## 3. 消息类型

### 节点 → 主控

| type | payload | 说明 |
|---|---|---|
| `auth` | AuthPayload | 首条认证消息（见 §2） |
| `heartbeat` | HeartbeatPayload | 周期心跳（默认 30s），携带系统指标与 agent 版本 |
| `traffic_report` | TrafficReportPayload | 流量批量上报（默认 60s）。带 `batch_id` 的批次，主控落库后回 `traffic_ack` |
| `result` | ResultPayload | 指令回执，`id` 回填请求 ID |
| `internal_uuid_report` | InternalUUIDReportPayload | relay 内部 UUID 变更上报。**主控侧已实现接收**，但节点当前不发送（内部 UUID 由 `setup_internal_account` 的 `result` 回执承载），保留类型供后续使用 |
| `upgrade_progress` | UpgradeProgressPayload | agent 升级进度状态上报（阶段流式通知，2026-09-03 新增） |

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
| `upgrade_agent` | UpgradeAgentPayload | 触发 agent 自升级到最新 release（见 §5 时序），回 `result`（data 为升级结果文本） |
| `agent_settings` | AgentSettingsPayload | 下发运行时设置（连接建立与设置保存时；0=不变），回 `result`（data 为变更摘要） |
| `traffic_ack` | TrafficAckPayload | 流量批次落库回执（`{batch_id, ok, error?}`）。节点收到 `ok=true` 才删本地发件箱中的该批次；未收到（写库失败/主控崩溃/回执丢失/连接断开）则保留重发，重复投递由主控按 `batch_id` 去重吸收。非指令，不回 `result` |

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
TrafficReportPayload { entries: TrafficEntry[], period,
                     batch_id?, seq?, boot_id? }
                     // period: RFC3339 上报时刻（**不是**消费区间，归属不依赖它）
                     // batch_id: 批次 UUID，重发复用同一 ID —— 主控去重键（缺失 = 旧 agent，不做去重）
                     // seq/boot_id: 批次序号 + 本次进程启动标识，仅供运维判缺口（同 boot_id 内 seq 跳号 = 丢批次）
TrafficEntry       { user_id, email?, inbound?, up_bytes, down_bytes, cycle_id? }
                     // cycle_id: 账期 ID（≥1），节点在**采集时刻**打标；主控据此归属计费周期。
                     // 0/缺失 = 未知（旧 agent / 入站维度条目），主控回退按上报时刻的小时桶归属
TrafficAckPayload  { batch_id, ok, error? }   // 主控→节点：落库回执，ok=true 才删本地批次
User               { uuid, email, flow?, level?, limit?, cycle_id? }
                     // cycle_id: 该用户当前账期 ID。节点收到与本地记录不同的值即视为发生周期切换，
                     // 立刻采集一次把切换前的增量按**旧账期**封账，此后采集的才按新账期
SyncUsersPayload   { users: { "<inbound_tag>": User[] } }
PushConfigPayload  { config_json }                 // 完整 xray 配置 JSON 字符串
GetLogsPayload     { lines }
SetupInternalAccountPayload { tag }
SetupInternalResult         { tag, uuid }          // uuid 由节点生成，主控以此覆盖 DB
PushCertPayload    { domain, cert_pem, key_pem }
InternalUUIDReportPayload { tag, uuid }
UpgradeAgentPayload { target? }              // 空 = 拉取最新 release
UpgradeProgressPayload { phase, target?, message, error?, ts }
                     // phase: starting/checking/downloading/verifying/replacing/restarting/failed/success
AgentSettingsPayload { report_interval_sec?, heartbeat_interval_sec? }
                    // 秒；0=不变；clamp 5s–30min；仅当前会话生效（不写回 agent.yaml）
ResultPayload      { ok, error?, data? }
StatusData         { xray_running, pid?, uptime_sec?, config_path?, started_at? }
```

## 5. 时序

- **心跳**：默认 30s 一次。主控落 `last_seen_at` + `status=1` + `agent_version`（有上报时），并写 `node_reports` 供仪表盘趋势。
- **流量上报**：默认 60s 一次，主控落 `traffic_logs` 并按周期聚合。
  **至少一次投递 + 主控去重**（v0.1.14+）：采集到的增量先落盘成不可变批次（`outbox_path`，默认
  `/etc/xray-agent/traffic_outbox.json`），上报后等 `traffic_ack`，只有 `ok=true` 才删批。主控把
  「批次去重记录 + 流量写入」放在同一事务，提交后才回执，因此
  写库失败 / 主控崩溃 / 回执丢失 / 连接断开 都不会丢数据；重发由主控按 `batch_id` 去重。
  投递节奏受三条规则约束，都是为了**不重复计量**（旧主控小时桶 upsert 是加法）：
  同一批在**同一条连接上只发一次**（发出即标记，等回执）；主控回 `ok=false` 立即解锁、下轮重试；
  连接重建后清空标记，未确认批次在新连接上重发一次（回执可能随旧连接丢失）。
  重连后立即补发积压，不必等下一个上报周期。
  未声明 `traffic_ack` 能力的主控（旧版本）下**不等回执、发完即删**（批次号照发，旧主控忽略未知字段）——
  与旧 agent 的投递语义一致。此时发件箱只提供「重启不丢未发送数据」，不再提供至少一次投递。
- **账期切换**：主控在购买/续费/重置时递增该用户 `cycle_id` 并立即推送 `sync_users`（经 OrderPaidEvent）。
  节点发现某用户账期变化时**先按旧账期采集一次**（封账），再更新本地账期映射——顺序不可颠倒，
  否则切换前的消费会被打上新账期。分割点在收到新名单的瞬间（≈切换时刻 + 推送延迟），
  比按上报周期切分精确得多。节点离线期间发生的切换只能等重连后收到新名单才知道，
  这段时间的增量归旧账期（少计新套餐，方向安全，不会超收）。
- **配置下发**：`push_config` → 节点 `xray -test` 校验 → 落盘 → 重启 xray → `result` 回执（失败自动回滚旧配置）。
- **用户热更新**：`sync_users` → gRPC AlterInbound 增删用户（不重启 xray）→ `result` 回执。
- **自升级**：`upgrade_agent` → 节点后台执行自升级，并在各阶段主动上报 `upgrade_progress`（checking → downloading → verifying → replacing → restarting / failed / success），主控实时缓存并暴露给面板步骤条展示；替换完成后**先发 `result` 回执再重启**（systemd 服务内 `systemctl restart` 会终止本进程，若等重启完成再回执则回执永远发不出去）。主控侧等待回执超时用 5 分钟长超时。
- **断线重连**：节点指数退避重连（上限默认 60s）；主控侧离线期间的配置变更在重连后自动补推。
- **运行时设置**：主控在节点连接建立与设置保存时下发 `agent_settings`，节点动态重建采集/上报/心跳 ticker
  （生效采集周期 = min(agent.yaml, 生效上报周期)，防上报快于采集的粒度倒挂）。未收到下发时按 agent.yaml 兜底；
  不支持该指令的旧 agent 静默忽略（不回应，主控侧回执超时忽略即可）。

## 6. 版本兼容

- 主控与 agent 各自独立发版（agent tag 即发布版本，如 `v0.1.0`）。
- 兼容策略：协议**只增不删**；新增字段必须 `omitempty`；旧 agent 可连新主控（缺省字段走主控默认值/保留旧值）。
- 流量投递确认与账期归属（v0.1.14）双向降级：
  - 旧 agent（不带 `batch_id`）→ 主控不去重、不回 `traffic_ack`，落库与归属按旧规则（按上报时刻的小时桶）；
  - 旧主控（`auth_ok` 无 `caps`、不认 `traffic_ack`、`sync_users` 不带 `cycle_id`）→ 节点经能力协商
    判定后**降级为发完即删**（不落批次、不重发，投递语义与旧 agent 一致，因此不会重复计量）；
    `cycle_id` 缺失（0）时节点不做账期切换处理，条目按未知账期上报、主控回退按上报时刻归属。
    代价只是失去「至少一次投递」与账期精确切分，**不产生错误计费**。
  - 结论：**升级顺序不构成数据风险，可任意先后**。仍建议先升主控再升 agent——先升主控才能让新
    agent 一上来就拿到回执与账期归属，否则新 agent 会先按降级路径跑一段（数据正确但粒度退化为旧口径）。
- `upgrade_agent` 于面板端触发（服务器页「升级」）；不支持该指令的旧 agent 收到后不回应，主控侧表现为等待回执超时。节点侧 CLI `xray-agent upgrade` 主动拉取升级仍然保留，两条路径共用 `internal/agent/upgrade` 的下载/校验/替换逻辑。
