# XPanel-Node

XPanel 面板的被控节点 Agent：托管 Xray-core 进程，通过 WSS 长连接与主控通信，上报心跳与流量，响应配置下发/用户热更新/证书下发等指令。

- 主控仓库：**XPanel**（`github.com/acdc-awa/xpanel`）
- 通信协议：见 [docs/PROTOCOL.md](docs/PROTOCOL.md)（协议代码单源在 `pkg/protocol/`，主控经 go.mod 引入）
- 锁定 Xray 版本：**v26.10.31**（Go 依赖钉 `v1.260327.1-0.20261031132019-a4a3dff5b20c`）
  - 26.10.31 是紧急安全更新：默认关闭 MPTCP（Linux 内核监听 IPv4 MPTCP 存在远程提权，而 Go 1.24+ 默认开启）。
    **v26.10.29 已被上游撤版**，节点应尽快升到 26.10.31。
  - 26.10.29 相对 26.10.21 的两条关键差异：①freedom `blockDelay` 对 DispatchLink 型入站
    （VLESS/Hysteria）终于生效——此前黑洞延迟形同虚设，客户端连接会一直挂到自身超时；
    ②VLESS 出站不带 TLS/vlessenc 时，只认 Go 口径的私网（RFC1918 / 回环 / 链路本地），
    `100.64.0.0/10`（CGNAT、Tailscale）、`198.18.0.0/15`、`192.0.0.0/24` 等特殊段不再放行。
  - 关于 REALITY 与 mihomo：Xray **≥26.9.8** 的服务端强制要求 ClientHello 携带 X25519MLKEM768
    keyshare，而 mihomo 默认会主动剥离它（为兼容旧服务端）。订阅侧已固定输出
    `support-x25519mlkem768: true`，因此面板签发的订阅在新内核上可直接使用；
    且实际可用的 uTLS 指纹只有 `chrome` / `random`（其余指纹模板不含该 keyshare）。
  - 历史上的「26.7.x 的 minClientVer 拒绝 mihomo」已不成立：该默认值在 26.9.8 起已被上游撤销。
  - 节点上的 xray 内核可由面板下发 `upgrade_xray` 指令在线替换（停 xray → 校验 → 替换 → 拉起，失败自动回滚）。

## 安装节点（推荐）

在 XPanel 管理端「服务器」页新增节点，复制生成的一键安装命令到节点执行（安装脚本来自本仓库 GitHub Releases，`--master` 指向主控节点 WS 网关，对外路径为面板域名 `/node/ws`）：

```bash
bash <(curl -fsSL https://github.com/acdc-awa/XPanel-Node/releases/latest/download/install-agent.sh) \
  --master wss://<面板域名>/node/ws \
  --node-id <ID> --secret <SECRET>
```

脚本完成：从本仓库 GitHub Releases 下载 agent 二进制（sha256 强制校验）→ 安装 Xray-core（官方 Releases + .dgst 校验）→ 写 `/etc/xray-agent/config.yml`（0600）→ 注册 systemd 并启动。

高级选项：`--agent-version` 钉版本、`--agent-mirror` 镜像/加速前缀、`--agent-url` 完全自定义下载地址、`--agent-file` 离线本地安装、`--agent-digest` 手动摘要校验。详见 `deploy/install-agent.sh` 头部注释。

Xray 内核与 Agent 版本解耦：安装脚本只在 xray 缺失/不可执行时安装内核，已装且可用的内核会保留（重跑脚本升级 Agent 不会覆盖它）。内核升级/降级由主控「服务器 → Xray 版本」操作，也可用脚本显式换版：`--xray-version vX.Y.Z` 或 `--xray-force`（按默认/指定版本重装）。

## 自升级

```bash
xray-agent upgrade           # 检查 GitHub 最新 release，校验 sha256 后原子替换并重启
xray-agent upgrade --check   # 仅检查版本
```

下载源默认 `acdc-awa/XPanel-Node` Releases，可在 `config.yml` 的 `update.repo` / `update.mirror` / `update.download_timeout` 覆盖（见 `agent.example.yaml`）。升级按镜像候选链逐个尝试：`update.mirror` 置顶，其后自动落回内置候选（github.com → ghproxy.net → gh-proxy.com → moeyy），单个下载源超时/HTTP 失败自动切换下一个；单镜像下载超时默认 10m，慢链路可调大。

## 其他子命令

```bash
xray-agent                 # 裸执行：状态 + 命令面板（systemd 托管时；手动部署等于前台启动）
xray-agent start           # 启动 agent 与它托管的 xray（后台）
xray-agent stop            # 停止 agent 与它托管的 xray
xray-agent status          # 节点状态（agent / xray 进程、日志占用、配置摘要）
xray-agent restart         # 重启 agent（xray 随之重启）
xray-agent logs            # 查看日志（-n 行数、-f 跟踪）
xray-agent logs --size     # 只看日志占用与上限
xray-agent logs --vacuum   # 立即把日志裁剪到上限内
xray-agent run             # 前台运行（调试；systemd 托管时需先 stop）
xray-agent lang en         # 切换 CLI 语言（zh / en / auto）
xray-agent uninstall       # 卸载（停用 systemd、清理文件）
```

`start` / `stop` 都是**后台行为**：systemd 托管时等价于 `systemctl start|stop xray-agent`；手动部署时由 CLI 自己把 `run` 拉成脱离终端的后台进程（pid 记在 xray pid 文件同目录的 `agent.pid`，自身日志写同目录 `agent.log`）。停止会先让 agent 优雅退出，由它收掉 xray 子进程，不留孤儿。

## 语言

CLI 默认中文，可切英文：`xray-agent lang en`（切回 `xray-agent lang zh`，清除设置 `xray-agent lang auto`）。
语言状态写在 `/run/xray-agent/lang`（systemd 系统上 `/run` 是 tmpfs：只存内存、重启恢复默认中文），
**不写数据库、不改 `config.yml`**。单次覆盖：`XRAY_AGENT_LANG=en xray-agent help`（环境变量优先级最高）。
当前覆盖范围是 CLI 的界面与提示文案；agent 运行期日志仍为中文。

## 日志

xray 的 stdout/stderr 由 agent 通过管道接收：一边写入 `xray.log`，一边在内存里保留最近 **500 行**（主控「服务器 → 诊断 → 运行日志」直接读内存，不再每次读盘）。日志文件按大小滑动——超过 `xray.log_max_size_mb`（默认 20MB）就裁掉最旧的一半，不压缩、不留多份；手工 `truncate` 或替换文件后 agent 会自动重开新文件继续写。

有人绕过 agent 手工启动 xray（`xray run -c /etc/xray-agent/config.json`）时，它的输出不经过 agent，日志不会进 `xray.log`：`status` 与面板日志顶部都会给出提示，`xray-agent restart` 可重新接管。

## 开发

```bash
go build -o agent ./cmd/agent   # 构建
go test ./...                   # 测试
go vet ./...                    # 静态检查
```

## 发布

打 tag 即触发 GitHub Actions 发布流水线（`.github/workflows/release.yml`）：

```bash
git tag v0.1.0 && git push origin v0.1.0
```

产出资产：`xray-agent-linux-amd64`、`xray-agent-linux-arm64`、`checksums.txt`、`install-agent.sh`。
版本号经 ldflags 注入 `internal/agent/upgrade.Version`，随心跳上报主控展示。

## 许可证

本项目基于 [GNU Affero General Public License v3.0 (AGPL-3.0)](LICENSE) 开源，Copyright (C) 2026 acdc-awa。

- 任何人都可以自由使用、修改和再分发本项目，但无论以二进制还是网络服务形式向他人提供，都必须以相同协议开放完整源代码。
- 安装脚本运行时下载的 [Xray-core](https://github.com/XTLS/Xray-core) 官方二进制遵循 [MPL-2.0](https://github.com/XTLS/Xray-core/blob/main/LICENSE)，不受本协议约束。
- 如需商业授权（闭源、豁免 AGPL 义务），请联系作者单独洽谈。
