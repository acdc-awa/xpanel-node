# XPanel-Node

XPanel 面板的被控节点 Agent：托管 Xray-core 进程，通过 WSS 长连接与主控通信，上报心跳与流量，响应配置下发/用户热更新/证书下发等指令。

- 主控仓库：**XPanel**（`github.com/acdc-awa/xpanel`）
- 通信协议：见 [docs/PROTOCOL.md](docs/PROTOCOL.md)（协议代码单源在 `pkg/protocol/`，主控经 go.mod 引入）
- 锁定 Xray 版本：**v26.6.27**（26.7.x 的 REALITY minClientVer 会拒绝 mihomo 客户端）

## 安装节点（推荐）

在 XPanel 管理端「服务器」页新增节点，复制生成的一键安装命令到节点执行（安装脚本来自本仓库 GitHub Releases，`--master` 指向主控节点 WS 网关，对外路径为面板域名 `/node/ws`）：

```bash
bash <(curl -fsSL https://github.com/acdc-awa/XPanel-Node/releases/latest/download/install-agent.sh) \
  --master wss://<面板域名>/node/ws \
  --node-id <ID> --secret <SECRET>
```

脚本完成：从本仓库 GitHub Releases 下载 agent 二进制（sha256 强制校验）→ 安装 Xray-core（官方 Releases + .dgst 校验）→ 写 `/etc/xray-agent/config.yml`（0600）→ 注册 systemd 并启动。

高级选项：`--agent-version` 钉版本、`--agent-mirror` 镜像/加速前缀、`--agent-url` 完全自定义下载地址、`--agent-file` 离线本地安装、`--agent-digest` 手动摘要校验。详见 `deploy/install-agent.sh` 头部注释。

## 自升级

```bash
xray-agent upgrade           # 检查 GitHub 最新 release，校验 sha256 后原子替换并重启
xray-agent upgrade --check   # 仅检查版本
```

下载源默认 `acdc-awa/XPanel-Node` Releases，可在 `config.yml` 的 `update.repo` / `update.mirror` / `update.download_timeout` 覆盖（见 `agent.example.yaml`）。升级按镜像候选链逐个尝试：`update.mirror` 置顶，其后自动落回内置候选（github.com → ghproxy.net → gh-proxy.com → moeyy），单个下载源超时/HTTP 失败自动切换下一个；单镜像下载超时默认 10m，慢链路可调大。

## 其他子命令

```bash
xray-agent status     # 节点状态（xray 进程/配置/版本）
xray-agent restart    # 重启 xray
xray-agent logs       # 查看 xray 日志
xray-agent uninstall  # 卸载（停用 systemd、清理文件）
```

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
