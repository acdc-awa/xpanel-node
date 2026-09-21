// Xray 节点 Agent 入口：托管 xray-core 并与主控保持 WSS 长连接。
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/acdc-awa/xpanel-node/internal/agent/accounts"
	"github.com/acdc-awa/xpanel-node/internal/agent/cli"
	"github.com/acdc-awa/xpanel-node/internal/agent/client"
	"github.com/acdc-awa/xpanel-node/internal/agent/collector"
	"github.com/acdc-awa/xpanel-node/internal/agent/config"
	"github.com/acdc-awa/xpanel-node/internal/agent/outbox"
	"github.com/acdc-awa/xpanel-node/internal/agent/stats"
	"github.com/acdc-awa/xpanel-node/internal/agent/upgrade"
	"github.com/acdc-awa/xpanel-node/internal/agent/xrayproc"
)

func main() {
	// 管理子命令分派：xray-agent status|restart|logs|uninstall|help
	args := os.Args[1:]
	forcedRun := false
	if len(args) > 0 && cli.IsSubcommand(args[0]) {
		if args[0] == "run" {
			forcedRun = true
			os.Args = append([]string{os.Args[0]}, args[1:]...)
		} else {
			os.Exit(cli.Run(args, os.Stdin, os.Stdout, os.Stderr))
		}
	}

	// 禁止裸启动第二个实例（实机互踢根因）：install-agent.sh 已装 systemd 单元时，
	// 终端手动执行 xray-agent 直接给出管理提示退出；systemd 服务进程带 INVOCATION_ID
	// 不受拦截，xray-agent run 仍可强制前台运行（调试/临时场景）。
	if !forcedRun && !cli.IsSystemdService() && cli.IsSystemdManaged() {
		fmt.Fprintln(os.Stderr, "xray-agent 已由 systemd 服务托管，请用 systemctl 管理：")
		fmt.Fprintln(os.Stderr, "  systemctl status xray-agent      查看运行状态")
		fmt.Fprintln(os.Stderr, "  systemctl restart xray-agent     重启服务")
		fmt.Fprintln(os.Stderr, "  journalctl -u xray-agent         查看日志")
		fmt.Fprintln(os.Stderr, "如需临时前台运行：先 systemctl stop xray-agent，再执行 xray-agent run")
		os.Exit(0)
	}

	cfgPath := flag.String("config", "", "配置文件路径（默认探测 /etc/xray-agent/config.yml、二进制同目录下的 agent.yaml、或 ./agent.yaml）")
	flag.Parse()

	path := *cfgPath
	if path == "" {
		// 按优先级探测：系统安装路径 → 二进制同目录 → 当前工作目录
		candidates := []string{"/etc/xray-agent/config.yml"}
		if exe, err := os.Executable(); err == nil {
			candidates = append(candidates, filepath.Join(filepath.Dir(exe), "agent.yaml"))
		}
		candidates = append(candidates, "agent.yaml")
		for _, cand := range candidates {
			if _, err := os.Stat(cand); err == nil {
				path = cand
				break
			}
		}
	}
	if path == "" {
		log.Fatal("未找到配置文件（可加 -config 指定路径，默认探测 /etc/xray-agent/config.yml、二进制同目录 agent.yaml、或当前目录 agent.yaml）")
	}

	cfg, err := config.Load(path)
	if err != nil {
		log.Fatalf("加载配置失败: %v", err)
	}

	proc := xrayproc.New(cfg.Xray.Bin, cfg.Xray.ConfigPath, cfg.Xray.LogPath, cfg.Xray.PidFile)
	proc.CleanupStale()

	// 自举：无配置时写入内嵌最小模板（log + api gRPC inbound(127.0.0.1:10085) + stats 等），
	// 保证 xray 装好即可启动、与 agent 的 gRPC 通信立即可用，不必等主控首次推送；
	// 主控推送到达后 RestartWithConfig 无缝覆盖，watchdog 此后按既有逻辑保活。
	// 开机体检：配置缺失写自举模板；-test 不通过则回退上一份可用配置（.good）——
	// 旧实现直接把坏配置交给 xray，watchdog 用坏配置反复拉起失败，节点永久宕机且无从知晓。
	if note := proc.EnsureUsableConfig(); note != "" {
		log.Printf("xray-agent: %s", note)
	}

	// 用户缓存按磁盘配置重建（不变量 I5）——缓存是「移除用户」的唯一依据，而 xray 每次从
	// 磁盘配置启动（冷更 / 回滚 / 自愈 / 慢探底 / 手动重启）都会换掉自己的用户集。
	// 旧实现只把缓存清空：紧随其后的第一次同步一个都摘不掉，且添加分支会把 payload 回填进
	// 缓存，分歧从此对后续同步永久不可见（被删 / 被封 / 超量 / 过期的用户最长 1h 仍可用，
	// 而主控认为同步成功）。钩子挂在 Start 成功之后，五条路径一次覆盖。
	statsCollector := stats.New(cfg.Stats.APIAddr)
	proc.OnStarted = func(configPath string) {
		if err := statsCollector.SeedUsersFromFile(configPath); err != nil {
			log.Printf("xray-agent: 按配置重建用户缓存失败（沿用旧视图）: %v", err)
		}
	}

	// 启动时若已有配置则拉起 xray（崩溃由 watchdog 保持）。
	// Start 现在要求"spawn 成功且活过就绪窗口"才算起来，失败会把退出码与 xray 自身的
	// stderr 摘要写进日志（旧实现只说"xray 已启动"，秒死也照说不误）。
	if _, err := os.Stat(cfg.Xray.ConfigPath); err == nil {
		if err := proc.Start(); err != nil {
			log.Printf("xray 启动失败（watchdog 将按退避重试，原因会随心跳上报主控）: %v", err)
		} else {
			log.Printf("xray 已启动")
		}
	}

	// 开机兜底：xray 可能在本进程接管之前就已在跑（自升级重启时 Start 早退、不触发回调），
	// 此时磁盘上的配置就是它加载的那份，按它重建缓存即可（重复重建无副作用）。
	if err := statsCollector.SeedUsersFromFile(cfg.Xray.ConfigPath); err != nil {
		log.Printf("xray-agent: 开机重建用户缓存失败（沿用旧视图）: %v", err)
	}

	// 面板触发自升级的重启回调：仅 systemd 服务内启动时可自重启（systemctl restart
	// 会 SIGTERM 本进程，服务重启拉起新二进制）；手动运行时无重启手段，升级回执里提示手动。
	// 必须在下面 `cli :=` 声明之前定义——此后标识符 cli 指向变量而非本包。
	var selfRestart func() error
	if cli.IsSystemdService() {
		selfRestart = func() error { return exec.Command("systemctl", "restart", "xray-agent").Run() }
	}

	cli := &client.Client{
		BaseURL:         cfg.Master.URL,
		NodeID:          cfg.Master.NodeID,
		Secret:          cfg.Master.Secret,
		Heartbeat:       cfg.Heartbeat,
		ReconnectMax:    cfg.ReconnectMax,
		Xray:            proc,
		Collector:       collector.New(),
		Stats:           statsCollector,
		CollectInterval: cfg.Stats.CollectInterval,
		ReportInterval:  cfg.Stats.ReportInterval,
		Accounts:        accounts.New(cfg.AccountsPath),
		CertsDir:        cfg.CertsDir,
		Outbox:          outbox.New(cfg.OutboxPath),
		Upgrade:         &upgrade.Fetcher{Repo: cfg.Update.Repo, Mirror: cfg.Update.Mirror, DownloadTimeout: cfg.Update.DownloadTimeout},
		SelfRestart:     selfRestart,
	}

	// Watchdog 崩溃自愈回调：xray 被拉起后触发向主控重连，重连握手成功后主控会自动全量
	// 下发 sync_users 补齐全部用户（兜底：缓存的权威重建已由 OnStarted 按磁盘配置完成，
	// 这里不再清空缓存——清空正是"冷更后摘不掉用户"那个 bug 的成因）。
	proc.OnRestart = func() {
		log.Println("xray-agent: 检测到 xray 异常拉起，触发重连向主控同步")
		cli.TriggerReconnect()
	}
	// 放弃自动拉起时立刻推一帧心跳：主控/面板立即拿到失败原因（报警），不必等下一个周期。
	proc.OnGiveUp = func(reason string) {
		cli.TriggerHeartbeat()
	}

	// 看门狗在回调就位之后再启动：它在锁内读 OnRestart/OnGiveUp，先启动会与之竞态，
	// 且放弃告警在回调为空时无处投递。xray 已在上面 Start 过一次，这里只是兜底巡检。
	wdStop := make(chan struct{})
	go proc.Watchdog(wdStop)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	log.Printf("xray-agent 启动（node=%s, master=%s, heartbeat=%s）", cfg.Master.NodeID, cfg.Master.URL, cfg.Heartbeat)
	cli.Run(ctx)

	// 退出清理（同步执行，确保 xray 被优雅停止，不遗留孤儿进程）
	close(wdStop)
	statsCollector.Close()
	_ = proc.Stop()
	log.Println("xray-agent 已退出")
}
