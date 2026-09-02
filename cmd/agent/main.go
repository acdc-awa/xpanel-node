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
	if written, err := xrayproc.EnsureBootstrapConfig(cfg.Xray.ConfigPath); err != nil {
		log.Printf("写入自举最小配置失败（xray 暂不启动，待主控推送配置）: %v", err)
	} else if written {
		log.Printf("已写入自举最小配置: %s", cfg.Xray.ConfigPath)
	}

	// 启动时若已有配置则拉起 xray（崩溃由 watchdog 保持）
	if _, err := os.Stat(cfg.Xray.ConfigPath); err == nil {
		if err := proc.Start(); err != nil {
			log.Printf("xray 启动失败（watchdog 将重试）: %v", err)
		} else {
			log.Printf("xray 已启动")
		}
	}

	wdStop := make(chan struct{})
	go proc.Watchdog(wdStop)

	statsCollector := stats.New(cfg.Stats.APIAddr)

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
		Upgrade:         &upgrade.Fetcher{Repo: cfg.Update.Repo, Mirror: cfg.Update.Mirror},
		SelfRestart:     selfRestart,
	}

	// Watchdog 崩溃自愈回调：Xray 崩溃拉起后清空内存中的用户列表并触发向主控重连，
	// 主控重连握手成功后会自动全量下发 sync_users 补齐全部用户（防静态配置用户丢失）。
	proc.OnRestart = func() {
		log.Println("xray-agent: 检测到 xray 异常拉起，重置内存用户列表并触发重连向主控同步")
		statsCollector.ResetUsers()
		cli.TriggerReconnect()
	}

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
