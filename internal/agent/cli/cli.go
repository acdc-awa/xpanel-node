// Package cli 提供 xray-agent 的管理子命令：start / stop / status / restart / logs / upgrade /
// rollback / uninstall / lang / help。无参数或 run 走主循环（见 cmd/agent/main.go）。
package cli

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/acdc-awa/xpanel-node/internal/agent/config"
	"github.com/acdc-awa/xpanel-node/internal/agent/i18n"
	"github.com/acdc-awa/xpanel-node/internal/agent/logfile"
	"github.com/acdc-awa/xpanel-node/internal/agent/upgrade"
)

// 分组键（渲染时才翻译，保证语言切换即时生效）。
const (
	groupService = "service"
	groupDiag    = "diag"
	groupMaint   = "maint"
	groupHelp    = "help"
)

// command 子命令描述（help 与裸执行命令面板共用同一份，避免两处文案漂移）。
type command struct {
	name   string
	usage  string // 完整用法（help <命令> 显示）
	descZh string // 一句话说明（列表显示）
	descEn string
	group  string
}

func (c command) desc() string { return i18n.T(c.descZh, c.descEn) }

// commandGroups 分组顺序（帮助与命令面板的排版骨架）。
var commandGroups = []string{groupService, groupDiag, groupMaint, groupHelp}

var commandList = []command{
	{name: "start", usage: "xray-agent start [-config <path>]", descZh: "启动 agent 与它托管的 xray（后台运行）", descEn: "Start the agent and the xray it manages (in background)", group: groupService},
	{name: "stop", usage: "xray-agent stop [-config <path>]", descZh: "停止 agent 与它托管的 xray", descEn: "Stop the agent and the xray it manages", group: groupService},
	{name: "restart", usage: "xray-agent restart [-config <path>]", descZh: "重启 agent（xray 随之重启）", descEn: "Restart the agent (xray restarts along)", group: groupService},
	{name: "status", usage: "xray-agent status [-config <path>]", descZh: "查看 agent / xray / systemd 状态与配置摘要", descEn: "Show agent / xray / systemd status and config summary", group: groupService},
	{name: "run", usage: "xray-agent run [-config <path>]", descZh: "前台运行主循环（调试用，Ctrl+C 退出）", descEn: "Run the main loop in foreground (debugging; Ctrl+C to quit)", group: groupDiag},
	{name: "logs", usage: "xray-agent logs [-n <n>] [-f] [--size] [--vacuum]", descZh: "查看日志；--size 看占用，--vacuum 立即裁剪", descEn: "Show logs; --size for usage, --vacuum to trim now", group: groupDiag},
	{name: "upgrade", usage: "xray-agent upgrade [--check] [-config <path>]", descZh: "升级到主控最新版本", descEn: "Upgrade to the latest version", group: groupMaint},
	{name: "rollback", usage: "xray-agent rollback [-config <path>] <version>", descZh: "回滚到指定版本", descEn: "Roll back to a specific version", group: groupMaint},
	{name: "uninstall", usage: "xray-agent uninstall [--force] [-config <path>]", descZh: "卸载 agent（含 xray-core 与 geo 数据）", descEn: "Uninstall the agent (incl. xray-core and geo data)", group: groupMaint},
	{name: "lang", usage: "xray-agent lang [zh|en|auto]", descZh: "查看或切换界面语言（默认中文）", descEn: "Show or switch the CLI language (default: Chinese)", group: groupHelp},
	{name: "help", usage: "xray-agent help [command]", descZh: "显示帮助或命令详情", descEn: "Show help or details for a command", group: groupHelp},
}

// commandByName 子命令查表。
var commandByName = func() map[string]command {
	m := make(map[string]command, len(commandList))
	for _, c := range commandList {
		m[c.name] = c
	}
	return m
}()

var helpAliases = map[string]bool{"help": true, "-h": true, "-help": true, "--help": true}

// IsSubcommand 判断第一个参数是否为已知子命令或帮助别名。
func IsSubcommand(arg string) bool {
	if _, ok := commandByName[arg]; ok {
		return true
	}
	return helpAliases[arg]
}

// configCandidates 子命令配置文件探测顺序（可被测试覆盖）。
var configCandidates = []string{"/etc/xray-agent/config.yml", "agent.yaml"}

// systemdUnitPaths systemd 单元文件候选路径（可被测试覆盖）。
var systemdUnitPaths = []string{"/etc/systemd/system/xray-agent.service", "/lib/systemd/system/xray-agent.service"}

// Run 执行子命令，返回进程退出码（0 成功 / 1 执行失败 / 2 用法错误）。
func Run(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, i18n.T("缺少子命令", "missing subcommand"))
		printHelp(stderr)
		return 2
	}
	cmd := args[0]
	rest := args[1:]

	if helpAliases[cmd] {
		if len(rest) > 0 {
			if _, ok := commandByName[rest[0]]; ok {
				printCmdHelp(rest[0], stdout)
				return 0
			}
			fmt.Fprintf(stderr, i18n.T("未知命令: %s\n\n", "unknown command: %s\n\n"), rest[0])
		}
		printHelp(stdout)
		return 0
	}

	switch cmd {
	case "start":
		return runStart(rest, stdout, stderr)
	case "stop":
		return runStop(rest, stdout, stderr)
	case "status":
		return runStatus(rest, stdout, stderr)
	case "restart":
		return runRestart(rest, stdout, stderr)
	case "logs":
		return runLogs(rest, stdout, stderr)
	case "uninstall":
		return runUninstall(rest, stdin, stdout, stderr)
	case "upgrade":
		return runUpgrade(rest, stdout, stderr)
	case "rollback":
		return runRollback(rest, stdout, stderr)
	case "lang":
		return runLang(rest, stdout, stderr)
	case "run":
		fmt.Fprintln(stderr, i18n.T("run 子命令请直接使用 xray-agent 启动（或省略 run）",
			"use `xray-agent run` (or omit it) to run the agent in foreground"))
		return 2
	default:
		fmt.Fprintf(stderr, i18n.T("未知命令: %s\n\n", "unknown command: %s\n\n"), cmd)
		printHelp(stderr)
		return 2
	}
}

// resolveConfigPath 解析配置文件路径：-config 优先，否则按候选顺序探测。
func resolveConfigPath(explicit string) string {
	if explicit != "" {
		return explicit
	}
	for _, cand := range configCandidates {
		if _, err := os.Stat(cand); err == nil {
			return cand
		}
	}
	return ""
}

// loadTolerant 加载配置，允许缺失（卸载场景按默认路径继续）。
func loadTolerant(path string) (*config.Config, error) {
	if path == "" {
		return nil, errors.New(i18n.T("未找到配置文件（可加 -config 指定）",
			"config file not found (specify it with -config)"))
	}
	return config.Load(path)
}

func groupTitle(g string) string {
	switch g {
	case groupService:
		return i18n.T("服务管理", "Service")
	case groupDiag:
		return i18n.T("运行与诊断", "Runtime & diagnostics")
	case groupMaint:
		return i18n.T("升级与维护", "Upgrade & maintenance")
	case groupHelp:
		return i18n.T("帮助", "Help")
	}
	return g
}

func printHelp(w io.Writer) {
	fmt.Fprintln(w, i18n.T("xray-agent · Xray 节点 Agent", "xray-agent · Xray Node Agent"))
	fmt.Fprintln(w)
	fmt.Fprintln(w, i18n.T("用法", "Usage"))
	fmt.Fprintf(w, "  %s%s\n", padLabel("xray-agent <"+i18n.T("命令", "command")+"> ["+i18n.T("选项", "options")+"]", 34),
		i18n.T("执行管理命令", "Execute a management command"))
	fmt.Fprintf(w, "  %s%s\n", padLabel("xray-agent [-config <path>]", 34),
		i18n.T("前台运行主循环（等价 run）", "Run the main loop in foreground (same as run)"))
	printCommandGroups(w, "")
	fmt.Fprintln(w)
	fmt.Fprintln(w, i18n.T("选项", "Options"))
	fmt.Fprintln(w, "  "+i18n.T("-config <path>   配置文件路径（默认探测 /etc/xray-agent/config.yml 或 ./agent.yaml）",
		"-config <path>   config file (default: /etc/xray-agent/config.yml or ./agent.yaml)"))
	fmt.Fprintln(w)
	fmt.Fprintln(w, i18n.T("更多: xray-agent help <命令>", "More: xray-agent help <command>"))
}

// printCommandGroups 按分组打印命令表（help 与裸执行命令面板共用）。
func printCommandGroups(w io.Writer, indent string) {
	for _, g := range commandGroups {
		first := true
		for _, c := range commandList {
			if c.group != g {
				continue
			}
			if first {
				fmt.Fprintln(w)
				fmt.Fprintf(w, "%s%s\n", indent, groupTitle(g))
				first = false
			}
			fmt.Fprintf(w, "%s  %-10s %s\n", indent, c.name, c.desc())
		}
	}
}

func printCmdHelp(cmd string, w io.Writer) {
	c, ok := commandByName[cmd]
	if !ok {
		return
	}
	fmt.Fprintf(w, i18n.T("用法: %s\n", "Usage: %s\n"), c.usage)
	fmt.Fprintln(w, c.desc())
	switch cmd {
	case "start":
		fmt.Fprintln(w, i18n.T("systemd 托管：等价 systemctl start xray-agent。",
			"systemd-managed: same as systemctl start xray-agent."))
		fmt.Fprintln(w, i18n.T("手动安装：后台拉起一份 agent（脱离终端），agent 自身日志落在 xray 日志同目录的 agent.log。",
			"Manual install: spawns a detached agent; its own log goes to agent.log next to the xray log."))
	case "stop":
		fmt.Fprintln(w, i18n.T("systemd 托管：等价 systemctl stop xray-agent，agent 退出时会先优雅停掉它托管的 xray。",
			"systemd-managed: same as systemctl stop xray-agent; the agent stops its xray before exiting."))
		fmt.Fprintln(w, i18n.T("手动安装：终止后台 agent 进程并停掉 xray（不留孤儿进程）。",
			"Manual install: terminates the background agent and stops xray (no orphan processes)."))
	case "restart":
		fmt.Fprintln(w, i18n.T("systemd 托管：systemctl restart；手动安装：停止后重新在后台拉起。",
			"systemd-managed: systemctl restart; manual install: stop then start again in background."))
	case "status":
		fmt.Fprintln(w, i18n.T("显示运行方式、agent/xray 进程、日志占用与配置摘要。",
			"Shows how the agent runs, agent/xray processes, log usage and a config summary."))
	case "run":
		fmt.Fprintln(w, i18n.T("前台运行，日志打到终端（Ctrl+C 退出）。systemd 托管时需先 xray-agent stop。",
			"Runs in foreground with logs on the terminal (Ctrl+C to quit). Stop the systemd service first."))
	case "logs":
		fmt.Fprintln(w, i18n.T("systemd 托管：journalctl -u xray-agent；手动安装：读 xray 日志文件尾部。",
			"systemd-managed: journalctl -u xray-agent; manual install: tail the xray log file."))
		fmt.Fprintln(w, i18n.T("  -n <行数>     显示行数（默认 100，上限 500）", "  -n <lines>    number of lines (default 100, max 500)"))
		fmt.Fprintln(w, i18n.T("  -f/--follow   持续跟踪", "  -f/--follow   keep following"))
		fmt.Fprintln(w, i18n.T("  --size        只看日志占用与上限（不打印内容）", "  --size        show log usage and limit only"))
		fmt.Fprintln(w, i18n.T("  --vacuum      立即把日志裁剪到上限内（释放磁盘）", "  --vacuum      trim the log to the limit now (frees disk)"))
	case "uninstall":
		fmt.Fprintln(w, i18n.T("删除 systemd 单元、二进制、配置、日志与 geo 数据。",
			"Removes the systemd unit, binaries, config, logs and geo data."))
	case "upgrade":
		fmt.Fprintln(w, i18n.T("下载 → sha256 校验 → 原子替换 → 重启；--check 仅检查是否有新版本。",
			"Download → sha256 check → atomic replace → restart; --check only reports the latest version."))
	case "rollback":
		fmt.Fprintln(w, i18n.T("下载 → sha256 校验 → 原子替换 → 重启。",
			"Download → sha256 check → atomic replace → restart."))
		fmt.Fprintln(w, i18n.T("示例: xray-agent rollback v0.1.15", "Example: xray-agent rollback v0.1.15"))
	case "lang":
		fmt.Fprintln(w, i18n.T("语言状态存在 /run/xray-agent/lang（内存盘，重启恢复默认中文），不写数据库与配置文件。",
			"Language state lives in /run/xray-agent/lang (tmpfs; resets to Chinese on reboot). No database or config file is touched."))
		fmt.Fprintln(w, i18n.T("  zh           切换为中文（默认）", "  zh           switch to Chinese (default)"))
		fmt.Fprintln(w, i18n.T("  en           切换为英文", "  en           switch to English"))
		fmt.Fprintln(w, i18n.T("  auto         清除设置，回到默认", "  auto         clear the setting and fall back to default"))
		fmt.Fprintln(w, i18n.T("单次覆盖: XRAY_AGENT_LANG=en xray-agent help", "One-off override: XRAY_AGENT_LANG=en xray-agent help"))
	case "help":
		fmt.Fprintln(w, i18n.T("不带参数打印完整帮助；带命令名查看该命令详情。",
			"Without arguments prints full help; with a command name shows its details."))
	}
}

// newFlagSet 构造带空输出的 FlagSet（错误信息由调用方输出）。
func newFlagSet(name string) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	return fs
}

// ---------- status ----------

func runStatus(rest []string, stdout, stderr io.Writer) int {
	fs := newFlagSet("status")
	cfgPath := fs.String("config", "", i18n.T("配置文件路径", "config file path"))
	if err := fs.Parse(rest); err != nil {
		fmt.Fprintf(stderr, i18n.T("参数错误: %v\n", "invalid arguments: %v\n"), err)
		return 2
	}
	path := resolveConfigPath(*cfgPath)
	cfg, err := loadTolerant(path)
	if err != nil {
		fmt.Fprintln(stderr, i18n.T("status 失败:", "status failed:"), err)
		return 1
	}
	printStatus(stdout, snapshotStatus(cfg, path))
	return 0
}

// ---------- lang ----------

func runLang(rest []string, stdout, stderr io.Writer) int {
	if len(rest) == 0 {
		fmt.Fprintf(stdout, "%s: %s\n",
			i18n.T("当前语言", "Current language"), langLabel(i18n.Lang()))
		if env := i18n.EnvOverride(); env != "" {
			fmt.Fprintln(stdout, i18n.Tf("注意: 环境变量 XRAY_AGENT_LANG=%s 优先于 lang 设置",
				"Note: XRAY_AGENT_LANG=%s overrides the lang setting", env))
		}
		fmt.Fprintln(stdout)
		fmt.Fprintln(stdout, i18n.T("用法: xray-agent lang <zh|en|auto>", "Usage: xray-agent lang <zh|en|auto>"))
		fmt.Fprintln(stdout, i18n.T("      auto = 清除设置，回到默认中文", "      auto = clear the setting, fall back to Chinese"))
		return 0
	}

	target := strings.ToLower(strings.TrimSpace(rest[0]))
	switch target {
	case "auto", "default", "reset":
		if err := i18n.Clear(); err != nil {
			fmt.Fprintln(stderr, i18n.T("清除语言设置失败:", "failed to clear the language setting:"), err)
			return 1
		}
		fmt.Fprintln(stdout, i18n.T("已恢复默认语言：中文", "Language reset to default: Chinese"))
		return 0
	}

	lang, err := i18n.Set(target)
	if err != nil {
		if lang == "" {
			fmt.Fprintln(stderr, i18n.Tf("不支持的语言: %s（可选 zh / en / auto）",
				"unsupported language: %s (expected zh / en / auto)", target))
			return 2
		}
		fmt.Fprint(stderr, i18n.Tf("无法写入 %s: %v\n", "cannot write %s: %v\n", i18n.StatePath, err))
		fmt.Fprintln(stderr, i18n.T("可改用环境变量单次生效: XRAY_AGENT_LANG=en xray-agent help",
			"Use the environment variable instead: XRAY_AGENT_LANG=en xray-agent help"))
		return 1
	}
	fmt.Fprintln(stdout, i18n.Tf("界面语言已切换为 %s", "CLI language switched to %s", langLabel(lang)))
	if env := i18n.EnvOverride(); env != "" && env != lang {
		fmt.Fprintln(stdout, i18n.Tf("注意: 当前 shell 设置了 XRAY_AGENT_LANG=%s，它的优先级更高",
			"Note: this shell sets XRAY_AGENT_LANG=%s, which takes precedence", env))
	}
	return 0
}

func langLabel(lang string) string {
	if lang == i18n.EN {
		return "English (en)"
	}
	return "中文 (zh)"
}

// ---------- start / stop ----------

func runStart(rest []string, stdout, stderr io.Writer) int {
	fs := newFlagSet("start")
	cfgPath := fs.String("config", "", i18n.T("配置文件路径", "config file path"))
	if err := fs.Parse(rest); err != nil {
		fmt.Fprintf(stderr, i18n.T("参数错误: %v\n", "invalid arguments: %v\n"), err)
		return 2
	}
	path := resolveConfigPath(*cfgPath)
	cfg, err := loadTolerant(path)
	if err != nil {
		fmt.Fprintln(stderr, i18n.T("start 失败:", "start failed:"), err)
		return 1
	}

	if IsSystemdManaged() {
		if out, err := runCmd("systemctl", "start", "xray-agent"); err != nil {
			fmt.Fprintln(stderr, i18n.T("启动失败:", "failed to start:"), err)
			if out != "" {
				fmt.Fprintln(stderr, out)
			}
			return 1
		}
		state, _ := runCmd("systemctl", "is-active", "xray-agent")
		fmt.Fprintln(stdout, i18n.Tf("已启动 systemd 服务 xray-agent（状态: %s）",
			"Started systemd service xray-agent (state: %s)", orUnknown(state)))
		return 0
	}

	// 手动安装：后台拉起一份 agent（脱离终端），agent 自身日志写 agent.log。
	if pids := agentPids(cfg); len(pids) > 0 {
		fmt.Fprintln(stdout, i18n.Tf("agent 已在运行（PID %s）", "agent is already running (PID %s)", joinPids(pids)))
		return 0
	}
	return startManual(cfg, path, stdout, stderr)
}

// startManual 手动安装下的后台启动：spawn 一份 `run`（新会话、脱离终端），写 agent.pid。
func startManual(cfg *config.Config, cfgPath string, stdout, stderr io.Writer) int {
	exe, err := os.Executable()
	if err != nil {
		fmt.Fprintln(stderr, i18n.T("获取自身路径失败:", "cannot resolve own executable:"), err)
		return 1
	}
	args := []string{"run"}
	if cfgPath != "" {
		args = append(args, "-config", cfgPath)
	}
	logPath := agentLogPath(cfg)
	env := append(os.Environ(), AgentLogEnv+"="+logPath)
	pid, err := spawnDetached(exe, args, env)
	if err != nil {
		fmt.Fprintln(stderr, i18n.T("启动失败:", "failed to start:"), err)
		return 1
	}
	pidFile := agentPidFile(cfg)
	_ = os.MkdirAll(filepath.Dir(pidFile), 0o755)
	_ = os.WriteFile(pidFile, []byte(strconv.Itoa(pid)), 0o644)
	// 等一小会儿确认没有立刻退出（配置错误会在毫秒级失败）。
	if !waitAlive(pid, 1000*time.Millisecond) {
		_ = os.Remove(pidFile)
		fmt.Fprintln(stderr, i18n.Tf("启动失败：agent 进程 %d 立即退出，请查看日志 %s",
			"start failed: agent process %d exited immediately, see log %s", pid, logPath))
		return 1
	}
	fmt.Fprintln(stdout, i18n.Tf("已在后台启动 agent（PID %d）", "agent started in background (PID %d)", pid))
	fmt.Fprintln(stdout, i18n.Tf("  agent 日志: %s", "  agent log: %s", logPath))
	fmt.Fprintln(stdout, i18n.Tf("  xray 日志 : %s", "  xray log : %s", cfg.Xray.LogPath))
	fmt.Fprintln(stdout, i18n.T("  查看状态  : xray-agent status", "  status   : xray-agent status"))
	return 0
}

func runStop(rest []string, stdout, stderr io.Writer) int {
	fs := newFlagSet("stop")
	cfgPath := fs.String("config", "", i18n.T("配置文件路径", "config file path"))
	if err := fs.Parse(rest); err != nil {
		fmt.Fprintf(stderr, i18n.T("参数错误: %v\n", "invalid arguments: %v\n"), err)
		return 2
	}
	path := resolveConfigPath(*cfgPath)
	cfg, err := loadTolerant(path)
	if err != nil {
		fmt.Fprintln(stderr, i18n.T("stop 失败:", "stop failed:"), err)
		return 1
	}

	if IsSystemdManaged() {
		if IsSystemdService() {
			fmt.Fprintln(stderr, i18n.T("当前进程即 systemd 服务进程，请在终端执行 xray-agent stop",
				"this process is the systemd service; run `xray-agent stop` from a terminal"))
			return 1
		}
		if out, err := runCmd("systemctl", "stop", "xray-agent"); err != nil {
			fmt.Fprintln(stderr, i18n.T("停止失败:", "failed to stop:"), err)
			if out != "" {
				fmt.Fprintln(stderr, out)
			}
			return 1
		}
		state, _ := runCmd("systemctl", "is-active", "xray-agent")
		fmt.Fprintln(stdout, i18n.Tf("已停止 systemd 服务 xray-agent（状态: %s）",
			"Stopped systemd service xray-agent (state: %s)", orUnknown(state)))
		reportXrayStopped(stdout, cfg)
		return 0
	}

	pids := agentPids(cfg)
	if len(pids) > 0 {
		killPids(pids)
		fmt.Fprintln(stdout, i18n.Tf("已停止 agent（PID %s）", "agent stopped (PID %s)", joinPids(pids)))
	} else {
		fmt.Fprintln(stdout, i18n.T("agent 未在运行", "agent is not running"))
	}
	_ = os.Remove(agentPidFile(cfg))
	reportXrayStopped(stdout, cfg)
	return 0
}

// reportXrayStopped 停止后回读 xray 进程，明确告诉运维「确实停干净了」。
func reportXrayStopped(w io.Writer, cfg *config.Config) {
	proc := newProc(cfg)
	if !proc.IsRunning() {
		fmt.Fprintln(w, i18n.T("xray 进程: 已停止", "xray process: stopped"))
		return
	}
	// agent 正常退出会自己收掉 xray；没停掉时兜底再停一次（服务被强杀的场景）。
	if err := proc.Stop(); err != nil {
		fmt.Fprintln(w, i18n.T("xray 进程: 仍在运行，停止失败:", "xray process: still running, stop failed:"), err)
		return
	}
	fmt.Fprintln(w, i18n.T("xray 进程: 已停止（兜底清理）", "xray process: stopped (fallback cleanup)"))
}

// ---------- restart ----------

func runRestart(rest []string, stdout, stderr io.Writer) int {
	fs := newFlagSet("restart")
	cfgPath := fs.String("config", "", i18n.T("配置文件路径", "config file path"))
	if err := fs.Parse(rest); err != nil {
		fmt.Fprintf(stderr, i18n.T("参数错误: %v\n", "invalid arguments: %v\n"), err)
		return 2
	}
	path := resolveConfigPath(*cfgPath)
	cfg, err := loadTolerant(path)
	if err != nil {
		fmt.Fprintln(stderr, i18n.T("restart 失败:", "restart failed:"), err)
		return 1
	}

	if IsSystemdManaged() {
		if err := runCmdErr("systemctl", "restart", "xray-agent"); err != nil {
			fmt.Fprintln(stderr, i18n.T("restart 失败:", "restart failed:"), err)
			return 1
		}
		fmt.Fprintln(stdout, i18n.T("已重启 systemd 服务 xray-agent", "Restarted systemd service xray-agent"))
		return 0
	}

	// 手动安装：有后台 agent 就「停 → 后台拉起」（与 start/stop 同一套语义）。
	if pids := agentPids(cfg); len(pids) > 0 {
		killPids(pids)
		_ = os.Remove(agentPidFile(cfg))
		time.Sleep(500 * time.Millisecond) // 等端口/pid 文件释放
		return startManual(cfg, path, stdout, stderr)
	}

	// 没有后台 agent（前台 run 调试或未运行）：只重启 xray，保持旧语义。
	proc := newProc(cfg)
	if err := proc.Stop(); err != nil {
		fmt.Fprintln(stderr, i18n.T("停止 xray 失败:", "failed to stop xray:"), err)
		return 1
	}
	if err := proc.Start(); err != nil {
		fmt.Fprint(stderr, i18n.T("启动 xray 失败: ", "failed to start xray: "), err)
		fmt.Fprintln(stderr, i18n.T("（请先在主控生成并下发配置）", " (generate and push a config from the panel first)"))
		return 1
	}
	fmt.Fprintln(stdout, i18n.T("xray 已重启（手动模式；agent 未在后台运行）",
		"xray restarted (manual mode; agent is not running in background)"))
	return 0
}

// ---------- logs ----------

func runLogs(rest []string, stdout, stderr io.Writer) int {
	fs := newFlagSet("logs")
	cfgPath := fs.String("config", "", i18n.T("配置文件路径", "config file path"))
	lines := fs.Int("n", 100, i18n.T("行数", "line count"))
	follow := fs.Bool("follow", false, i18n.T("持续跟踪", "keep following"))
	fs.BoolVar(follow, "f", false, i18n.T("持续跟踪", "keep following"))
	showSize := fs.Bool("size", false, i18n.T("只看日志占用与上限", "show log usage and limit only"))
	vacuum := fs.Bool("vacuum", false, i18n.T("立即把日志裁剪到上限内", "trim the log to the limit now"))
	if err := fs.Parse(rest); err != nil {
		fmt.Fprintf(stderr, i18n.T("参数错误: %v\n", "invalid arguments: %v\n"), err)
		return 2
	}
	path := resolveConfigPath(*cfgPath)
	cfg, err := loadTolerant(path)
	if err != nil {
		fmt.Fprintln(stderr, i18n.T("logs 失败:", "logs failed:"), err)
		return 1
	}

	// --size / --vacuum 只做对应动作，不打印日志内容。
	if *showSize || *vacuum {
		if *vacuum {
			freed, err := logfile.SlideNow(cfg.Xray.LogPath, cfg.Xray.LogMaxBytes())
			if err != nil {
				fmt.Fprintln(stderr, i18n.T("裁剪失败:", "trim failed:"), err)
				return 1
			}
			if freed > 0 {
				fmt.Fprintln(stdout, i18n.Tf("已裁剪 %s，当前占用 %s", "trimmed %s, now %s",
					humanBytes(freed), humanBytes(logfile.Size(cfg.Xray.LogPath))))
			} else {
				fmt.Fprintln(stdout, i18n.Tf("无需裁剪（当前占用 %s）", "nothing to trim (now %s)",
					humanBytes(logfile.Size(cfg.Xray.LogPath))))
			}
		}
		if *showSize {
			fmt.Fprintln(stdout, i18n.Tf("日志文件: %s", "log file: %s", cfg.Xray.LogPath))
			fmt.Fprintln(stdout, i18n.Tf("当前占用: %s", "usage   : %s", humanBytes(logfile.Size(cfg.Xray.LogPath))))
			if max := cfg.Xray.LogMaxBytes(); max > 0 {
				fmt.Fprintln(stdout, i18n.Tf("上限    : %s（超过即滑动，保留尾部一半）",
					"limit   : %s (slides when exceeded, keeps the newest half)", humanBytes(max)))
			} else {
				fmt.Fprintln(stdout, i18n.T("上限    : 未限制（xray.log_max_size_mb = 0）",
					"limit   : unlimited (xray.log_max_size_mb = 0)"))
			}
			if ap := agentLogPath(cfg); logfile.Size(ap) > 0 {
				fmt.Fprintln(stdout, i18n.Tf("agent 日志: %s（%s）", "agent log: %s (%s)", ap, humanBytes(logfile.Size(ap))))
			}
		}
		return 0
	}

	if IsSystemdManaged() {
		jArgs := []string{"-u", "xray-agent", "--no-pager", "-n", strconv.Itoa(*lines)}
		if *follow {
			jArgs = append(jArgs, "-f")
		}
		cmd := exec.Command("journalctl", jArgs...)
		cmd.Stdout = stdout
		cmd.Stderr = stderr
		if err := cmd.Run(); err != nil {
			fmt.Fprintln(stderr, i18n.T("journalctl 失败:", "journalctl failed:"), err)
			return 1
		}
		return 0
	}

	if *follow {
		cmd := exec.Command("tail", "-f", "-n", strconv.Itoa(*lines), cfg.Xray.LogPath)
		cmd.Stdout = stdout
		cmd.Stderr = stderr
		if err := cmd.Run(); err != nil {
			fmt.Fprintln(stderr, i18n.T("tail 失败:", "tail failed:"), err)
			return 1
		}
		return 0
	}
	logs, err := logfile.Tail(cfg.Xray.LogPath, *lines)
	if err != nil {
		fmt.Fprintln(stderr, i18n.T("读取日志失败:", "failed to read the log:"), err)
		return 1
	}
	fmt.Fprintln(stdout, i18n.Tf("# xray 日志 (%s)", "# xray log (%s)", cfg.Xray.LogPath))
	if logs == "" {
		fmt.Fprintln(stdout, i18n.T("(无日志)", "(no logs)"))
	} else {
		fmt.Fprintln(stdout, logs)
	}
	fmt.Fprintln(stdout)
	fmt.Fprintln(stdout, i18n.T(
		"# 提示: 手动模式下 agent 自身日志见同目录 agent.log；systemd 模式请用 journalctl -u xray-agent",
		"# Hint: in manual mode the agent's own log is agent.log in the same directory; under systemd use journalctl -u xray-agent"))
	return 0
}

// ---------- uninstall ----------

// uninstallTargets 卸载目标清单。
type uninstallTargets struct {
	unitPaths []string
	files     []string
	dirs      []string
	pidFile   string
}

func collectTargets(cfg *config.Config) uninstallTargets {
	exe, _ := os.Executable()
	return uninstallTargets{
		unitPaths: systemdUnitPaths,
		files:     []string{cfg.Xray.Bin, exe},
		dirs:      []string{"/etc/xray-agent", "/var/log/xray-agent", "/run/xray-agent", "/usr/local/share/xray"},
		pidFile:   cfg.Xray.PidFile,
	}
}

// removeTargets 执行删除，返回警告列表。
func removeTargets(t uninstallTargets, w io.Writer) []string {
	var warns []string
	rm := func(p string, desc string) {
		if err := os.RemoveAll(p); err != nil {
			warns = append(warns, i18n.Tf("删除%s失败 %s: %v", "failed to remove %s %s: %v", desc, p, err))
		}
	}
	for _, f := range t.files {
		rm(f, i18n.T("文件", "file"))
	}
	for _, d := range t.dirs {
		rm(d, i18n.T("目录", "directory"))
	}
	if t.pidFile != "" {
		rm(t.pidFile, i18n.T("pid 文件", "pid file"))
	}
	return warns
}

func runUninstall(rest []string, stdin io.Reader, stdout, stderr io.Writer) int {
	fs := newFlagSet("uninstall")
	cfgPath := fs.String("config", "", i18n.T("配置文件路径", "config file path"))
	force := fs.Bool("force", false, i18n.T("跳过确认", "skip confirmation"))
	if err := fs.Parse(rest); err != nil {
		fmt.Fprintf(stderr, i18n.T("参数错误: %v\n", "invalid arguments: %v\n"), err)
		return 2
	}
	path := resolveConfigPath(*cfgPath)
	cfg, err := loadTolerant(path)
	if err != nil {
		fmt.Fprintln(stderr, i18n.Tf("警告: %v（按默认路径卸载）", "warning: %v (uninstalling with default paths)", err))
		cfg = config.Default()
	}

	if !*force {
		prompt := i18n.T("将卸载 xray-agent（含 xray-core 与 geo 数据），确认? [y/N] ",
			"This will uninstall xray-agent (incl. xray-core and geo data). Continue? [y/N] ")
		if !confirm(stdin, stdout, prompt) {
			fmt.Fprintln(stdout, i18n.T("已取消", "cancelled"))
			return 1
		}
	}

	exe, _ := os.Executable()
	mode := i18n.T("手动", "manual")
	if IsSystemdManaged() {
		mode = "systemd"
	}
	fmt.Fprintln(stdout, i18n.Tf("==> 停止服务（%s 模式）", "==> stopping the service (%s mode)", mode))

	if IsSystemdManaged() {
		_ = runCmdErr("systemctl", "stop", "xray-agent")
		_ = runCmdErr("systemctl", "disable", "xray-agent")
		for _, p := range systemdUnitPaths {
			_ = os.RemoveAll(p)
		}
		_ = runCmdErr("systemctl", "daemon-reload")
	} else {
		pids := findPids(exe, true)
		if len(pids) == 0 {
			pids = findPids("xray-agent", true)
		}
		if len(pids) > 0 {
			killPids(pids)
		}
	}

	proc := newProc(cfg)
	_ = proc.Stop()

	fmt.Fprintln(stdout, i18n.T("==> 删除文件", "==> removing files"))
	warns := removeTargets(collectTargets(cfg), stdout)
	for _, w := range warns {
		fmt.Fprintln(stderr, i18n.T("警告:", "warning:"), w)
	}

	fmt.Fprintln(stdout, i18n.T("==> 卸载完成", "==> uninstall complete"))
	fmt.Fprintln(stdout, i18n.T("提示: 可在主控管理端删除对应服务器节点以清理主控侧数据",
		"Hint: delete this server in the panel to clean up master-side data"))
	return 0
}

// ---------- upgrade ----------

func runUpgrade(rest []string, stdout, stderr io.Writer) int {
	fs := newFlagSet("upgrade")
	cfgPath := fs.String("config", "", i18n.T("配置文件路径", "config file path"))
	checkOnly := fs.Bool("check", false, i18n.T("仅检查版本", "check the version only"))
	if err := fs.Parse(rest); err != nil {
		fmt.Fprintf(stderr, i18n.T("参数错误: %v\n", "invalid arguments: %v\n"), err)
		return 2
	}
	path := resolveConfigPath(*cfgPath)
	cfg, err := loadTolerant(path)
	if err != nil {
		fmt.Fprintln(stderr, i18n.T("upgrade 失败:", "upgrade failed:"), err)
		return 1
	}

	f := &upgrade.Fetcher{Repo: cfg.Update.Repo, Mirror: cfg.Update.Mirror, DownloadTimeout: cfg.Update.DownloadTimeout}
	if *checkOnly {
		latest, err := f.Latest()
		if err != nil {
			fmt.Fprintln(stderr, i18n.T("检查失败:", "check failed:"), err)
			return 1
		}
		if upgrade.Compare(upgrade.CurrentVersion(), latest) >= 0 {
			fmt.Fprintln(stdout, i18n.Tf("已是最新（当前 %s，主控 %s）", "already up to date (current %s, master %s)",
				upgrade.CurrentVersion(), latest))
		} else {
			fmt.Fprintln(stdout, i18n.Tf("有新版本 %s（当前 %s），执行 xray-agent upgrade 升级",
				"new version %s available (current %s); run `xray-agent upgrade`", latest, upgrade.CurrentVersion()))
		}
		return 0
	}

	exe, err := os.Executable()
	if err != nil {
		fmt.Fprintln(stderr, i18n.T("获取自身路径失败:", "cannot resolve own executable:"), err)
		return 1
	}
	restart := func() error {
		if IsSystemdManaged() {
			return runCmdErr("systemctl", "restart", "xray-agent")
		}
		fmt.Fprintln(stdout, i18n.T("手动模式：请手动重启 agent（systemctl restart xray-agent 或重新运行）",
			"manual mode: restart the agent yourself (systemctl restart xray-agent or re-run)"))
		return nil
	}
	if err := upgrade.Apply(f, exe, restart, stdout); err != nil {
		if errors.Is(err, upgrade.ErrUpToDate) {
			return 0
		}
		fmt.Fprintln(stderr, i18n.T("升级失败:", "upgrade failed:"), err)
		return 1
	}
	return 0
}

// ---------- rollback ----------

func runRollback(rest []string, stdout, stderr io.Writer) int {
	fs := newFlagSet("rollback")
	cfgPath := fs.String("config", "", i18n.T("配置文件路径", "config file path"))
	if err := fs.Parse(rest); err != nil {
		fmt.Fprintf(stderr, i18n.T("参数错误: %v\n", "invalid arguments: %v\n"), err)
		return 2
	}
	args := fs.Args()
	if len(args) == 0 {
		fmt.Fprintln(stderr, i18n.T("缺少目标版本号（用法: xray-agent rollback [-config <path>] <版本号>）",
			"missing target version (usage: xray-agent rollback [-config <path>] <version>)"))
		return 2
	}
	target := strings.TrimSpace(args[0])
	if target == "" {
		fmt.Fprintln(stderr, i18n.T("目标版本号不能为空", "target version must not be empty"))
		return 2
	}
	if !strings.HasPrefix(target, "v") {
		target = "v" + target
	}
	path := resolveConfigPath(*cfgPath)
	cfg, err := loadTolerant(path)
	if err != nil {
		fmt.Fprintln(stderr, i18n.T("rollback 失败:", "rollback failed:"), err)
		return 1
	}

	exe, err := os.Executable()
	if err != nil {
		fmt.Fprintln(stderr, i18n.T("获取自身路径失败:", "cannot resolve own executable:"), err)
		return 1
	}
	f := &upgrade.Fetcher{Repo: cfg.Update.Repo, Mirror: cfg.Update.Mirror, DownloadTimeout: cfg.Update.DownloadTimeout}
	restart := func() error {
		if IsSystemdManaged() {
			return runCmdErr("systemctl", "restart", "xray-agent")
		}
		fmt.Fprintln(stdout, i18n.T("手动模式：请手动重启 agent（systemctl restart xray-agent 或重新运行）",
			"manual mode: restart the agent yourself (systemctl restart xray-agent or re-run)"))
		return nil
	}
	if err := upgrade.ApplyTarget(f, target, true, exe, restart, stdout); err != nil {
		if errors.Is(err, upgrade.ErrUpToDate) {
			return 0
		}
		fmt.Fprintln(stderr, i18n.T("回滚失败:", "rollback failed:"), err)
		return 1
	}
	return 0
}

// ---------- 工具 ----------

// IsSystemdManaged 判断本机是否安装了 xray-agent 的 systemd 单元（无论服务是否在运行）。
func IsSystemdManaged() bool {
	for _, p := range systemdUnitPaths {
		if _, err := os.Stat(p); err == nil {
			return true
		}
	}
	return false
}

// IsSystemdService 判断当前进程是否由 systemd 拉起：服务进程带 INVOCATION_ID 环境变量，
// 终端手动执行无此变量——用于区分「服务内启动」与「交互式误启动」。
func IsSystemdService() bool {
	_, ok := os.LookupEnv("INVOCATION_ID")
	return ok
}

func confirm(r io.Reader, w io.Writer, prompt string) bool {
	fmt.Fprint(w, prompt)
	line, err := bufio.NewReader(r).ReadString('\n')
	if err != nil && line == "" {
		return false
	}
	ans := strings.ToLower(strings.TrimSpace(line))
	return ans == "y" || ans == "yes"
}

// findPids 用 pgrep -f 查找匹配进程，可排除自身。
func findPids(pattern string, excludeSelf bool) []int {
	out, err := exec.Command("pgrep", "-f", pattern).Output()
	if err != nil {
		return nil // 无匹配或 pgrep 不存在
	}
	var pids []int
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		pid, perr := strconv.Atoi(strings.TrimSpace(line))
		if perr != nil || pid <= 0 {
			continue
		}
		if excludeSelf && pid == os.Getpid() {
			continue
		}
		pids = append(pids, pid)
	}
	return pids
}

func runCmd(name string, args ...string) (string, error) {
	out, err := exec.Command(name, args...).CombinedOutput()
	return strings.TrimSpace(string(out)), err
}

func runCmdErr(name string, args ...string) error {
	cmd := exec.Command(name, args...)
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	return cmd.Run()
}

func readPidFile(path string) int {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	pid, _ := strconv.Atoi(strings.TrimSpace(string(data)))
	return pid
}

// procUptime 读取 /proc/<pid>/stat 的 starttime，返回已运行时长（如 "3h12m"；未知返回空）。
func procUptime(pid int) string {
	if pid <= 0 {
		return ""
	}
	stat, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return ""
	}
	s := string(stat)
	i := strings.LastIndex(s, ") ")
	if i < 0 {
		return ""
	}
	fields := strings.Fields(s[i+2:])
	if len(fields) <= 19 {
		return ""
	}
	ticks, err := strconv.ParseFloat(fields[19], 64)
	if err != nil {
		return ""
	}
	up, err := os.ReadFile("/proc/uptime")
	if err != nil {
		return ""
	}
	upSec, _ := strconv.ParseFloat(strings.Fields(string(up))[0], 64)
	sec := int64(upSec - ticks/100) // CLK_TCK=100
	if sec < 0 {
		return ""
	}
	return formatDur(sec)
}

func formatDur(sec int64) string {
	if sec >= 3600 {
		return fmt.Sprintf("%dh%dm", sec/3600, sec%3600/60)
	}
	if sec >= 60 {
		return fmt.Sprintf("%dm%ds", sec/60, sec%60)
	}
	return fmt.Sprintf("%ds", sec)
}
