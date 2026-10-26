package cli

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/acdc-awa/xpanel-node/internal/agent/config"
	"github.com/acdc-awa/xpanel-node/internal/agent/i18n"
	"github.com/acdc-awa/xpanel-node/internal/agent/logfile"
	"github.com/acdc-awa/xpanel-node/internal/agent/upgrade"
	"github.com/acdc-awa/xpanel-node/internal/agent/xrayproc"
)

// AgentLogEnv 手动（无 systemd）后台模式下的 agent 自身日志路径：
// start 通过环境变量把路径传给子进程，main 里按同一套滑动策略落盘。
const AgentLogEnv = "XRAY_AGENT_LOG_FILE"

// SetupAgentLog 后台模式下把 agent 自身日志同时写文件（与 xray 日志同一套滑动策略）。
// 返回的 io.Closer 由调用方在进程退出时关闭。
func SetupAgentLog(path string, maxBytes int64) (io.Closer, error) {
	f, err := logfile.Open(path, maxBytes)
	if err != nil {
		return nil, err
	}
	log.SetOutput(io.MultiWriter(os.Stderr, f))
	return f, nil
}

// statusSnapshot 一次采集的状态快照（status 与裸执行命令面板共用）。
type statusSnapshot struct {
	ConfigPath   string
	MasterURL    string
	NodeID       string
	Systemd      bool
	SystemdState string
	AgentPIDs    []int
	XrayRunning  bool
	XrayPID      int
	XrayUptime   string
	Unmanaged    []int
	Bin          string
	XrayConfig   string
	LogPath      string
	LogSize      int64
	LogMaxBytes  int64
}

// newProc 按配置构造托管器（日志上限一并带上，避免各处漏配）。
func newProc(cfg *config.Config) *xrayproc.Proc {
	p := xrayproc.New(cfg.Xray.Bin, cfg.Xray.ConfigPath, cfg.Xray.LogPath, cfg.Xray.PidFile)
	p.LogMaxBytes = cfg.Xray.LogMaxBytes()
	return p
}

// snapshotStatus 采集状态（只做只读探测：systemctl / pgrep / stat）。
func snapshotStatus(cfg *config.Config, path string) statusSnapshot {
	s := statusSnapshot{
		ConfigPath:  path,
		MasterURL:   cfg.Master.URL,
		NodeID:      cfg.Master.NodeID,
		Bin:         cfg.Xray.Bin,
		XrayConfig:  cfg.Xray.ConfigPath,
		LogPath:     cfg.Xray.LogPath,
		LogSize:     logfile.Size(cfg.Xray.LogPath),
		LogMaxBytes: cfg.Xray.LogMaxBytes(),
		AgentPIDs:   agentPids(cfg),
	}
	if IsSystemdManaged() {
		s.Systemd = true
		active, _ := runCmd("systemctl", "is-active", "xray-agent")
		enabled, _ := runCmd("systemctl", "is-enabled", "xray-agent")
		s.SystemdState = orUnknown(active) + ", " + orUnknown(enabled)
	}
	proc := newProc(cfg)
	s.XrayRunning = proc.IsRunning()
	if s.XrayRunning {
		s.XrayPID = readPidFile(cfg.Xray.PidFile)
		s.XrayUptime = procUptime(s.XrayPID)
	}
	s.Unmanaged = proc.UnmanagedPIDs()
	return s
}

// printStatus 渲染完整状态（xray-agent status）。
func printStatus(w io.Writer, s statusSnapshot) {
	mode := i18n.T("手动", "manual")
	if s.Systemd {
		mode = "systemd"
	}
	statusLine(w, i18n.T("配置文件", "Config file"), s.ConfigPath)
	statusLine(w, i18n.T("主控地址", "Master URL"), s.MasterURL)
	statusLine(w, i18n.T("节点 ID", "Node ID"), s.NodeID)
	statusLine(w, i18n.T("运行方式", "Mode"), mode)
	if s.Systemd {
		statusLine(w, i18n.T("systemd 服务", "systemd service"), s.SystemdState)
	}
	if len(s.AgentPIDs) > 0 {
		statusLine(w, i18n.T("agent 进程", "agent process"), i18n.Tf("运行中 (PID %s)", "running (PID %s)", joinPids(s.AgentPIDs)))
	} else {
		statusLine(w, i18n.T("agent 进程", "agent process"), i18n.T("未运行", "not running"))
	}
	if s.XrayRunning {
		statusLine(w, i18n.T("xray 进程", "xray process"), i18n.Tf("运行中 (PID %d%s)", "running (PID %d%s)", s.XrayPID, uptimeSuffix(s.XrayUptime)))
	} else {
		statusLine(w, i18n.T("xray 进程", "xray process"), i18n.T("未运行", "not running"))
	}
	if len(s.Unmanaged) > 0 {
		statusLine(w, i18n.T("非托管 xray", "Unmanaged xray"),
			i18n.Tf("PID %s（其输出不经过 agent，不会写入 xray 日志）",
				"PID %s (its output bypasses the agent and never reaches the xray log)", joinPids(s.Unmanaged)))
	}
	statusLine(w, i18n.T("xray 日志", "xray log"),
		i18n.Tf("%s（%s / 上限 %s）", "%s (%s / limit %s)", s.LogPath, humanBytes(s.LogSize), humanLogCap(s.LogMaxBytes)))
	statusLine(w, i18n.T("xray 二进制", "xray binary"), s.Bin)
	statusLine(w, i18n.T("xray 配置", "xray config"), s.XrayConfig)
}

// Dashboard 裸执行 `xray-agent` 时的界面：状态头 + 分组命令面板。
// 之前这里只打一句「已由 systemd 托管」，对运维没有任何信息量。
func Dashboard(stdout io.Writer) int {
	fmt.Fprintf(stdout, i18n.T("xray-agent %s · Xray 节点 Agent\n", "xray-agent %s · Xray Node Agent\n"),
		upgrade.CurrentVersion())

	path := resolveConfigPath("")
	if cfg, err := loadTolerant(path); err == nil {
		s := snapshotStatus(cfg, path)
		fmt.Fprintln(stdout)
		fmt.Fprintln(stdout, "  "+i18n.T("服务状态", "Status"))
		mode := i18n.T("手动（无 systemd 单元）", "manual (no systemd unit)")
		if s.Systemd {
			mode = i18n.T("systemd（已托管）", "systemd (managed)")
		}
		line(stdout, i18n.T("运行方式", "Mode"), mode)
		if len(s.AgentPIDs) > 0 {
			line(stdout, "agent", i18n.Tf("运行中 · PID %s", "running · PID %s", joinPids(s.AgentPIDs)))
		} else {
			line(stdout, "agent", i18n.T("未运行", "not running"))
		}
		if s.XrayRunning {
			line(stdout, "xray", i18n.Tf("运行中 · PID %d%s", "running · PID %d%s", s.XrayPID, uptimeSuffix(s.XrayUptime)))
		} else {
			line(stdout, "xray", i18n.T("未运行", "not running"))
		}
		if len(s.Unmanaged) > 0 {
			line(stdout, i18n.T("非托管 xray", "Unmanaged xray"),
				i18n.Tf("PID %s（日志不经过 agent）", "PID %s (logs bypass the agent)", joinPids(s.Unmanaged)))
		}
		line(stdout, i18n.T("配置文件", "Config file"), s.ConfigPath)
		line(stdout, i18n.T("主控", "Master"), s.MasterURL)
		line(stdout, i18n.T("节点", "Node"), s.NodeID)
		line(stdout, i18n.T("xray 日志", "xray log"),
			i18n.Tf("%s（%s / 上限 %s）", "%s (%s / limit %s)", s.LogPath, humanBytes(s.LogSize), humanLogCap(s.LogMaxBytes)))
	}

	printCommandGroups(stdout, "  ")

	fmt.Fprintln(stdout)
	fmt.Fprintln(stdout, "  "+i18n.T("提示", "Notes"))
	if i18n.Lang() == i18n.EN {
		fmt.Fprintln(stdout, "    · start/stop/restart act on the background service (systemctl when systemd-managed).")
		fmt.Fprintln(stdout, "    · Stopping lets the agent exit gracefully and reap its xray child; no orphans left.")
		fmt.Fprintln(stdout, "    · Foreground debugging: `xray-agent stop`, then `xray-agent run`.")
		fmt.Fprintln(stdout, "    · Language: `xray-agent lang en|zh` (kept in memory, never written to the database).")
	} else {
		fmt.Fprintln(stdout, "    · start/stop/restart 都是后台行为，等价于 systemctl 对应操作（systemd 托管时）。")
		fmt.Fprintln(stdout, "    · 停止会先让 agent 优雅退出，由它自己收掉 xray 子进程，不留孤儿。")
		fmt.Fprintln(stdout, "    · 前台调试：先 xray-agent stop，再 xray-agent run。")
		fmt.Fprintln(stdout, "    · 语言切换：xray-agent lang en|zh（只存内存，不落库）。")
	}
	return 0
}

// statusLine "标签 : 值"（按当前语言的标签宽度对齐）。
func statusLine(w io.Writer, label, value string) {
	fmt.Fprintf(w, "%s: %s\n", padLabel(label, labelWidth()), value)
}

// line 面板里的一行「标签  值」：按显示宽度对齐（中文占两列），避免中英混排时值参差不齐。
func line(w io.Writer, label, value string) {
	fmt.Fprintf(w, "    %s %s\n", padLabel(label, labelWidth()), value)
}

// labelWidth 标签列宽随语言变化（英文标签更长，如 "Unmanaged xray"）。
func labelWidth() int {
	if i18n.Lang() == i18n.EN {
		return 15
	}
	return 12
}

// padLabel 按终端显示宽度右补空格；宽字符（CJK/全角）按 2 列计。
func padLabel(s string, width int) string {
	if w := displayWidth(s); w < width {
		return s + strings.Repeat(" ", width-w)
	}
	return s
}

func displayWidth(s string) int {
	w := 0
	for _, r := range s {
		if isWideRune(r) {
			w += 2
		} else {
			w++
		}
	}
	return w
}

func isWideRune(r rune) bool {
	switch {
	case r >= 0x1100 && r <= 0x115F, // Hangul Jamo
		r >= 0x2E80 && r <= 0xA4CF, // CJK 部首 / 汉字 / 注音 / 假名
		r >= 0xAC00 && r <= 0xD7A3, // Hangul 音节
		r >= 0xF900 && r <= 0xFAFF, // CJK 兼容汉字
		r >= 0xFE30 && r <= 0xFE6F, // CJK 兼容形式
		r >= 0xFF00 && r <= 0xFF60, // 全角字符
		r >= 0xFFE0 && r <= 0xFFE6:
		return true
	}
	return false
}

// agentPids 找出正在运行的 agent 进程：先信 agent.pid，再用 pgrep 兜底（旧版本没有 pid 文件）。
func agentPids(cfg *config.Config) []int {
	var pids []int
	seen := map[int]bool{}
	add := func(pid int) {
		if pid > 0 && !seen[pid] && processAlive(pid) && pid != os.Getpid() {
			seen[pid] = true
			pids = append(pids, pid)
		}
	}
	exe, _ := os.Executable()
	add(readPidFile(agentPidFile(cfg)))
	if exe != "" {
		for _, pid := range findPids(exe, true) {
			if isAgentProcess(pid, exe) {
				add(pid)
			}
		}
	}
	for _, pid := range findPids("xray-agent", true) {
		if isAgentProcess(pid, exe) {
			add(pid)
		}
	}
	return pids
}

// agentPidFile agent 自身 pid 文件（与 xray.pid 同目录，便于手动模式精确停止）。
func agentPidFile(cfg *config.Config) string {
	return filepath.Join(filepath.Dir(cfg.Xray.PidFile), "agent.pid")
}

// agentLogPath 手动后台模式下 agent 自身的日志（与 xray 日志同目录）。
func agentLogPath(cfg *config.Config) string {
	return filepath.Join(filepath.Dir(cfg.Xray.LogPath), "agent.log")
}

func joinPids(pids []int) string {
	parts := make([]string, 0, len(pids))
	for _, pid := range pids {
		parts = append(parts, strconv.Itoa(pid))
	}
	return strings.Join(parts, ", ")
}

func orUnknown(s string) string {
	if strings.TrimSpace(s) == "" {
		return i18n.T("未知", "unknown")
	}
	return s
}

// humanBytes 人类可读体积（1 KiB = 1024）。
func humanBytes(n int64) string {
	if n < 1024 {
		return fmt.Sprintf("%d B", n)
	}
	units := []string{"KiB", "MiB", "GiB", "TiB"}
	v := float64(n)
	for _, u := range units {
		v /= 1024
		if v < 1024 {
			return fmt.Sprintf("%.1f %s", v, u)
		}
	}
	return fmt.Sprintf("%.1f PiB", v/1024)
}

func humanLogCap(n int64) string {
	if n <= 0 {
		return i18n.T("不限制", "unlimited")
	}
	return humanBytes(n)
}

// uptimeSuffix 把「已运行 3h12m」拼到状态行尾（未知时长返回空）。
func uptimeSuffix(dur string) string {
	if dur == "" {
		return ""
	}
	return i18n.Tf("，已运行 %s", ", up %s", dur)
}
