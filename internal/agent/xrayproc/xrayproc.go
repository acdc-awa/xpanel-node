// Package xrayproc 托管 xray-core 子进程：启动/停止/配置校验重启/看门狗拉起。
// 结论依据《知识状态清单》A 类实测：SIGTERM 优雅退出(exit 0)、SIGKILL(137)、
// `-test` 配置错误 exit 23、启动失败 exit 255、watchdog 2s 拉起。
//
// 「启动成功」的判据（2026-09-21 实机修正）：xray 的致命错误分两段——`-test`
// （= core.LoadConfig + core.New，只做配置解析）与 server.Start()（真正绑定端口、构建路由）。
// 端口被占这类问题 -test 完全看不见（实测 `-test` 打印 Configuration OK. 而 run 秒死
// `Failed to start: ... failed to listen TCP on 443 ... bind: ...`），进程 exec 成功后
// 立刻退出。因此 Start() 必须等一个就绪窗口确认进程没退出才算"已启动"，并把退出码与
// stderr 摘要留档（Health）供心跳回传主控；看门狗按连续失败退避，达上限后停止自动拉起
// 但保留低频「慢探底」（环境类故障修好后无需人工介入即可自愈）。
//
// 另一处配套约束（2026-09-21）：主控每 2 分钟重推一次待推送配置，而"过了 -test 却起不来"
// 的配置每次应用都要先停掉正在服务的 xray 再回滚——同一份内容失败后进入冷却期，
// 冷却内的重复推送直接拒绝且不碰运行中的进程（见 RestartWithConfig）。
package xrayproc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	// maxStartFailures 连续启动失败达到该次数即放弃自动拉起（停止永无止境的重启），
	// 状态与原因由心跳上报主控报警；面板「重启 Xray」或新的 push_config 会重新给机会。
	// 取 8 是为了让退避真正走到 backoffMax（见 backoffFor），否则曲线在第 5 次就被截断。
	maxStartFailures = 8

	// stderrTailBytes 留档的 stderr 尾部容量（错误摘要只取关键行）。
	stderrTailBytes = 4096

	// goodSuffix 上一份成功启动过的配置副本，供开机 -test 失败时回退。
	goodSuffix = ".good"
)

// 以下为可注入的时序参数（测试用小值替换，避免用例等真实退避）：
//   - startFailureGrace 进程存活时间短于该阈值才计入「连续启动失败」（拉起失败）；
//   - watchdogInterval 看门狗巡检周期（兼作"进程已死"的发现延迟）；
//   - readyWindow spawn 后判定"起来了"的就绪窗口，覆盖 xray 百毫秒级的致命退出；
//   - backoffBase/backoffMax 连续失败的重试退避区间；
//   - probeInterval 放弃自动拉起后的慢探底周期；
//   - rejectCooldown 同一份配置内容应用失败后的重复推送冷却期。
var (
	startFailureGrace = 60 * time.Second
	watchdogInterval  = 2 * time.Second
	readyWindow       = 800 * time.Millisecond
	backoffBase       = 2 * time.Second
	backoffMax        = 60 * time.Second
	probeInterval     = 10 * time.Minute
	rejectCooldown    = 5 * time.Minute
	// crashWindow/crashWindowCount：窗口外死亡（跑了一阵才崩）的滑动窗口。这类死亡原本每次
	// 把连续失败重置为 1，于是「每隔 60s+ 死一次」（如被 OOM 杀）永远到不了上限、永不告警、
	// 永远以 2s 节奏重启；窗口内累计到阈值即视为故障，与秒死共用同一套计数与放弃通道。
	crashWindow      = 10 * time.Minute
	crashWindowCount = 3
)

// stopGracePeriod SIGTERM 后的优雅退出等待上限。
const stopGracePeriod = 5 * time.Second

// run 一次子进程运行的可观测状态。退出信息由 Wait goroutine 写入、写入完毕后关闭 exited，
// 其它 goroutine 在 exited 关闭后读取（关闭操作提供 happens-before）；absorbed 仅在 p.mu 下读写。
type run struct {
	tail     *ringBuffer
	manual   atomic.Bool
	exited   chan struct{}
	code     int
	at       time.Time
	absorbed bool
	// logFile 本次运行的日志文件。子进程的 stdout/stderr 经 os/exec 的管道 + 拷贝 goroutine
	// 落到这里（writer 不是 *os.File 时 os/exec 必建管道），因此**必须持有到子进程退出为止**：
	// 提前关闭会让拷贝 goroutine 写失败退出，os/exec 随即关掉管道读端，子进程下一次写
	// stdout 就收到 SIGPIPE 当场死亡（见 logSink 注释）。
	logFile *os.File
}

// Health xray 健康快照（心跳上报主控，面板据此显示状态与失败原因）。
type Health struct {
	State     string    // running / restarting / failed / stopped
	LastError string    // 最近一次启动失败原因（含已恢复的历史原因，UI 自行决定是否淡显）
	ErrorAt   time.Time // 该原因的观测时刻
	Failures  int       // 连续启动失败次数（启动成功后归零）
	GaveUp    bool      // 是否已停止自动拉起
}

// Proc 管理单个 xray 实例。
type Proc struct {
	Bin        string
	ConfigPath string
	LogPath    string
	PidFile    string

	mu        sync.Mutex
	run       *run
	started   bool // 是否曾启动（用于看门狗判定"崩溃后拉起"）
	startedAt time.Time
	OnRestart func() // 崩溃后被 Watchdog 成功拉起时的回调
	// OnStarted xray 按磁盘配置成功启动后的回调（含「已在运行」的早退路径）。
	// agent 用它按该配置重建用户缓存（不变量 I5）：冷更、回滚、崩溃自愈、慢探底、手动重启
	// 都会换掉运行中 xray 的用户集，缓存必须跟着换，而不是清空（见 stats.SeedUsers）。
	OnStarted func(configPath string)
	// OnGiveUp 连续启动失败达上限、停止自动拉起时的告警回调（agent 用它立刻推一帧心跳，
	// 让主控/面板马上看到原因，而不是干等下一个心跳周期）。
	OnGiveUp func(reason string)

	// 启动失败可观测性（2026-09-21）：连续失败计数、最近原因、退避时刻与放弃标志。
	failures     int
	gaveUp       bool
	lastErr      string
	lastErrAt    time.Time
	nextTryAt    time.Time
	giveUpNotice string      // 待投递的放弃告警：锁内置位，看门狗锁外取走并回调
	crashTimes   []time.Time // 窗口外死亡的时刻（滑动窗口，见 crashWindow）

	// 放弃前的一次性 .good 回退（2026-09-21）：回滚原本只存在于 RestartWithConfig 的
	// Start 失败分支，而"跑了一阵才崩"走不到那里。goodTried 保证每次机会只试一次。
	goodTried           bool
	goodRollbackPending bool // 待执行的 .good 回退：锁内置位，看门狗锁外执行

	// runningHash 本进程最后一次成功启动时磁盘上的配置内容哈希（心跳上报主控对账用）。
	runningHash string

	// 同内容冷却（2026-09-21）：主控每 2 分钟重推一次待推送配置，而"过了 -test 却起不来"
	// 的配置每应用一次都要先停掉正在服务的 xray 再回滚；同一份内容失败后进入冷却期，
	// 冷却内的重复推送直接拒绝且不碰运行中的进程（见 RestartWithConfig）。
	rejectedHash string
	rejectedAt   time.Time
	rejectedErr  string
}

// New 构造托管器。
func New(bin, configPath, logPath, pidFile string) *Proc {
	return &Proc{Bin: bin, ConfigPath: configPath, LogPath: logPath, PidFile: pidFile}
}

// testTimeout 是 xray -test 的最长等待时间；超时视为配置校验失败，
// 避免因 xray 进程卡死导致 agent 消息循环永久阻塞。
const testTimeout = 10 * time.Second

// TestConfig 用 `xray -test` 校验配置（exit 0 = 通过）。
func (p *Proc) TestConfig(path string) error {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, p.Bin, "-test", "-config", path)
	out, err := cmd.CombinedOutput()
	if err != nil {
		if ctx.Err() != nil {
			return fmt.Errorf("xray -test 超时（%v）", testTimeout)
		}
		// 退出码 23 = 配置错误（实测）；其他为启动异常
		return fmt.Errorf("xray -test 未通过: %v / %s", err, strings.TrimSpace(string(out)))
	}
	return nil
}

// ownedPIDs 列出 cmdline 含本配置路径的 xray 实例 pid。
// 首选 pgrep -f 匹配完整 config_path（唯一），避免误杀；pgrep 自动排除自身。
// 精简镜像常不带 procps（pgrep 缺失），此时退回 /proc 扫描——否则归属校验恒为 false，
// Stop() 会退化成"只删 pid 文件、不杀进程"，留下跑着却没人管的孤儿 xray。
func (p *Proc) ownedPIDs() []int {
	if _, err := exec.LookPath("pgrep"); err == nil {
		out, perr := exec.Command("pgrep", "-f", p.ConfigPath).Output()
		if perr == nil {
			var pids []int
			for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
				pid, aerr := strconv.Atoi(strings.TrimSpace(line))
				if aerr == nil && pid > 0 && pid != os.Getpid() {
					pids = append(pids, pid)
				}
			}
			return pids
		}
		// pgrep 无匹配时 exit 1（正常）；其它错误交给 /proc 兜底
		if ee := new(exec.ExitError); errors.As(perr, &ee) {
			return nil
		}
	}
	return scanProcForConfig(p.ConfigPath, os.Getpid())
}

// pidOwned 校验 pid 是否属于本配置路径的 xray 实例（P1-2：防 pid 文件陈旧 + pid 复用误杀）。
func (p *Proc) pidOwned(pid int) bool {
	for _, owned := range p.ownedPIDs() {
		if owned == pid {
			return true
		}
	}
	return false
}

// CleanupStale 停止同配置路径的残留 xray 实例（agent 异常退出遗留的孤儿进程）。
func (p *Proc) CleanupStale() {
	pids := p.ownedPIDs()
	if len(pids) == 0 {
		return
	}
	for _, pid := range pids {
		_ = killProcess(pid, syscall.SIGTERM)
	}
	// 等待残留进程退出（最多 3s）
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		left := false
		for _, pid := range pids {
			if isProcessAlive(pid) {
				left = true
				break
			}
		}
		if !left {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// Start 启动 xray 子进程（配置已存在时使用）。
// 返回 nil 表示"确实起来了"：spawn 成功且进程活过了就绪窗口；秒死则返回带退出码与
// stderr 摘要的错误（并计入连续失败计数）。
func (p *Proc) Start() error {
	p.mu.Lock()
	err := p.startLocked(false)
	p.mu.Unlock()
	if err != nil {
		return err
	}
	p.notifyStarted()
	return nil
}

// startProbe 慢探底：与 Start 同一套启动流程，区别只在失败路径——只更新失败原因与下次
// 探底时刻，不计入连续失败、不重复投递放弃告警（面板上"连续失败 N 次"停在放弃那一刻）。
func (p *Proc) startProbe() error {
	p.mu.Lock()
	err := p.startLocked(true)
	p.mu.Unlock()
	if err != nil {
		return err
	}
	p.notifyStarted()
	return nil
}

// notifyStarted 在锁外投递「xray 已按 ConfigPath 起来」。回调会读配置文件并取别的锁
// （agent 用它重建用户缓存），持锁调用会与 Stop/Start 互等；与放弃告警同样是
// 「锁内置位、锁外投递」的写法。
func (p *Proc) notifyStarted() {
	p.mu.Lock()
	onStarted, configPath := p.OnStarted, p.ConfigPath
	p.mu.Unlock()
	if onStarted != nil {
		onStarted(configPath)
	}
}

func (p *Proc) startLocked(probe bool) error {
	if p.IsRunning() {
		p.started = true
		if p.runningHash == "" {
			if data, rerr := os.ReadFile(p.ConfigPath); rerr == nil {
				p.runningHash = contentHash(string(data))
			}
		}
		return nil // 已在运行
	}
	if _, err := os.Stat(p.ConfigPath); err != nil {
		reason := fmt.Sprintf("xray 配置不存在: %s", p.ConfigPath)
		p.noteFailureLocked(reason, time.Now(), true, probe)
		return errors.New(reason)
	}

	if err := os.MkdirAll(filepath.Dir(p.LogPath), 0o755); err != nil {
		return err
	}
	logFile, err := os.OpenFile(p.LogPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}

	cmd := exec.Command(p.Bin, "run", "-c", p.ConfigPath)
	r := &run{tail: newRingBuffer(stderrTailBytes), exited: make(chan struct{}), logFile: logFile}
	// stderr/stdout 同时落日志文件与环形缓冲：文件是运维现场（跨次累积），
	// 缓冲用于把「本次启动」的致命错误摘要回传主控。logSink 保证写日志失败不会连带杀死子进程。
	cmd.Stdout = io.MultiWriter(logSink{logFile}, r.tail)
	cmd.Stderr = io.MultiWriter(logSink{logFile}, r.tail)
	setSysProcAttr(cmd)
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		reason := fmt.Sprintf("xray 启动失败: %v", err)
		p.noteFailureLocked(reason, time.Now(), true, probe)
		return errors.New(reason)
	}
	p.run = r
	p.started = true
	p.startedAt = time.Now()
	if err := os.MkdirAll(filepath.Dir(p.PidFile), 0o755); err == nil {
		_ = os.WriteFile(p.PidFile, []byte(strconv.Itoa(cmd.Process.Pid)), 0o644)
	}
	// 异步 Wait 回收子进程，避免僵尸（僵尸会导致 kill(pid,0) 误判存活），
	// 同时把退出码与 stderr 尾部留档——旧实现直接丢弃，导致"为什么没起来"无从回答。
	// 日志文件在 Wait 返回后关闭：此时 os/exec 的拷贝 goroutine 已结束，关闭不再影响任何人。
	go func() {
		werr := cmd.Wait()
		r.code = exitCodeOf(werr)
		r.at = time.Now()
		_ = logFile.Close()
		close(r.exited)
	}()

	// 就绪窗口：进程在窗口内退出 = 启动失败（xray 的 bind 失败/配置错误都在百毫秒级退出）。
	select {
	case <-r.exited:
		return fmt.Errorf("xray 启动后立即退出（%s）", p.absorbExitLocked(r, probe))
	case <-time.After(readyWindow):
	}
	// 进程活过就绪窗口：清除退避重试时刻
	p.nextTryAt = time.Time{}
	if probe {
		// 慢探底成功：环境已恢复，解除放弃态与失败计数
		p.failures, p.gaveUp = 0, false
	} else if !p.startedAt.IsZero() && time.Since(p.startedAt) >= startFailureGrace {
		// 存活时间已满足稳定运行阈值：清零连续失败与回退标志
		p.failures, p.gaveUp, p.goodTried = 0, false, false
	}
	// 注意：p.crashTimes 为 10 分钟滑动窗口，绝不在就绪窗口结束时清空；
	// 它按时间自淘汰，或在 ResetFailures 时显式清零。
	// 记下「本进程启动时磁盘上的内容」：心跳把它与磁盘哈希一起上报，主控据此判断
	// 节点跑的是不是它以为的那份配置（磁盘被第三方改过时两个哈希会分叉）。
	if data, rerr := os.ReadFile(p.ConfigPath); rerr == nil {
		p.runningHash = contentHash(string(data))
	}
	return nil
}

// Stop 停止 xray（SIGTERM 优雅退出，超时后 SIGKILL）。
func (p *Proc) Stop() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.started = false
	// 标记本次退出是主动停止：Wait goroutine 据此分类，不计入启动失败。
	if p.run != nil {
		p.run.manual.Store(true)
	}
	// 没有"本进程启动过"的实例了，运行中哈希随之作废（心跳据此区分"停止"与"跑着别的配置"）。
	p.runningHash = ""

	pid := p.pidFromFile()
	if pid <= 0 {
		_ = os.Remove(p.PidFile)
		return nil
	}
	// P1-2：归属校验——pid 文件陈旧或 pid 已被系统复用给无关进程时，绝不误杀，仅清理陈旧文件
	if !p.pidOwned(pid) {
		_ = os.Remove(p.PidFile)
		return nil
	}
	if err := killProcess(pid, syscall.SIGTERM); err != nil {
		_ = os.Remove(p.PidFile)
		return nil // 进程已不存在
	}
	deadline := time.Now().Add(stopGracePeriod)
	for time.Now().Before(deadline) {
		if !isProcessAlive(pid) {
			_ = os.Remove(p.PidFile)
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	_ = killProcess(pid, syscall.SIGKILL)
	_ = os.Remove(p.PidFile)
	return nil
}

// RestartWithConfig 写配置 → 校验 → 重启。新配置"spawn 成功但秒死"也算启动失败并回滚
// 到上一份配置（旧实现只看 spawn，坏配置会永久顶掉好配置、节点静默宕机）。
//
// 同一份内容在冷却期内被重复推送时直接拒绝、不碰运行中的进程：主控每 2 分钟补推一次待推送
// 配置，而每次应用都要先停掉正在服务的 xray 再回滚（回滚成功也要重启一次），会退化成
// "每 2 分钟断一次用户"。冷却过后自动放行重试，环境类故障修好即可自愈。
func (p *Proc) RestartWithConfig(configJSON string) error {
	hash := contentHash(configJSON)
	if err := p.rejectRepeat(hash); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(p.ConfigPath), 0o755); err != nil {
		return err
	}
	// U5（2026-08-14）：先 -test 后落盘——临时文件校验通过才原子替换，
	// 避免坏配置覆盖磁盘上的好配置（xray 崩溃后 watchdog 用坏配置反复拉起失败，节点永久宕机）。
	tmp := p.tmpConfigPath("apply")
	if err := os.WriteFile(tmp, []byte(configJSON), 0o644); err != nil {
		return fmt.Errorf("写入临时配置失败: %w", err)
	}
	if err := p.TestConfig(tmp); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	// 保留上一份好配置作回滚（覆盖前备份）
	if _, err := os.Stat(p.ConfigPath); err == nil {
		_ = os.Rename(p.ConfigPath, p.ConfigPath+".bak")
	}
	if err := os.Rename(tmp, p.ConfigPath); err != nil {
		return fmt.Errorf("替换配置失败: %w", err)
	}
	// 新配置给一次干净的失败预算（否则上次的放弃态会立刻压制它）
	p.ResetFailures()
	if err := p.Stop(); err != nil {
		return err
	}
	if err := p.Start(); err != nil {
		// 记下"这份内容起不来"：冷却期内主控重推同一份内容会被直接拒绝（不再反复停服）
		p.noteRejected(hash, err)
		// 启动失败：回滚到上一份好配置并再试一次
		if _, serr := os.Stat(p.ConfigPath + ".bak"); serr == nil {
			_ = os.Rename(p.ConfigPath+".bak", p.ConfigPath)
			_ = p.Stop()
			if rerr := p.Start(); rerr == nil {
				return fmt.Errorf("新配置无法启动（%v），已回滚到上一份配置", err)
			}
			return err
		}
		return err
	}
	// 新配置已生效：清理备份，并留一份"上一份可用配置"副本供开机回退
	_ = os.Remove(p.ConfigPath + ".bak")
	_ = copyFile(p.ConfigPath, p.ConfigPath+goodSuffix)
	p.clearRejected()
	return nil
}

// tmpConfigPath 配置的临时写入路径。必须以 .json 结尾：xray 配置格式纯按扩展名判定
// （core/config.go GetFormatByExtension 仅认 json/jsonc/yaml/yml/toml/pb，无内容嗅探），
// 旧命名 .tmp 会让 -test 直接报 "Failed to get format"（实机 pending 悬挂的真正根因）。
func (p *Proc) tmpConfigPath(tag string) string {
	return strings.TrimSuffix(p.ConfigPath, ".json") + "." + tag + ".json"
}

// WriteConfigIfValid 只把配置写进磁盘（临时文件 -test 通过后原子替换），**不重启 xray**。
//
// 用途：热更（gRPC AlterInbound）之后让磁盘与运行中的用户集保持一致（不变量 I2：磁盘 =
// 运行中配置的快照）。磁盘是"下次启动的输入"，热更本身不影响运行中的进程，所以这里
// 绝不能 Stop/Start——它只是把同一份用户集写下去，好让节点重启（自愈 / 开机 / 手动重启）
// 不会把用户集回退到上一次冷更。
//
// 与 RestartWithConfig 的两点刻意差异：
//  1. 不碰同内容冷却：内容冷却针对的是"整份配置应用失败"，用去拒绝热更会让封禁 / 到期
//     用户摘不掉（把一个小问题放大成 P1 同类的问题）。这里 -test 不过就直接报错回主控。
//  2. 不更新 .good：.good 的语义是"最后一次真正启动成功过的配置"，而这份内容还没被启动
//     验证过（没重启），顶掉它会让开机回退退到一份未经验证的内容。
func (p *Proc) WriteConfigIfValid(configJSON string) error {
	if configJSON == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(p.ConfigPath), 0o755); err != nil {
		return err
	}
	tmp := p.tmpConfigPath("hot")
	if err := os.WriteFile(tmp, []byte(configJSON), 0o644); err != nil {
		return fmt.Errorf("写入临时配置失败: %w", err)
	}
	if err := p.TestConfig(tmp); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, p.ConfigPath); err != nil {
		return fmt.Errorf("替换配置失败: %w", err)
	}
	return nil
}

// rejectRepeat 同内容冷却拒绝：这份配置内容刚应用失败过且仍在冷却期内，直接返回错误，
// 不写配置、不重启（当前运行的配置继续服务）。
// 只有真正尝试并失败才刷新冷却时刻（noteRejected），拒绝本身不刷新——否则主控的周期重推
// 会把冷却无限推后，环境修好后也永远等不到重试。
func (p *Proc) rejectRepeat(hash string) error {
	if hash == "" {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if hash != p.rejectedHash {
		return nil
	}
	left := time.Until(p.rejectedAt.Add(rejectCooldown))
	if left <= 0 {
		return nil
	}
	return fmt.Errorf("同一份配置在 %s 前应用失败（%s），%s 内不再重复应用（当前配置继续运行，冷却过后自动重试）",
		time.Since(p.rejectedAt).Truncate(time.Second), p.rejectedErr, left.Truncate(time.Second))
}

// noteRejected 记下「这份内容应用失败」并开始冷却（见 rejectRepeat）。
func (p *Proc) noteRejected(hash string, cause error) {
	if hash == "" {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.rejectedHash, p.rejectedAt = hash, time.Now()
	p.rejectedErr = truncateRunes(cause.Error(), maxErrText)
}

// clearRejected 清除同内容冷却（这份内容已成功应用）。
func (p *Proc) clearRejected() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.clearRejectedLocked()
}

// clearRejectedLocked 清除同内容冷却（调用方须持有 p.mu）。
func (p *Proc) clearRejectedLocked() {
	p.rejectedHash, p.rejectedErr = "", ""
	p.rejectedAt = time.Time{}
}

// IsRunning 通过 pid 文件 + isProcessAlive 判断进程存活；僵尸（Z）视为不在运行。
func (p *Proc) IsRunning() bool {
	pid := p.pidFromFile()
	if pid <= 0 {
		return false
	}
	if !isProcessAlive(pid) {
		return false
	}
	// 僵尸进程 kill(0) 仍成功，需检查 /proc/<pid>/stat 的 state
	if state, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat"); err == nil {
		// 形如 "12345 (xray) S ..."，state 是最后一个 ') ' 后的第一个字符
		rest := string(state)
		if i := strings.LastIndex(rest, ") "); i >= 0 && i+2 < len(rest) {
			return rest[i+2] != 'Z'
		}
	}
	return true
}

func (p *Proc) pidFromFile() int {
	data, err := os.ReadFile(p.PidFile)
	if err != nil {
		return 0
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil || pid <= 0 {
		return 0
	}
	return pid
}

// Watchdog 崩溃自动拉起（每 watchdogInterval 检测一次）。
// 连续启动失败按 backoffFor 退避，达 maxStartFailures 后停止自动拉起（不再永无止境地
// 拉起 xray），此后按 probeInterval 慢探底；状态与原因留给心跳上报主控。
// 只有真正拉起来的那次（含探底成功）才触发 OnRestart。
func (p *Proc) Watchdog(stop <-chan struct{}) {
	ticker := time.NewTicker(watchdogInterval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			p.check()
		}
	}
}

// check 一个看门狗周期：吸收退出信息 → 判定是否需要拉起（退避中不动手；已放弃则走慢探底）。
func (p *Proc) check() {
	p.dispatchGiveUpNotice() // 先投递上一拍遗留的告警
	p.mu.Lock()
	if !p.started {
		p.mu.Unlock()
		return
	}
	if r := p.run; r != nil {
		select {
		case <-r.exited:
			p.absorbExitLocked(r, false)
		default:
		}
	}
	running := p.IsRunning()
	gaveUp := p.gaveUp
	wait := time.Until(p.nextTryAt)
	p.mu.Unlock()

	if running {
		p.mu.Lock()
		if p.failures > 0 && !p.startedAt.IsZero() && time.Since(p.startedAt) >= startFailureGrace {
			// 进程已稳定运行超过启动宽限期：清零连续启动失败计数与 .good 回退标志
			p.failures = 0
			p.gaveUp = false
			p.goodTried = false
		}
		p.mu.Unlock()
		return
	}
	// 达上限且还没试过回退：把磁盘配置换回「上一份真正启动成功过的配置」（.good）再拉一次。
	// 这一步必须在 Start 之前（否则会先按坏配置白拉一轮），且只做一次。
	if p.takeGoodRollback() {
		if p.tryGoodRollback() {
			p.fireRestart()
			return
		}
		p.dispatchGiveUpNotice() // 回退也失败：立即告警，不等下一拍
		return
	}
	if wait > 0 {
		return
	}
	if gaveUp {
		// 慢探底：放弃自动拉起后仍按 probeInterval 试一次。失败不计数、不重复告警，
		// 环境类故障（开机期端口被占等）修好后无需人工介入即可自愈。
		if err := p.startProbe(); err != nil {
			log.Printf("xrayproc: xray 慢探底失败（保持停止自动拉起，%s 后再试）: %v", probeInterval, err)
			return
		}
	} else if err := p.Start(); err != nil {
		p.mu.Lock()
		failures, gaveUpNow := p.failures, p.gaveUp
		p.mu.Unlock()
		// 放弃那条日志由 noteStartFailureLocked 在状态跃迁时打，这里只报单次失败
		if !gaveUpNow {
			log.Printf("xrayproc: xray 拉起失败（连续 %d/%d 次）: %v", failures, maxStartFailures, err)
		}
		p.dispatchGiveUpNotice() // 本拍刚达上限：立即告警，不等下一拍
		return
	}
	// 真的拉起来了（普通拉起或慢探底成功）：触发崩溃自愈回调。
	// 探底成功时 startLocked 已清空 failures/gaveUp/nextTryAt，状态自然回到 running。
	p.fireRestart()
}

// fireRestart 投递「xray 被拉起来了」回调（锁外执行，回调会触发重连等外部动作）。
func (p *Proc) fireRestart() {
	p.mu.Lock()
	onRestart := p.OnRestart
	p.mu.Unlock()
	if onRestart != nil {
		go onRestart()
	}
}

// takeGoodRollback 取走「该做一次 .good 回退」的标志（锁内置位、锁外执行）。
func (p *Proc) takeGoodRollback() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	pending := p.goodRollbackPending
	p.goodRollbackPending = false
	return pending
}

// tryGoodRollback 放弃自动拉起之前的一次性尝试：把磁盘配置换回「上一份真正启动成功过的
// 配置」（.good，RestartWithConfig 成功时留的副本）再拉一次。
//
// 为什么需要：回滚逻辑原本只存在于 RestartWithConfig 的 Start 失败分支，而「跑了一阵才崩」
// （就绪窗口之外）与开机拉起根本走不到那里——那类故障里最可能的正是"这份配置本身有问题"，
// 而它不会自己恢复，只能靠这一次回退。只试一次（goodTried）：失败即转入放弃 + 报警，
// 不构成新的死循环；环境类故障（端口被占）回退也救不了，仍由慢探底等环境恢复。
func (p *Proc) tryGoodRollback() bool {
	good := p.ConfigPath + goodSuffix
	data, err := os.ReadFile(good)
	if err != nil {
		// 没有可回退的副本：这条路走不通，直接放弃（再从 1 数到 8 只是多挨 8 次）
		p.markGaveUp(p.failureReason())
		return false
	}
	if err := os.WriteFile(p.ConfigPath, data, 0o644); err != nil {
		log.Printf("xrayproc: 回退上一份可用配置失败: %v", err)
		p.markGaveUp(p.failureReason())
		return false
	}
	// 回退后给一次干净的失败预算：这份内容此前真的启动成功过，值得从零重试
	p.ResetFailures()
	p.markGoodTried()
	log.Printf("xrayproc: 连续启动失败达上限，已回退到上一份可用配置（%s）并重试", good)
	if err := p.Start(); err != nil {
		reason := fmt.Sprintf("回退到上一份可用配置后仍启动失败: %v", err)
		log.Printf("xrayproc: %s", reason)
		p.markGaveUp(reason)
		return false
	}
	return true
}

// markGoodTried 标记「本次机会已用过 .good 回退」（成功启动或新的机会会清零）。
func (p *Proc) markGoodTried() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.goodTried = true
}

// failureReason 最近一次失败原因（空则给一句兜底文案——告警不能是空串）。
func (p *Proc) failureReason() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.lastErr == "" {
		return "连续启动失败"
	}
	return p.lastErr
}

// markGaveUp 直接进入放弃态（用于"回退也失败"这种手段已用尽的情形）：从 1 数到 8 只会让
// 节点多挨 8 次无效拉起，这里直接置到上限并投递告警。
func (p *Proc) markGaveUp(reason string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.failures = maxStartFailures
	p.lastErr, p.lastErrAt = reason, time.Now()
	p.giveUpLocked(reason)
}

// dispatchGiveUpNotice 取走并投递放弃告警（回调在锁外执行，避免持锁跑外部代码）。
// 回调尚未就位时不取走通知，留给下一拍——先取后判会让告警被静默丢弃。
func (p *Proc) dispatchGiveUpNotice() {
	p.mu.Lock()
	onGiveUp := p.OnGiveUp
	var notice string
	if onGiveUp != nil {
		notice = p.takeGiveUpNoticeLocked()
	}
	p.mu.Unlock()
	if notice != "" {
		onGiveUp(notice)
	}
}

// absorbExitLocked 把某次运行的退出信息并入失败账（仅当进程已退出），返回本次退出的原因摘要。
// 调用方须持有 p.mu；同一 run 只吸收一次（重复调用返回空串）。
func (p *Proc) absorbExitLocked(r *run, probe bool) string {
	if r.absorbed {
		return ""
	}
	r.absorbed = true
	summary := formatExit(r.code, r.tail.String())
	if r.manual.Load() {
		return summary // 我们主动停的，不是故障
	}
	quick := r.at.Sub(p.startedAt) < startFailureGrace
	p.noteFailureLocked(summary, r.at, quick, probe)
	return summary
}

// noteFailureLocked 记一次启动失败：普通拉起走连续计数（达上限即放弃并告警），
// 慢探底只更新原因与下次探底时刻。
func (p *Proc) noteFailureLocked(reason string, at time.Time, quick, probe bool) {
	if probe {
		p.noteProbeFailureLocked(reason, at)
		return
	}
	p.noteStartFailureLocked(reason, at, quick)
}

// noteStartFailureLocked 记一次启动失败：连续计数、原因留档、达上限即放弃自动拉起
// （此后转入慢探底，见 check）。
// quick=false 表示进程跑了一阵才崩：单次算新故障（重新计 1 次），但同一滑动窗口内
// 累计到 crashWindowCount 次就不再当偶发——否则"每隔 60s+ 死一次"永远到不了上限。
func (p *Proc) noteStartFailureLocked(reason string, at time.Time, quick bool) {
	if quick {
		p.failures++
	} else {
		p.crashTimes = append(p.crashTimes, at)
		cut := at.Add(-crashWindow)
		kept := p.crashTimes[:0]
		for _, t := range p.crashTimes {
			if t.After(cut) {
				kept = append(kept, t)
			}
		}
		p.crashTimes = kept
		if len(p.crashTimes) >= crashWindowCount {
			p.failures++
		} else {
			p.failures = 1
		}
	}
	p.lastErr = reason
	p.lastErrAt = at
	if p.failures >= maxStartFailures {
		if !p.goodTried && !p.gaveUp {
			// 放弃之前先给"回退到上一份可用配置"一次机会（见 tryGoodRollback）：
			// 锁内置位，由 check() 在锁外执行（回退要写文件 + Start，不能在锁内做）。
			p.goodTried, p.goodRollbackPending = true, true
			p.nextTryAt = time.Time{} // 下一拍立即执行
			log.Printf("xrayproc: xray 连续 %d 次启动失败，先回退到上一份可用配置再试一次（最近原因: %s）",
				p.failures, reason)
			return
		}
		p.giveUpLocked(reason)
		return
	}
	p.nextTryAt = time.Now().Add(backoffFor(p.failures))
}

// giveUpLocked 进入放弃态：停自动拉起、留一条待投递的告警、转入慢探底（调用方须持有 p.mu）。
func (p *Proc) giveUpLocked(reason string) {
	if !p.gaveUp {
		p.gaveUp = true
		p.giveUpNotice = reason
		log.Printf("xrayproc: xray 连续 %d 次启动失败，已停止自动拉起（最近原因: %s）；"+
			"此后每 %s 慢探底一次，也可在面板「重启 Xray」或重新下发配置立即重试",
			p.failures, reason, probeInterval)
	}
	p.nextTryAt = time.Now().Add(probeInterval)
}

// noteProbeFailureLocked 记一次慢探底失败：只更新原因与下次探底时刻，不增连续失败计数、
// 不重复投递放弃告警——面板上的"连续失败 N 次"停在放弃那一刻的数值。
func (p *Proc) noteProbeFailureLocked(reason string, at time.Time) {
	p.lastErr = reason
	p.lastErrAt = at
	p.nextTryAt = time.Now().Add(probeInterval)
}

// takeGiveUpNoticeLocked 取走待投递的放弃告警（调用方须持有 p.mu）。
func (p *Proc) takeGiveUpNoticeLocked() string {
	n := p.giveUpNotice
	p.giveUpNotice = ""
	return n
}

// ResetFailures 清零连续失败、放弃态、窗口外死亡窗口与同内容冷却（面板「重启 Xray」/
// 新配置下发时调用，重新给机会——包括立刻重试此前被冷却拒绝的那份配置，这正是管理员
// 修好环境后的动作）。.good 回退机会一并复位：每次新机会都允许试一次回退。
func (p *Proc) ResetFailures() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.failures, p.gaveUp, p.nextTryAt = 0, false, time.Time{}
	p.crashTimes, p.goodTried = nil, false
	p.clearRejectedLocked()
}

// Health 返回 xray 健康快照（心跳上报主控）。
func (p *Proc) Health() Health {
	p.mu.Lock()
	defer p.mu.Unlock()
	h := Health{LastError: p.lastErr, ErrorAt: p.lastErrAt, Failures: p.failures, GaveUp: p.gaveUp}
	switch {
	case p.IsRunning():
		// 先看进程实况：pid 文件指向活着的 xray（含上一轮 agent 遗留、本轮未托管的实例）
		// 也应如实报 running，不能因为本进程没 spawn 过就说"未启动"。
		h.State = "running"
	case !p.started:
		h.State = "stopped"
	case p.gaveUp:
		h.State = "failed"
	default:
		h.State = "restarting"
	}
	return h
}

// EnsureUsableConfig 开机配置体检：配置缺失时写自举最小模板；-test 不通过时回退到
// 上一份可用配置副本（.good）。返回一句"发生了什么"的说明（空 = 无需说明）；
// 除"已写入自举最小配置"这条正常路径外，说明同时记入 Health.LastError，
// 让面板能看到"这次开机为什么不是当前配置在跑"。
func (p *Proc) EnsureUsableConfig() string {
	if _, err := os.Stat(p.ConfigPath); err != nil {
		written, werr := EnsureBootstrapConfig(p.ConfigPath)
		if werr != nil {
			note := fmt.Sprintf("写入自举最小配置失败（xray 暂不启动）: %v", werr)
			p.setNotice(note)
			return note
		}
		if written {
			return "已写入自举最小配置（主控尚未下发配置）"
		}
	}
	if err := p.TestConfig(p.ConfigPath); err == nil {
		return ""
	} else if _, serr := os.Stat(p.ConfigPath + goodSuffix); serr != nil {
		// 无回退可用：如实上报，交给看门狗退避重试（可能是环境问题，重启未必能修好）
		note := fmt.Sprintf("开机配置校验失败且无可回退副本: %v", err)
		p.setNotice(note)
		return note
	}
	data, rerr := os.ReadFile(p.ConfigPath + goodSuffix)
	if rerr != nil {
		note := fmt.Sprintf("读取上一份可用配置失败: %v", rerr)
		p.setNotice(note)
		return note
	}
	if werr := os.WriteFile(p.ConfigPath, data, 0o644); werr != nil {
		note := fmt.Sprintf("回退上一份可用配置失败: %v", werr)
		p.setNotice(note)
		return note
	}
	if terr := p.TestConfig(p.ConfigPath); terr != nil {
		note := fmt.Sprintf("回退的上一份可用配置也校验不过: %v", terr)
		p.setNotice(note)
		return note
	}
	note := "开机配置校验失败，已回退到上一份可用配置"
	p.setNotice(note)
	return note
}

// setNotice 把一条"配置层"的说明写进 Health.LastError（不计入连续失败计数）。
func (p *Proc) setNotice(note string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.lastErr = note
	p.lastErrAt = time.Now()
}

// copyFile 复制文件内容（同目录 rename 语义的简化版：目标先截断再写，失败不影响源）。
func copyFile(src, dst string) error {
	data, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	return os.WriteFile(dst, data, 0o644)
}

// Status 返回运行状态。与 Health() 同口径：pid 文件指向活着的 xray 即算运行
// （含上一轮 agent 遗留、本轮未托管的实例），否则同一帧心跳里 xray_running 与
// xray_state 会互相打架。未托管时 startedAt 为零值，运行时长报 0。
func (p *Proc) Status() (running bool, pid int, startedAt time.Time, uptimeSec int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.IsRunning() {
		return false, 0, time.Time{}, 0
	}
	var uptime int64
	if !p.startedAt.IsZero() {
		uptime = int64(time.Since(p.startedAt).Seconds())
	}
	return true, p.pidFromFile(), p.startedAt, uptime
}

// Hashes 返回磁盘配置与「本进程最后一次成功启动时」的配置内容哈希（心跳上报，主控据此
// 对账：running 与主控记的已生效内容不一致 = 节点跑的不是它以为的那份配置）。
// running 为空表示当前没有本进程启动过的实例（未托管 / 已停止）。
func (p *Proc) Hashes() (disk, running string) {
	if data, err := os.ReadFile(p.ConfigPath); err == nil {
		disk = contentHash(string(data))
	}
	p.mu.Lock()
	running = p.runningHash
	p.mu.Unlock()
	return disk, running
}

// Logs 读取最近 n 行日志。
func (p *Proc) Logs(n int) (string, error) {
	if n <= 0 {
		n = 100
	}
	data, err := os.ReadFile(p.LogPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", nil
		}
		return "", err
	}
	lines := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n"), nil
}
