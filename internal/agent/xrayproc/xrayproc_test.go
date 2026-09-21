package xrayproc

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// 本包用「测试二进制本身冒充 xray-core」验证启动语义，跨平台可跑（CI 与 Windows 开发机）。
// 真实 xray 的关键行为由配置内容与参数复刻：
//   - `-test -config <cfg>`：配置含 BADTEST 时按配置错退出 23，否则打印 Configuration OK. 退出 0；
//   - `run -c <cfg>`：配置含 DIE 时按「端口被占」的真实报错退出 255，否则常驻不退出。
//
// 之所以要这套夹具：2026-09-21 实机事故（caddy 占 443）暴露的正是"spawn 成功 ≠ 起来了"，
// 单测必须能稳定复现"exec 成功但秒死"，而真 xray 需要真的占用端口才能复现。
func TestMain(m *testing.M) {
	if os.Getenv("XRAYPROC_FAKE") == "1" && len(os.Args) > 1 {
		switch os.Args[1] {
		case "-test":
			os.Exit(fakeTest(fakeConfigPath(os.Args[2:])))
		case "run":
			fakeRun(fakeConfigPath(os.Args[2:]))
		}
	}
	os.Exit(m.Run())
}

// fakeConfigPath 取 -c/-config 之后的配置路径。
func fakeConfigPath(args []string) string {
	for i, a := range args {
		if (a == "-c" || a == "-config") && i+1 < len(args) {
			return args[i+1]
		}
	}
	return ""
}

func fakeReadConfig(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return string(data)
}

func fakeTest(path string) int {
	if strings.Contains(fakeReadConfig(path), "BADTEST") {
		fmt.Fprintln(os.Stderr, "Failed to start: main: failed to load config files: ["+path+"] > infra/conf: invalid field rule")
		return 23
	}
	fmt.Println("Configuration OK.")
	return 0
}

// fakeRun 常驻或按端口冲突的真实形态秒死（exit 255，与官方 xray 实测一致）。
func fakeRun(path string) {
	// 登记自身 pid：常驻冒充进程在 Windows 上杀不掉（Stop 依赖 pgrep/proc 归属校验），
	// 用例靠这份清单兜底清理，避免残留进程占住测试二进制。
	if reg := os.Getenv("XRAYPROC_FAKE_PIDS"); reg != "" {
		if f, err := os.OpenFile(reg, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644); err == nil {
			fmt.Fprintf(f, "%d\n", os.Getpid())
			_ = f.Close()
		}
	}
	if !strings.Contains(fakeReadConfig(path), "DIE") {
		time.Sleep(60 * time.Second) // 冒充"活着"：由用例显式杀掉
		os.Exit(0)
	}
	fmt.Fprintln(os.Stderr, "Failed to start: app/proxyman/inbound: failed to listen TCP on 443 > "+
		"transport/internet: failed to listen on address: 0.0.0.0:443 > transport/internet/tcp: "+
		"failed to listen TCP on 0.0.0.0:443 > listen tcp 0.0.0.0:443: bind: address already in use")
	os.Exit(255)
}

// fakeProc 构造一个 Bin=测试二进制 的托管器（配置内容决定行为）。
func fakeProc(t *testing.T, configContent string) *Proc {
	t.Helper()
	dir := t.TempDir()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cfgPath := filepath.Join(dir, "config.json")
	if err := os.WriteFile(cfgPath, []byte(configContent), 0o644); err != nil {
		t.Fatal(err)
	}
	regPath := filepath.Join(dir, "fakes.pids")
	t.Setenv("XRAYPROC_FAKE", "1")
	t.Setenv("XRAYPROC_FAKE_PIDS", regPath)
	p := New(exe, cfgPath, filepath.Join(dir, "xray.log"), filepath.Join(dir, "xray.pid"))
	t.Cleanup(func() { killFake(t, p, regPath) })
	return p
}

// killFake 清理冒充进程：优先按 pid 文件，再按登记清单兜底
// （不能依赖 Stop：Windows 上 pgrep 缺失、归属校验恒 false，Stop 只删 pid 文件不杀进程）。
func killFake(t *testing.T, p *Proc, regPath string) {
	t.Helper()
	pids := map[int]bool{}
	if pid := p.pidFromFile(); pid > 0 {
		pids[pid] = true
	}
	if data, err := os.ReadFile(regPath); err == nil {
		for _, line := range strings.Fields(string(data)) {
			if pid, aerr := strconv.Atoi(line); aerr == nil && pid > 0 {
				pids[pid] = true
			}
		}
	}
	for pid := range pids {
		if proc, err := os.FindProcess(pid); err == nil {
			_ = proc.Kill()
		}
	}
}

// shortTiming 把时序参数压到毫秒级，避免用例等真实退避（用例结束恢复）。
func shortTiming(t *testing.T) {
	t.Helper()
	ow, orb, orm := watchdogInterval, readyWindow, backoffMax
	opi, orc := probeInterval, rejectCooldown
	watchdogInterval, readyWindow, backoffMax = 20*time.Millisecond, 60*time.Millisecond, 5*time.Millisecond
	probeInterval, rejectCooldown = 5*time.Millisecond, 5*time.Millisecond
	t.Cleanup(func() {
		watchdogInterval, readyWindow, backoffMax = ow, orb, orm
		probeInterval, rejectCooldown = opi, orc
	})
}

// TestStartReportsImmediateExit 秒死的 xray 不得被判为"已启动"：Start 必须报错，
// 且错误里带着退出码与 xray 自己的 stderr（面板据此说明"为什么没起来"）。
func TestStartReportsImmediateExit(t *testing.T) {
	shortTiming(t)
	p := fakeProc(t, `{"inbounds":[{"port":443,"tag":"DIE"}]}`)

	err := p.Start()
	if err == nil {
		t.Fatal("进程 exec 后立刻退出，Start 不应返回成功")
	}
	if !strings.Contains(err.Error(), "255") || !strings.Contains(err.Error(), "bind:") {
		t.Fatalf("错误应含退出码与 xray 原始报错，实际: %v", err)
	}

	h := p.Health()
	if h.Failures != 1 {
		t.Fatalf("连续失败次数应为 1，实际 %d", h.Failures)
	}
	if !strings.Contains(h.LastError, "Failed to start") {
		t.Fatalf("LastError 应含 xray 报错原文，实际: %q", h.LastError)
	}
	if h.ErrorAt.IsZero() {
		t.Fatal("LastError 应带观测时刻")
	}
	if runtime.GOOS != "windows" { // Windows 的 isProcessAlive 是桩，状态判定只在 Linux 有意义
		if h.State != "restarting" {
			t.Fatalf("未达上限时状态应为 restarting，实际 %q", h.State)
		}
	}
}

// TestStartHealthyClearsFailures 正常起来的进程：Start 返回 nil、状态 running、失败计数归零。
func TestStartHealthyClearsFailures(t *testing.T) {
	shortTiming(t)
	p := fakeProc(t, `{"inbounds":[{"port":443}]}`)
	p.failures = 3 // 模拟此前有失败

	if err := p.Start(); err != nil {
		t.Fatalf("常驻进程应启动成功: %v", err)
	}
	h := p.Health()
	if h.Failures != 0 {
		t.Fatalf("启动成功后失败计数应归零，实际 %d", h.Failures)
	}
	if runtime.GOOS != "windows" && h.State != "running" {
		t.Fatalf("状态应为 running，实际 %q", h.State)
	}
}

// TestWatchdogGivesUpAfterMaxFailures 连续拉起失败达上限后必须停手（不再永无止境地拉起），
// 并投递一次告警回调；只有真正拉起来才触发 OnRestart。
func TestWatchdogGivesUpAfterMaxFailures(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows 的 isProcessAlive 是桩（FindProcess 恒成功），看门狗无法判定进程已死")
	}
	shortTiming(t)
	p := fakeProc(t, `{"inbounds":[{"port":443,"tag":"DIE"}]}`)
	restarts, alarms := 0, 0
	p.OnRestart = func() { restarts++ }
	p.OnGiveUp = func(reason string) {
		alarms++
		if !strings.Contains(reason, "bind:") {
			t.Errorf("告警应带失败原因，实际: %q", reason)
		}
	}

	if err := p.Start(); err == nil {
		t.Fatal("首次 Start 应因秒死而报错（这是后续连续失败计数的起点）")
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		p.check()
		p.mu.Lock()
		done := p.gaveUp
		p.mu.Unlock()
		if done {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	h := p.Health()
	if !h.GaveUp || h.State != "failed" {
		t.Fatalf("达上限后应放弃自动拉起，实际 state=%q gaveUp=%v failures=%d", h.State, h.GaveUp, h.Failures)
	}
	if h.Failures < maxStartFailures {
		t.Fatalf("失败计数应达上限 %d，实际 %d", maxStartFailures, h.Failures)
	}
	if alarms != 1 {
		t.Fatalf("放弃时应投递一次告警，实际 %d", alarms)
	}
	if restarts != 0 {
		t.Fatalf("从未成功拉起，不应触发 OnRestart（旧实现会把本地故障放大成主控连接风暴），实际 %d", restarts)
	}

	// 停手断言：继续巡检不得再尝试拉起（失败计数不再增长）
	before := h.Failures
	for i := 0; i < 20; i++ {
		p.check()
		time.Sleep(2 * time.Millisecond)
	}
	if after := p.Health().Failures; after != before {
		t.Fatalf("放弃后仍在尝试拉起：失败计数 %d → %d", before, after)
	}

	// 面板「重启 Xray」/ 新配置下发会重置预算，重新获得机会
	p.ResetFailures()
	if hh := p.Health(); hh.GaveUp || hh.Failures != 0 {
		t.Fatalf("ResetFailures 后应清空放弃态，实际 gaveUp=%v failures=%d", hh.GaveUp, hh.Failures)
	}
}

// TestRestartWithConfigRollsBackOnDeadStart 新配置"过 -test 但起不来"必须回滚到上一份配置：
// 旧实现只认 spawn 成功，坏配置会永久顶掉好配置并让节点静默宕机。
func TestRestartWithConfigRollsBackOnDeadStart(t *testing.T) {
	shortTiming(t)
	good := `{"inbounds":[{"port":8443}]}`
	p := fakeProc(t, good)
	if err := p.Start(); err != nil {
		t.Fatalf("好配置应能启动: %v", err)
	}

	err := p.RestartWithConfig(`{"inbounds":[{"port":443,"tag":"DIE"}]}`)
	if err == nil {
		t.Fatal("起不来的新配置应报错")
	}
	if !strings.Contains(err.Error(), "已回滚") {
		t.Fatalf("错误应说明已回滚，实际: %v", err)
	}
	onDisk, rerr := os.ReadFile(p.ConfigPath)
	if rerr != nil {
		t.Fatal(rerr)
	}
	if string(onDisk) != good {
		t.Fatalf("磁盘配置应回滚到上一份，实际: %s", string(onDisk))
	}
	if runtime.GOOS != "windows" && p.Health().State != "running" {
		t.Fatalf("回滚后应处于运行态，实际 %q", p.Health().State)
	}
}

// TestEnsureUsableConfigFallsBackToGood 开机体检：当前配置 -test 不过时回退 .good，
// 并把原因写进 Health（面板能看到"这次开机为什么不是当前配置在跑"）。
func TestEnsureUsableConfigFallsBackToGood(t *testing.T) {
	shortTiming(t)
	good := `{"inbounds":[{"port":8443}]}`
	p := fakeProc(t, `{"inbounds":[{"port":8443}],"BADTEST":true}`)
	if err := os.WriteFile(p.ConfigPath+goodSuffix, []byte(good), 0o644); err != nil {
		t.Fatal(err)
	}

	note := p.EnsureUsableConfig()
	if note == "" {
		t.Fatal("配置校验失败应给出说明")
	}
	if !strings.Contains(note, "回退") {
		t.Fatalf("说明应提到回退，实际: %q", note)
	}
	onDisk, rerr := os.ReadFile(p.ConfigPath)
	if rerr != nil {
		t.Fatal(rerr)
	}
	if string(onDisk) != good {
		t.Fatalf("应落盘回退后的配置，实际: %s", string(onDisk))
	}
	if h := p.Health(); !strings.Contains(h.LastError, "回退") {
		t.Fatalf("回退原因应记入 Health.LastError，实际: %q", h.LastError)
	}
}

// TestRestartWithConfigRejectsRepeatDuringCooldown 同一份"过 -test 但起不来"的配置在冷却期内
// 被重复推送（主控每 2 分钟补推一次待推送配置）时必须直接拒绝，且不得碰正在运行的 xray——
// 否则会退化成"每 2 分钟停一次服、回滚、再起一次"。
func TestRestartWithConfigRejectsRepeatDuringCooldown(t *testing.T) {
	shortTiming(t)
	rejectCooldown = time.Hour // 本用例要的就是"冷却期内"分支
	t.Cleanup(func() { rejectCooldown = 5 * time.Millisecond })

	good := `{"inbounds":[{"port":8443}]}`
	bad := `{"inbounds":[{"port":443,"tag":"DIE"}]}`
	p := fakeProc(t, good)
	if err := p.Start(); err != nil {
		t.Fatalf("好配置应能启动: %v", err)
	}

	if err := p.RestartWithConfig(bad); err == nil || !strings.Contains(err.Error(), "已回滚") {
		t.Fatalf("首次应用坏配置应报错并回滚，实际: %v", err)
	}
	pidBefore, cfgBefore := p.pidFromFile(), readFile(t, p.ConfigPath)
	if pidBefore <= 0 {
		t.Fatal("回滚后应有运行中的进程")
	}

	err := p.RestartWithConfig(bad)
	if err == nil {
		t.Fatal("冷却期内重推同一份配置应被拒绝")
	}
	if !strings.Contains(err.Error(), "冷却") {
		t.Fatalf("拒绝原因应说明冷却，实际: %v", err)
	}
	if got := p.pidFromFile(); got != pidBefore {
		t.Fatalf("拒绝时不得重启进程：pid %d → %d", pidBefore, got)
	}
	if got := readFile(t, p.ConfigPath); got != cfgBefore {
		t.Fatalf("拒绝时不得改写磁盘配置，实际: %s", got)
	}
	if runtime.GOOS != "windows" && !p.IsRunning() {
		t.Fatal("拒绝时当前配置应继续运行")
	}
}

// TestRestartWithConfigRetriesAfterCooldown 冷却过期后同一份内容必须重新放行（真的再试一次），
// 否则环境修好后永远等不到重试；应用成功后冷却态清零。
func TestRestartWithConfigRetriesAfterCooldown(t *testing.T) {
	shortTiming(t) // rejectCooldown 压到 5ms

	good := `{"inbounds":[{"port":8443}]}`
	bad := `{"inbounds":[{"port":443,"tag":"DIE"}]}`
	p := fakeProc(t, good)
	if err := p.Start(); err != nil {
		t.Fatalf("好配置应能启动: %v", err)
	}
	if err := p.RestartWithConfig(bad); err == nil {
		t.Fatal("首次应用坏配置应失败")
	}
	time.Sleep(20 * time.Millisecond) // 等冷却过期

	if err := p.RestartWithConfig(bad); err == nil || strings.Contains(err.Error(), "冷却") {
		t.Fatalf("冷却过期后应重新放行（错误应为启动失败而非冷却拒绝），实际: %v", err)
	}
	// 换一份能起来的配置：成功后冷却态应清零
	if err := p.RestartWithConfig(`{"inbounds":[{"port":9443}]}`); err != nil {
		t.Fatalf("好配置应能应用: %v", err)
	}
	if p.rejectedHash != "" {
		t.Fatalf("应用成功后应清除冷却态，实际残留 %q", p.rejectedHash)
	}
}

// TestWatchdogProbesAfterGiveUp 放弃自动拉起后仍按 probeInterval 慢探底：探底失败不计数、
// 不重复告警（面板数字停在放弃那一刻）；环境修好后自动恢复运行并触发一次 OnRestart。
func TestWatchdogProbesAfterGiveUp(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows 的 isProcessAlive 是桩（FindProcess 恒成功），看门狗无法判定进程已死")
	}
	shortTiming(t)
	p := fakeProc(t, `{"inbounds":[{"port":443,"tag":"DIE"}]}`)
	var restarts atomic.Int32
	alarms := 0
	p.OnRestart = func() { restarts.Add(1) }
	p.OnGiveUp = func(string) { alarms++ }

	if err := p.Start(); err == nil {
		t.Fatal("首次 Start 应因秒死而报错")
	}
	// check() 是看门狗的巡检入口：用例手动推进（不起真实 ticker），条件闭包里驱动
	waitFor(t, "进入放弃态", func() bool {
		p.check()
		return p.Health().GaveUp
	})

	// 探底持续失败：计数与告警都不得再动
	before := p.Health().Failures
	time.Sleep(5 * probeInterval)
	for i := 0; i < 5; i++ {
		p.check()
		time.Sleep(5 * time.Millisecond)
	}
	h := p.Health()
	if h.Failures != before {
		t.Fatalf("探底失败不得增长连续失败计数：%d → %d", before, h.Failures)
	}
	if alarms != 1 {
		t.Fatalf("探底失败不得重复投递放弃告警，实际 %d", alarms)
	}
	if !h.GaveUp || h.State != "failed" {
		t.Fatalf("探底失败应保持放弃态，实际 state=%q gaveUp=%v", h.State, h.GaveUp)
	}

	// 环境修好（同一路径下的配置能起来了）→ 下一次探底自动恢复
	if err := os.WriteFile(p.ConfigPath, []byte(`{"inbounds":[{"port":8443}]}`), 0o644); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "探底成功后解除放弃态", func() bool {
		p.check()
		return !p.Health().GaveUp
	})
	h = p.Health()
	if h.Failures != 0 {
		t.Fatalf("探底成功后失败计数应归零，实际 %d", h.Failures)
	}
	if h.State != "running" {
		t.Fatalf("探底成功后状态应为 running，实际 %q", h.State)
	}
	if alarms != 1 {
		t.Fatalf("恢复不应再投递放弃告警，实际 %d", alarms)
	}
	waitFor(t, "探底成功触发一次 OnRestart", func() bool { return restarts.Load() == 1 })
}

// TestStatusMatchesHealthForUnmanagedInstance 未托管实例（pid 文件指向活着的 xray、但本轮
// agent 没 spawn 过它）上 Status 与 Health 必须同口径报"运行中"；此时没有启动时刻，
// 运行时长必须报 0 而不是零值时间的巨大秒数。
func TestStatusMatchesHealthForUnmanagedInstance(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows 的 isProcessAlive 是桩，状态判定只在 Linux 有意义")
	}
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.json")
	if err := os.WriteFile(cfgPath, []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}
	pidFile := filepath.Join(dir, "xray.pid")
	// 用测试进程自身冒充"上一轮 agent 遗留、本轮未托管"的 xray
	if err := os.WriteFile(pidFile, []byte(strconv.Itoa(os.Getpid())), 0o644); err != nil {
		t.Fatal(err)
	}
	p := New("/bin/sh", cfgPath, filepath.Join(dir, "x.log"), pidFile)

	running, pid, startedAt, uptime := p.Status()
	if !running || pid != os.Getpid() {
		t.Fatalf("未托管但活着的实例应报运行中: running=%v pid=%d", running, pid)
	}
	if !startedAt.IsZero() || uptime != 0 {
		t.Fatalf("未托管实例无启动时刻，应报零值与 0 时长，实际 %v / %d 秒", startedAt, uptime)
	}
	if h := p.Health(); h.State != "running" {
		t.Fatalf("Health 应与 Status 同口径报 running，实际 %q", h.State)
	}
}

// waitFor 轮询等待条件成立（看门狗/探底是异步推进的），超时即判失败。
// 条件闭包内可先推进 p.check()——看门狗巡检在用例里是手动驱动的。
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("等待超时: %s", what)
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

// TestOwnedPIDsFindsConfigProcess 归属校验：cmdline 含配置路径的进程应被认领
// （Stop 才不会退化成"只删 pid 文件、不杀进程"）。
func TestOwnedPIDsFindsConfigProcess(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows 无 pgrep/无 /proc，归属校验依赖 Linux")
	}
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.json")
	if err := os.WriteFile(cfgPath, []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}
	p := New("/bin/sh", cfgPath, filepath.Join(dir, "x.log"), filepath.Join(dir, "x.pid"))

	// sh 的 cmdline 里带上配置路径（# 注释末尾），使 pgrep -f / /proc 扫描命中
	cmd := exec.Command("sh", "-c", "sleep 60 # "+cfgPath)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill() }()

	if !p.pidOwned(cmd.Process.Pid) {
		t.Fatal("cmdline 含配置路径的进程应被认领")
	}
	if p.pidOwned(os.Getpid()) {
		t.Fatal("自身不应被认领")
	}
}
