package xrayproc

// 回归用例（2026-09-21）：失败路径闭环与热更落盘。
// 背景（docs/architecture/配置下发-热更冷更与状态机.md §4.2 缺口 1/2）：
//   - 回滚逻辑原本只在 RestartWithConfig 的 Start 失败分支，而"跑了一阵才崩"走不到那里；
//   - 窗口外死亡每次把连续失败重置为 1，于是"每隔 60s+ 死一次"永远到不了上限、永不告警。

import (
	"os"
	"runtime"
	"strings"
	"testing"
	"time"
)

// TestGoodRollbackBeforeGiveUp 达上限后先回退 .good 再决定是否放弃：
// 磁盘上这份起不来（DIE），.good 是上一份真正启动成功过的配置 → 必须换回 .good 拉起来，
// 不投递放弃告警，并触发 OnStarted（agent 据此按回退后的配置重建用户缓存）。
func TestGoodRollbackBeforeGiveUp(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows 的 isProcessAlive 是桩（FindProcess 恒成功），看门狗无法判定进程已死")
	}
	shortTiming(t)
	p := fakeProc(t, `{"inbounds":[{"port":443,"tag":"DIE"}]}`)
	good := `{"inbounds":[{"port":8443}]}`
	if err := os.WriteFile(p.ConfigPath+goodSuffix, []byte(good), 0o644); err != nil {
		t.Fatal(err)
	}
	alarms, started := 0, 0
	p.OnGiveUp = func(string) { alarms++ }
	p.OnStarted = func(configPath string) {
		started++
		if got := readFile(t, configPath); got != good {
			t.Errorf("OnStarted 应指向回退后的配置，实际: %s", got)
		}
	}

	if err := p.Start(); err == nil {
		t.Fatal("DIE 配置应报错（连续失败计数的起点）")
	}
	waitFor(t, "回退到 .good 并进入运行态", func() bool {
		p.check()
		return p.Health().State == "running"
	})
	if got := readFile(t, p.ConfigPath); got != good {
		t.Fatalf("磁盘应换成 .good 的内容，实际: %s", got)
	}
	if alarms != 0 {
		t.Fatalf("回退成功不应投递放弃告警，实际 %d", alarms)
	}
	if h := p.Health(); h.GaveUp || h.Failures != 0 {
		t.Fatalf("回退成功后应清空失败态，实际 gaveUp=%v failures=%d", h.GaveUp, h.Failures)
	}
	if started == 0 {
		t.Fatal("回退拉起必须触发 OnStarted（否则缓存与运行中的用户集脱钩）")
	}
}

// TestGoodRollbackFailsThenGivesUp 回退也起不来时直接进入放弃态 + 告警：
// 不再从 1 数到 8（那只是让节点多挨 8 次无效拉起）。
func TestGoodRollbackFailsThenGivesUp(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows 的 isProcessAlive 是桩，看门狗无法判定进程已死")
	}
	shortTiming(t)
	p := fakeProc(t, `{"inbounds":[{"port":443,"tag":"DIE"}]}`)
	// .good 也是起不来的内容（环境类故障：端口被占，回退救不了）
	if err := os.WriteFile(p.ConfigPath+goodSuffix, []byte(`{"inbounds":[{"port":443,"tag":"DIE"}]}`), 0o644); err != nil {
		t.Fatal(err)
	}
	alarms := 0
	p.OnGiveUp = func(reason string) {
		alarms++
		if reason == "" {
			t.Error("放弃告警不得为空")
		}
	}

	if err := p.Start(); err == nil {
		t.Fatal("DIE 配置应报错")
	}
	waitFor(t, "进入放弃态", func() bool {
		p.check()
		return p.Health().GaveUp
	})
	if alarms != 1 {
		t.Fatalf("放弃时应投递一次告警，实际 %d", alarms)
	}
	if h := p.Health(); h.Failures != maxStartFailures {
		t.Fatalf("回退失败后失败计数应置到上限 %d，实际 %d", maxStartFailures, h.Failures)
	}
}

// TestOutOfWindowCrashWindowCounts 窗口外死亡（跑了一阵才崩）在滑动窗口内累计到阈值即计入
// 连续失败——旧实现每次都重置为 1，于是"每隔 60s+ 死一次"永远到不了上限、永不告警。
func TestOutOfWindowCrashWindowCounts(t *testing.T) {
	p := New("/bin/false", t.TempDir()+"/config.json", t.TempDir()+"/x.log", t.TempDir()+"/x.pid")
	now := time.Now()

	// 第 1、2 次窗口外死亡：仍按"新故障"从 1 计起
	p.noteStartFailureLocked("crash-1", now, false)
	if p.failures != 1 {
		t.Fatalf("首次窗口外死亡应从 1 计起，实际 %d", p.failures)
	}
	p.noteStartFailureLocked("crash-2", now.Add(1*time.Minute), false)
	if p.failures != 1 {
		t.Fatalf("窗口内第 2 次仍按新故障计 1，实际 %d", p.failures)
	}
	// 第 3 次落在同一窗口内 → 视为故障，开始累加
	p.noteStartFailureLocked("crash-3", now.Add(2*time.Minute), false)
	if p.failures != 2 {
		t.Fatalf("窗口内第 3 次应开始累加，实际 %d", p.failures)
	}
	// 持续崩溃最终应触发上限（旧实现永远停在 1）
	for i := 3; i < 10 && p.failures < maxStartFailures; i++ {
		p.noteStartFailureLocked("crash", now.Add(time.Duration(i)*time.Minute), false)
	}
	if p.failures < maxStartFailures {
		t.Fatalf("持续窗口外崩溃应达上限 %d，实际 %d", maxStartFailures, p.failures)
	}
}

// TestOutOfWindowCrashWindowExpires 窗口外的旧死亡不参与累计：隔了很久才崩一次仍是偶发。
func TestOutOfWindowCrashWindowExpires(t *testing.T) {
	p := New("/bin/false", t.TempDir()+"/config.json", t.TempDir()+"/x.log", t.TempDir()+"/x.pid")
	now := time.Now()
	p.noteStartFailureLocked("crash-1", now, false)
	p.noteStartFailureLocked("crash-2", now.Add(1*time.Minute), false)
	// 距前两次已超出窗口 → 窗口内只剩本次，回到"新故障计 1"
	p.noteStartFailureLocked("crash-3", now.Add(crashWindow+time.Minute), false)
	if p.failures != 1 {
		t.Fatalf("窗口外的历史崩溃不应累计，实际 failures=%d", p.failures)
	}
}

// TestWriteConfigIfValidDoesNotRestart 热更落盘：只写盘、不重启、不动 .good；
// -test 不过的内容一个字节都不许落盘。
func TestWriteConfigIfValidDoesNotRestart(t *testing.T) {
	shortTiming(t)
	p := fakeProc(t, `{"inbounds":[{"port":8443}]}`)
	if err := p.Start(); err != nil {
		t.Fatalf("好配置应能启动: %v", err)
	}
	_, pidBefore, _, _ := p.Status()

	hot := `{"inbounds":[{"port":8443}],"note":"hot-sync"}`
	if err := p.WriteConfigIfValid(hot); err != nil {
		t.Fatalf("落盘应成功: %v", err)
	}
	if got := readFile(t, p.ConfigPath); got != hot {
		t.Fatalf("磁盘应换成新内容，实际: %s", got)
	}
	if _, pidAfter, _, _ := p.Status(); pidAfter != pidBefore {
		t.Fatalf("热更落盘不得重启 xray：pid %d → %d", pidBefore, pidAfter)
	}
	if _, err := os.Stat(p.ConfigPath + goodSuffix); err == nil {
		t.Fatal(".good 不得被热更落盘覆盖（那份内容还没被启动验证过）")
	}

	if err := p.WriteConfigIfValid(`{"BADTEST":true}`); err == nil {
		t.Fatal("坏配置应报错")
	}
	if got := readFile(t, p.ConfigPath); got != hot {
		t.Fatalf("校验失败不得动磁盘，实际: %s", got)
	}
	if _, err := os.Stat(p.tmpConfigPath("hot")); err == nil {
		t.Fatal("校验失败的临时文件应被删除")
	}
	if err := p.WriteConfigIfValid(""); err != nil {
		t.Fatalf("空内容应视为无操作: %v", err)
	}
}

// TestHashesDistinguishDiskAndRunning 双哈希：热更落盘后磁盘哈希变、运行中哈希不变
// （xray 只在启动时读一次配置，磁盘改动对跑着的进程零影响），主控据此对账。
func TestHashesDistinguishDiskAndRunning(t *testing.T) {
	shortTiming(t)
	p := fakeProc(t, `{"inbounds":[{"port":8443}]}`)
	if err := p.Start(); err != nil {
		t.Fatalf("好配置应能启动: %v", err)
	}
	disk0, running0 := p.Hashes()
	if disk0 == "" || disk0 != running0 {
		t.Fatalf("启动后磁盘与运行中应同源，实际 disk=%q running=%q", disk0, running0)
	}
	if err := p.WriteConfigIfValid(`{"inbounds":[{"port":8443}],"note":"hot"}`); err != nil {
		t.Fatal(err)
	}
	disk1, running1 := p.Hashes()
	if disk1 == disk0 {
		t.Fatal("热更落盘后磁盘哈希应变化")
	}
	if running1 != running0 {
		t.Fatalf("热更落盘不得改运行中哈希（没重启），实际 %q → %q", running0, running1)
	}
	if !strings.Contains(p.ConfigPath, "config.json") {
		t.Fatalf("夹具配置路径异常: %s", p.ConfigPath)
	}
}

// TestOutOfWindowCrashTimesSurvivesStartLocked 进程启动并活过 readyWindow 不得清空 crashTimes：
// 否则"每隔 60s+ 死一次"在每次拉起 800ms 后都被清空，滑动窗口永远无法累加到阈值。
func TestOutOfWindowCrashTimesSurvivesStartLocked(t *testing.T) {
	shortTiming(t)
	p := fakeProc(t, `{"inbounds":[{"port":8443}]}`)
	now := time.Now()
	// 模拟此前已有 2 次窗口外死亡记录
	p.noteStartFailureLocked("crash-1", now, false)
	p.noteStartFailureLocked("crash-2", now.Add(1*time.Minute), false)
	if len(p.crashTimes) != 2 {
		t.Fatalf("应有 2 次崩溃记录，实际 %d", len(p.crashTimes))
	}
	// 启动进程，活过 readyWindow
	if err := p.Start(); err != nil {
		t.Fatalf("启动应成功: %v", err)
	}
	// 关键验证：Start 成功后，crashTimes 绝对不能被清空！
	if len(p.crashTimes) != 2 {
		t.Fatalf("Start 成功后 crashTimes 不得被抹掉（破坏滑动窗口），实际 %d", len(p.crashTimes))
	}
}

