//go:build linux

package xrayproc

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func writePidFile(t *testing.T, p *Proc, pid int) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(p.PidFile), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p.PidFile, []byte(strconv.Itoa(pid)), 0o644); err != nil {
		t.Fatal(err)
	}
}

func waitProcessGone(pid int, d time.Duration) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if !isProcessAlive(pid) {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return false
}

// TestStopKillsOwnedXray 伪造"我们的 xray"（argv 含配置路径），Stop 应能终止它并清理 pid 文件。
func TestStopKillsOwnedXray(t *testing.T) {
	cfgPath := filepath.Join(t.TempDir(), "xray.json")
	pidFile := filepath.Join(t.TempDir(), "xray.pid")
	p := New("/bin/sh", cfgPath, filepath.Join(t.TempDir(), "x.log"), pidFile)

	// sh 的 cmdline 里带上配置路径（# 注释末尾），使 pgrep -f 命中；TERM 时连带杀掉子进程
	script := "sleep 300 & C=$!; trap 'kill $C 2>/dev/null; exit 0' TERM; wait # " + cfgPath
	cmd := exec.Command("sh", "-c", script)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	writePidFile(t, p, cmd.Process.Pid)

	if err := p.Stop(); err != nil {
		t.Fatalf("Stop 报错: %v", err)
	}
	if !waitProcessGone(cmd.Process.Pid, 5*time.Second) {
		t.Fatal("Stop 未终止被归属的进程")
	}
	if _, err := os.Stat(pidFile); !os.IsNotExist(err) {
		t.Fatalf("pid 文件应被清理: %v", err)
	}
}

// TestStopRefusesUnrelatedPID pid 文件指向无关进程（cmdline 无配置路径）：
// Stop 不得误杀，仅清理陈旧 pid 文件（P1-2 核心断言）。
func TestStopRefusesUnrelatedPID(t *testing.T) {
	cfgPath := filepath.Join(t.TempDir(), "xray.json")
	pidFile := filepath.Join(t.TempDir(), "xray.pid")
	p := New("/bin/sh", cfgPath, filepath.Join(t.TempDir(), "x.log"), pidFile)

	cmd := exec.Command("sleep", "300")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill() }()
	writePidFile(t, p, cmd.Process.Pid)

	if err := p.Stop(); err != nil {
		t.Fatalf("Stop 报错: %v", err)
	}
	if !isProcessAlive(cmd.Process.Pid) {
		t.Fatal("归属校验失败：无关进程被误杀")
	}
	if _, err := os.Stat(pidFile); !os.IsNotExist(err) {
		t.Fatalf("陈旧 pid 文件应被清理: %v", err)
	}
}
