package client

// 回归用例（2026-11-15）：热更失败不能吞掉「顺带落盘」的整份配置。
//
// 事故现场：节点迁移到新服务器后，agent 跑的是自举最小配置，主控在磁盘偏离时通过
// sync_users 顺带下发整份配置。旧实现先做 gRPC 热更，热更因 handler not found 失败就
// 整条 return——后面的 WriteConfigIfValid 永远走不到，磁盘偏离无法收敛。
//
// 本用例用「无人监听的 xray API」稳定复现热更失败，并断言配置仍然落盘（-test 通过）。

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/acdc-awa/xpanel-node/internal/agent/stats"
	"github.com/acdc-awa/xpanel-node/internal/agent/xrayproc"
	"github.com/acdc-awa/xpanel-node/pkg/protocol"
)

// TestMain 冒充 xray 二进制：被 xrayproc.TestConfig 以 `-test -config <path>` 调起时
// 直接退出 0（等价 -test 通过）。只在子进程继承了 CLIENT_FAKE_XRAY=1 时生效。
func TestMain(m *testing.M) {
	if os.Getenv("CLIENT_FAKE_XRAY") == "1" {
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func TestSyncUsersWritesConfigWhenHotSyncFails(t *testing.T) {
	dir := t.TempDir()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("CLIENT_FAKE_XRAY", "1") // 子进程（冒充 xray）据此直接退出 0
	cfgPath := filepath.Join(dir, "config.json")
	p := xrayproc.New(exe, cfgPath, filepath.Join(dir, "xray.log"), filepath.Join(dir, "xray.pid"))
	c := &Client{
		Xray:  p,
		Stats: stats.New("127.0.0.1:1"), // 无人监听 → gRPC 热更必然失败
	}

	const cfg = `{"inbounds":[{"tag":"in-a"}]}`
	res := c.dispatch(mustMsg(t, protocol.MsgSyncUsers, protocol.SyncUsersPayload{
		Users:      map[string][]protocol.User{"in-a": {{Email: "u1@t.local", UUID: "11111111-1111-1111-1111-111111111111"}}},
		ConfigJSON: cfg,
	}))
	if res == nil || res.OK {
		t.Fatalf("热更失败应如实回执 OK=false，实际: %+v", res)
	}
	got, rerr := os.ReadFile(cfgPath)
	if rerr != nil {
		t.Fatalf("热更失败时仍必须尝试落盘（偏离修复依赖它）: %v", rerr)
	}
	if string(got) != cfg {
		t.Fatalf("落盘内容应与主控下发一致，实际: %s", got)
	}
}
