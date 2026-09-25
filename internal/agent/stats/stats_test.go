package stats

import (
	"testing"
	"time"

	statsService "github.com/xtls/xray-core/app/stats/command"
)

func TestInboundCounterRegex(t *testing.T) {
	cases := map[string]bool{
		"inbound>>>vless-in>>>traffic>>>uplink":  true,
		"inbound>>>in-1>>>traffic>>>downlink":    true,
		"inbound>>>vless-in>>>traffic>>>uplinkx": false,
		"inbound>>>vless-in>>>online":            false,
		"user>>>a@b.com>>>traffic>>>uplink":      false,
		"inboundXXX>>>tag>>>traffic>>>downlink":  false,
	}
	for name, want := range cases {
		if got := inboundRe.MatchString(name); got != want {
			t.Errorf("inboundRe(%q) = %v, want %v", name, got, want)
		}
	}
}

func TestOnlineUsersFromResp(t *testing.T) {
	resp := &statsService.GetUsersStatsResponse{
		Users: []*statsService.UserStat{
			{Email: "u1@panel.local", Ips: []*statsService.OnlineIPEntry{
				{Ip: "1.2.3.4", LastSeen: 100},
				{Ip: "5.6.7.8", LastSeen: 200},
			}},
			{Email: "", Ips: []*statsService.OnlineIPEntry{{Ip: "9.9.9.9"}}}, // 空 email 过滤
			{Email: "u2@panel.local", Ips: nil},                              // 无 IP 过滤
			nil,                                                              // 空条目过滤
		},
	}
	users := onlineUsersFromResp(resp)
	if len(users) != 1 {
		t.Fatalf("len(users) = %d, want 1", len(users))
	}
	if users[0].Email != "u1@panel.local" {
		t.Errorf("Email = %q, want u1@panel.local", users[0].Email)
	}
	if len(users[0].IPs) != 2 || users[0].IPs[0] != "1.2.3.4" || users[0].IPs[1] != "5.6.7.8" {
		t.Errorf("IPs = %v, want [1.2.3.4 5.6.7.8]", users[0].IPs)
	}
	if users[0].LastSeen["1.2.3.4"] != 100 || users[0].LastSeen["5.6.7.8"] != 200 {
		t.Errorf("LastSeen = %v, want map[1.2.3.4:100 5.6.7.8:200]", users[0].LastSeen)
	}

	if got := onlineUsersFromResp(&statsService.GetUsersStatsResponse{}); got != nil && len(got) != 0 {
		t.Errorf("空回复应得空列表, got %v", got)
	}
}

func TestCloneOnlineUsers(t *testing.T) {
	src := []OnlineUser{{Email: "a@b.c", IPs: []string{"1.1.1.1"}, LastSeen: map[string]int64{"1.1.1.1": 42}}}
	dst := cloneOnlineUsers(src)
	dst[0].IPs[0] = "2.2.2.2"
	dst[0].LastSeen["1.1.1.1"] = 43
	if src[0].IPs[0] != "1.1.1.1" || src[0].LastSeen["1.1.1.1"] != 42 {
		t.Errorf("clone 应为深拷贝, src 被改动: %v %v", src[0].IPs, src[0].LastSeen)
	}
	if cloneOnlineUsers(nil) != nil {
		t.Error("clone(nil) 应返回 nil")
	}
}

// TestOnlineForHeartbeat 验证心跳取快照的三条容错路径：
// xray 未运行直接清零；RPC 失败沿用未老化的旧快照；旧快照超龄则不上报（防冻结残影）。
func TestOnlineForHeartbeat(t *testing.T) {
	c := New("127.0.0.1:1") // 无 xray，RPC 必失败
	seed := []OnlineUser{{Email: "u1@panel.local", IPs: []string{"1.2.3.4"}, LastSeen: map[string]int64{"1.2.3.4": 100}}}

	t.Run("xray未运行直接清零", func(t *testing.T) {
		c.onlineUsers = cloneOnlineUsers(seed)
		c.onlineAt = time.Now()
		n, users := c.OnlineForHeartbeat(false)
		if n != 0 || users != nil {
			t.Fatalf("xray 未运行应返回空, got n=%d users=%v", n, users)
		}
		if c.onlineUsers != nil || !c.onlineAt.IsZero() {
			t.Fatalf("内部快照应被清零, users=%v at=%v", c.onlineUsers, c.onlineAt)
		}
	})

	t.Run("RPC失败沿用未老化旧快照", func(t *testing.T) {
		c.onlineUsers = cloneOnlineUsers(seed)
		c.onlineAt = time.Now().Add(-10 * time.Second) // 未超龄
		n, users := c.OnlineForHeartbeat(true)
		if n != 1 || len(users) != 1 || users[0].Email != "u1@panel.local" {
			t.Fatalf("RPC 失败且未老化应沿用旧快照, got n=%d users=%v", n, users)
		}
	})

	t.Run("超龄快照不上报", func(t *testing.T) {
		c.onlineUsers = cloneOnlineUsers(seed)
		c.onlineAt = time.Now().Add(-maxOnlineStale - time.Second)
		n, users := c.OnlineForHeartbeat(true)
		if n != 0 || users != nil {
			t.Fatalf("超龄快照应返回空, got n=%d users=%v", n, users)
		}
	})
}
