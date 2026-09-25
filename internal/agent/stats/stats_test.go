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

// TestOnlineForHeartbeat 验证心跳读内存快照的容错路径（2026-09-25 重构后为纯读，零 gRPC）：
// 无任何成功快照返回空；xray 未运行发布空快照；快照超龄（xray 卡死、拉取持续失败）不上报。
func TestOnlineForHeartbeat(t *testing.T) {
	seed := []OnlineUser{{Email: "u1@panel.local", IPs: []string{"1.2.3.4"}, LastSeen: map[string]int64{"1.2.3.4": 100}}}

	t.Run("无快照返回空", func(t *testing.T) {
		c := New("127.0.0.1:1")
		n, users := c.OnlineForHeartbeat(true)
		if n != 0 || users != nil {
			t.Fatalf("无成功快照应返回空, got n=%d users=%v", n, users)
		}
	})

	t.Run("xray未运行发布空快照", func(t *testing.T) {
		c := New("127.0.0.1:1")
		c.online.Store(&onlineSnapshot{users: cloneForTest(seed), at: time.Now()})
		n, users := c.OnlineForHeartbeat(false)
		if n != 0 || users != nil {
			t.Fatalf("xray 未运行应返回空, got n=%d users=%v", n, users)
		}
		snap := c.online.Load()
		if snap == nil || !snap.at.IsZero() || len(snap.users) != 0 {
			t.Fatalf("内部应发布空快照, got %+v", snap)
		}
	})

	t.Run("未超龄沿用内存快照", func(t *testing.T) {
		c := New("127.0.0.1:1")
		c.online.Store(&onlineSnapshot{users: cloneForTest(seed), at: time.Now().Add(-10 * time.Second)})
		n, users := c.OnlineForHeartbeat(true)
		if n != 1 || len(users) != 1 || users[0].Email != "u1@panel.local" {
			t.Fatalf("未超龄应返回内存快照, got n=%d users=%v", n, users)
		}
	})

	t.Run("超龄快照不上报", func(t *testing.T) {
		c := New("127.0.0.1:1")
		c.online.Store(&onlineSnapshot{users: cloneForTest(seed), at: time.Now().Add(-maxOnlineStale - time.Second)})
		n, users := c.OnlineForHeartbeat(true)
		if n != 0 || users != nil {
			t.Fatalf("超龄快照应返回空, got n=%d users=%v", n, users)
		}
	})
}

// TestOnlineSnapshotImmutable 验证已发布快照的免拷贝共享是安全的：读者拿到的切片
// 与发布点持有的是同一份不可变数据（写时复制，发布后无人修改）。
func TestOnlineSnapshotImmutable(t *testing.T) {
	c := New("127.0.0.1:1")
	seed := []OnlineUser{{Email: "u1@panel.local", IPs: []string{"1.2.3.4"}, LastSeen: map[string]int64{"1.2.3.4": 100}}}
	c.online.Store(&onlineSnapshot{users: seed, at: time.Now()})

	_, users := c.OnlineForHeartbeat(true)
	if len(users) != 1 || &users[0] != &seed[0] {
		t.Fatalf("应返回已发布快照本体（免拷贝）, got %v", users)
	}
	// RefreshOnlineOnce 失败（无 xray）时旧快照原样保留
	c.RefreshOnlineOnce()
	_, users2 := c.OnlineForHeartbeat(true)
	if len(users2) != 1 || users2[0].Email != "u1@panel.local" {
		t.Fatalf("拉取失败应沿用旧快照, got %v", users2)
	}
	// CloseOnline 幂等（连接未建立也不得 panic）
	c.CloseOnline()
}

// cloneForTest 测试播种用深拷贝（生产路径写时复制，无需克隆）。
func cloneForTest(src []OnlineUser) []OnlineUser {
	out := make([]OnlineUser, len(src))
	for i, u := range src {
		ls := make(map[string]int64, len(u.LastSeen))
		for ip, t := range u.LastSeen {
			ls[ip] = t
		}
		out[i] = OnlineUser{Email: u.Email, IPs: append([]string(nil), u.IPs...), LastSeen: ls}
	}
	return out
}
