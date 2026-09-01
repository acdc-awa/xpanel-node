package stats

import (
	"testing"

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

	if got := onlineUsersFromResp(&statsService.GetUsersStatsResponse{}); got != nil && len(got) != 0 {
		t.Errorf("空回复应得空列表, got %v", got)
	}
}

func TestCloneOnlineUsers(t *testing.T) {
	src := []OnlineUser{{Email: "a@b.c", IPs: []string{"1.1.1.1"}}}
	dst := cloneOnlineUsers(src)
	dst[0].IPs[0] = "2.2.2.2"
	if src[0].IPs[0] != "1.1.1.1" {
		t.Errorf("clone 应为深拷贝, src 被改动: %v", src[0].IPs)
	}
	if cloneOnlineUsers(nil) != nil {
		t.Error("clone(nil) 应返回 nil")
	}
}
