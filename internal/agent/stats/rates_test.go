package stats

import (
	"testing"

	statsService "github.com/xtls/xray-core/app/stats/command"
)

// TestApplyRateDeltas 速率差分的四类边界（2026-11-01 快车道）：
//
//	① 正常增量按 dt 折算成字节/秒；
//	② 计数器首次出现（用户刚开始用）只建基线、本帧记 0，不能把历史总量当一秒流量；
//	③ xray 重启导致值变小同样只重建基线，绝不能算出负数或巨大尖峰；
//	④ 输出 = 在线用户 ∪ 本帧有增量的用户（有增量的用户即使不在在线名单里也必须报，
//	   见 TestApplyRateDeltasKeepsActiveWithoutOnlineSnapshot）。
func TestApplyRateDeltas(t *testing.T) {
	c := &Collector{rateLast: map[string]int64{}}
	stat := func(kv map[string]int64) []*statsService.Stat {
		out := make([]*statsService.Stat, 0, len(kv))
		for name, v := range kv {
			out = append(out, &statsService.Stat{Name: name, Value: v})
		}
		return out
	}

	const a = "user>>>u1.i3@panel.local>>>traffic>>>uplink"
	const ad = "user>>>u1.i3@panel.local>>>traffic>>>downlink"
	const b = "user>>>u2.i3@panel.local>>>traffic>>>uplink"

	// ② 首帧：全部只建基线
	c.seedRateBaseline(stat(map[string]int64{a: 1000, ad: 2000, b: 5000}))
	if got := c.applyRateDeltas(stat(map[string]int64{a: 1000, ad: 2000, b: 5000}), 1, map[string]struct{}{"u1.i3@panel.local": {}}); len(got) != 1 || got[0].Up != 0 || got[0].Down != 0 {
		t.Fatalf("无增量时应为 0，实际 %+v", got)
	}

	// ① 2 秒窗口：a +2000 上行 / +4000 下行 = 1000 / 2000 B/s；b 也有增量（5000→9000）
	// 虽不在在线名单里，按「有增量即算活跃」必须一并输出。
	got := c.applyRateDeltas(stat(map[string]int64{a: 3000, ad: 6000, b: 9000}), 2,
		map[string]struct{}{"u1.i3@panel.local": {}})
	if len(got) != 2 {
		t.Fatalf("应输出在线用户 + 有增量用户共 2 行，实际 %+v", got)
	}
	if got[0].Email != "u1.i3@panel.local" || got[0].Up != 1000 || got[0].Down != 2000 {
		t.Fatalf("速率 = %+v，want u1 1000/2000", got[0])
	}
	if got[1].Email != "u2.i3@panel.local" || got[1].Up != 2000 || got[1].Down != 0 {
		t.Fatalf("非在线但有增量的用户 = %+v，want u2 2000/0", got[1])
	}

	// ③ 值变小（xray 重启）：本帧 0，基线重置为新值
	got = c.applyRateDeltas(stat(map[string]int64{a: 10, ad: 20, b: 30}), 1,
		map[string]struct{}{"u1.i3@panel.local": {}})
	if len(got) != 1 || got[0].Up != 0 || got[0].Down != 0 {
		t.Fatalf("归零帧应为 0（不得出现负值或尖峰），实际 %+v", got)
	}
	// 重置后下一帧恢复正常增量
	got = c.applyRateDeltas(stat(map[string]int64{a: 1010, ad: 2020, b: 30}), 1,
		map[string]struct{}{"u1.i3@panel.local": {}})
	if got[0].Up != 1000 || got[0].Down != 2000 {
		t.Fatalf("重置后速率 = %v/%v，want 1000/2000", got[0].Up, got[0].Down)
	}
}

// TestApplyRateDeltasKeepsActiveWithoutOnlineSnapshot 在线快照为空时仍要报有增量的用户。
//
// xray 的 OnlineMap.AddIP 对 127.0.0.1 / [::1] 直接 return（app/stats/online_map.go），
// 所以纯回环联调、或在线快照超龄时 online 集合是空的。此时若只看在线名单，会出现
// 「用户明明在跑流量、面板却一条速率都没有」——本用例把这条行为钉住。
func TestApplyRateDeltasKeepsActiveWithoutOnlineSnapshot(t *testing.T) {
	c := &Collector{rateLast: map[string]int64{}}
	const a = "user>>>u1.i3@panel.local>>>traffic>>>uplink"
	const ad = "user>>>u1.i3@panel.local>>>traffic>>>downlink"
	const idle = "user>>>u2.i3@panel.local>>>traffic>>>downlink"
	stat := []*statsService.Stat{{Name: a, Value: 100}, {Name: ad, Value: 200}, {Name: idle, Value: 900}}
	c.seedRateBaseline(stat)

	got := c.applyRateDeltas([]*statsService.Stat{
		{Name: a, Value: 1100}, {Name: ad, Value: 2200}, {Name: idle, Value: 900},
	}, 1, map[string]struct{}{})
	if len(got) != 1 {
		t.Fatalf("应只报有增量的那个用户，实际 %+v", got)
	}
	if got[0].Email != "u1.i3@panel.local" || got[0].Up != 1000 || got[0].Down != 2000 {
		t.Fatalf("速率行 = %+v，want u1 1000/2000", got[0])
	}
}

// TestApplyRateDeltasPrunesGoneCounters 计数器消失（用户/入站被删）时基线同步清理，防 map 无界增长。
func TestApplyRateDeltasPrunesGoneCounters(t *testing.T) {
	c := &Collector{rateLast: map[string]int64{
		"user>>>u9.i1@panel.local>>>traffic>>>uplink": 100,
	}}
	online := map[string]struct{}{"u1.i1@panel.local": {}}
	c.applyRateDeltas(nil, 1, online)
	if _, still := c.rateLast["user>>>u9.i1@panel.local>>>traffic>>>uplink"]; still {
		t.Fatal("已消失的计数器基线应被清理")
	}
}
