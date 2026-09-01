package stats

import "testing"

func TestOnlineCounterRegex(t *testing.T) {
	cases := map[string]bool{
		"user>>>a@b.com>>>online":               true,
		"user>>>user-1@panel.local>>>online":    true,
		"user>>>a@b.com>>>traffic>>>uplink":     false,
		"inbound>>>vless-in>>>traffic>>>uplink": false,
	}
	for name, want := range cases {
		if got := onlineRe.MatchString(name); got != want {
			t.Errorf("onlineRe(%q) = %v, want %v", name, got, want)
		}
	}
}

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
