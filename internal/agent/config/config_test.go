package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// fullYAML 拼出合法配置：必填字段齐全，extras 追加额外字段片段。
func fullYAML(extras string) string {
	return "master:\n" +
		"  url: ws://127.0.0.1:18082/node/ws\n" +
		"  node_id: node-1\n" +
		"  secret: s3cret\n" +
		"xray:\n" +
		"  bin: /usr/local/bin/xray\n" + extras
}

func writeConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "agent.yaml")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadValid(t *testing.T) {
	cfg, err := Load(writeConfig(t, fullYAML("stats:\n  api_addr: 127.0.0.1:10085\n")))
	if err != nil {
		t.Fatalf("Load(valid) 报错: %v", err)
	}
	// 未显式配置的 duration 字段应保留默认值
	if cfg.Heartbeat != 30*time.Second {
		t.Errorf("Heartbeat = %v, want 30s 默认", cfg.Heartbeat)
	}
	if cfg.ReconnectMax != 60*time.Second {
		t.Errorf("ReconnectMax = %v, want 60s 默认", cfg.ReconnectMax)
	}
	// 未写 geo 段时也应有默认周期与目录（资产列表为空 = 只用官方内核包）
	if cfg.Geo.Interval != 168*time.Hour || cfg.Geo.Dir == "" {
		t.Errorf("Geo 默认值 = %+v，期望 168h + 默认目录", cfg.Geo)
	}
}

func TestLoadGeoSection(t *testing.T) {
	body := fullYAML("stats:\n  api_addr: 127.0.0.1:10085\n" +
		"geo:\n  interval: 24h\n  assets:\n" +
		"    - file: geoip.dat\n      url: https://example.com/geoip.dat\n      sha256: abc\n")
	cfg, err := Load(writeConfig(t, body))
	if err != nil {
		t.Fatalf("Load(geo) 报错: %v", err)
	}
	if cfg.Geo.Interval != 24*time.Hour {
		t.Errorf("Geo.Interval = %v，期望 24h", cfg.Geo.Interval)
	}
	if len(cfg.Geo.Assets) != 1 || cfg.Geo.Assets[0].File != "geoip.dat" ||
		cfg.Geo.Assets[0].SHA256 != "abc" {
		t.Errorf("Geo.Assets 解析错误: %+v", cfg.Geo.Assets)
	}
}

func TestLoadRejectsNonPositiveDurations(t *testing.T) {
	statsBase := "stats:\n  api_addr: 127.0.0.1:10085\n"
	cases := []struct {
		name  string
		yaml  string
		field string // 期望错误信息包含的配置路径；空 = 仅断言报错（yaml 解析层拒绝，错误文案不含字段名）
	}{
		{"heartbeat=0s", fullYAML(statsBase + "heartbeat_interval: 0s\n"), "heartbeat_interval"},
		{"heartbeat 负值", fullYAML(statsBase + "heartbeat_interval: -5s\n"), "heartbeat_interval"},
		{"heartbeat 裸0（解析层拒绝）", fullYAML(statsBase + "heartbeat_interval: 0\n"), ""},
		{"reconnect_max=0s", fullYAML(statsBase + "reconnect_max: 0s\n"), "reconnect_max"},
		{"collect=0s", fullYAML(statsBase + "  collect_interval: 0s\n"), "stats.collect_interval"},
		{"report 负值", fullYAML(statsBase + "  report_interval: -1s\n"), "stats.report_interval"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load(writeConfig(t, tc.yaml))
			if err == nil {
				t.Fatalf("Load(%s) 未报错，want 拒绝", tc.name)
			}
			if tc.field != "" && !strings.Contains(err.Error(), tc.field) {
				t.Errorf("错误信息 %q 未包含字段 %q", err.Error(), tc.field)
			}
		})
	}
}
