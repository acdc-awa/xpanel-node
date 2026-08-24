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
		"  url: ws://127.0.0.1:18080/api/v1/node/ws\n" +
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
