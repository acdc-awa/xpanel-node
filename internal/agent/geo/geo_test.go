package geo

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/acdc-awa/xpanel-node/internal/agent/upgrade"
)

func TestApplyMirror(t *testing.T) {
	const raw = "https://github.com/Loyalsoldier/v2ray-rules-dat/releases/latest/download/geoip.dat"
	cases := []struct{ mirror, want string }{
		{"", raw},
		{"https://github.com", raw},
		{"https://ghproxy.net/https://github.com/", "https://ghproxy.net/https://github.com/Loyalsoldier/v2ray-rules-dat/releases/latest/download/geoip.dat"},
		{"https://mirror.example", "https://mirror.example/Loyalsoldier/v2ray-rules-dat/releases/latest/download/geoip.dat"},
	}
	for _, c := range cases {
		if got := ApplyMirror(raw, c.mirror); got != c.want {
			t.Errorf("ApplyMirror(mirror=%q) = %q, want %q", c.mirror, got, c.want)
		}
	}
	if got := ApplyMirror("https://example.com/geoip.dat", "https://ghproxy.net/https://github.com"); got != "https://example.com/geoip.dat" {
		t.Errorf("非 github 链接不应改写，got %q", got)
	}
}

func TestParseSha256Sum(t *testing.T) {
	const h = "4a1b1eddabf5614fa51dd93ac0498856cb82dd65933cd6d6d5890d8c7bb5e902"
	cases := []struct{ body, name, want string }{
		{h + "  geoip.dat\n", "geoip.dat", h},
		{h + " *geosite.dat\n", "geosite.dat", h},
		{h + "\n", "anything", h},
		{h + "  other.dat\n", "geoip.dat", ""},
		{"not-a-hash  geoip.dat\n", "geoip.dat", ""},
	}
	for _, c := range cases {
		if got := parseSha256Sum([]byte(c.body), c.name); got != c.want {
			t.Errorf("parseSha256Sum(%q, %q) = %q, want %q", c.body, c.name, got, c.want)
		}
	}
}

func TestValidateGeoFileName(t *testing.T) {
	for _, good := range []string{"geoip.dat", "geosite.dat"} {
		if err := upgrade.ValidateGeoFileName(good); err != nil {
			t.Errorf("%q 应被接受: %v", good, err)
		}
	}
	for _, bad := range []string{"", "../geoip.dat", "a/b.dat", ".hidden", `a\b.dat`} {
		if err := upgrade.ValidateGeoFileName(bad); err == nil {
			t.Errorf("%q 应被拒绝", bad)
		}
	}
}

// TestUpdateWithRealXray 用真实 xray 二进制 + 真实规则库跑一遍完整更新链路
// （URL 下载 → 暂存 → XRAY_LOCATION_ASSET 下 -test → 原子替换）。
// 需要显式提供环境变量，避免默认测试依赖大文件：
//
//	XRAY_TEST_BIN=<xray 可执行文件>
//	XRAY_TEST_GEOIP=<geoip.dat>
//	XRAY_TEST_GEOSITE=<geosite.dat>
func TestUpdateWithRealXray(t *testing.T) {
	bin := os.Getenv("XRAY_TEST_BIN")
	geoipPath := os.Getenv("XRAY_TEST_GEOIP")
	geositePath := os.Getenv("XRAY_TEST_GEOSITE")
	if bin == "" || geoipPath == "" || geositePath == "" {
		t.Skip("未提供 XRAY_TEST_BIN/XRAY_TEST_GEOIP/XRAY_TEST_GEOSITE，跳过真实二进制用例")
	}
	geoipData, err := os.ReadFile(geoipPath)
	if err != nil {
		t.Fatalf("读取 geoip.dat 失败: %v", err)
	}
	geositeData, err := os.ReadFile(geositePath)
	if err != nil {
		t.Fatalf("读取 geosite.dat 失败: %v", err)
	}

	mux := http.NewServeMux()
	sum := func(b []byte) []byte {
		h := sha256.Sum256(b)
		return []byte(hex.EncodeToString(h[:]) + "  file\n")
	}
	mux.HandleFunc("/geoip.dat", func(w http.ResponseWriter, r *http.Request) { w.Write(geoipData) })
	mux.HandleFunc("/geoip.dat.sha256sum", func(w http.ResponseWriter, r *http.Request) { w.Write(sum(geoipData)) })
	mux.HandleFunc("/geosite.dat", func(w http.ResponseWriter, r *http.Request) { w.Write(geositeData) })
	mux.HandleFunc("/geosite.dat.sha256sum", func(w http.ResponseWriter, r *http.Request) { w.Write(sum(geositeData)) })
	srv := httptest.NewServer(mux)
	defer srv.Close()

	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.json")
	cfg := map[string]any{
		"log":      map[string]any{"loglevel": "warning"},
		"inbounds": []any{},
		"outbounds": []any{
			map[string]any{"protocol": "freedom", "tag": "direct"},
		},
		"routing": map[string]any{"rules": []any{map[string]any{
			"type": "field", "domain": []string{"geosite:cn", "geosite:google"},
			"ip": []string{"geoip:cn", "geoip:private"}, "outboundTag": "direct",
		}}},
	}
	data, _ := json.Marshal(cfg)
	if err := os.WriteFile(cfgPath, data, 0o644); err != nil {
		t.Fatal(err)
	}

	geoDir := filepath.Join(dir, "geo")
	up := New(Config{
		Dir:       geoDir,
		Interval:  time.Hour,
		StatePath: filepath.Join(dir, "state.json"),
		Assets: []Asset{
			{File: "geoip.dat", URL: srv.URL + "/geoip.dat"},
			{File: "geosite.dat", URL: srv.URL + "/geosite.dat"},
		},
	}, bin, cfgPath, "", "test-node", upgrade.NewXrayFetcher("", time.Minute))
	var logs []string
	up.Logf = func(format string, args ...any) { logs = append(logs, format) }

	if err := up.Update(t.Context()); err != nil {
		t.Fatalf("Update 失败: %v\n日志: %v", err, logs)
	}
	for _, name := range []string{"geoip.dat", "geosite.dat"} {
		got, err := os.ReadFile(filepath.Join(geoDir, name))
		if err != nil {
			t.Fatalf("%s 未落盘: %v", name, err)
		}
		want := geoipData
		if name == "geosite.dat" {
			want = geositeData
		}
		if len(got) != len(want) {
			t.Errorf("%s 大小 %d，期望 %d", name, len(got), len(want))
		}
	}
	var st State
	raw, _ := os.ReadFile(filepath.Join(dir, "state.json"))
	_ = json.Unmarshal(raw, &st)
	if st.LastError != "" || st.LastSuccess.IsZero() {
		t.Errorf("状态异常: %+v", st)
	}

	// 反向验证：再喂一份缺 code 的坏数据，-test 必须拒绝且旧文件保持不变。
	badCfg := []byte(`{"log":{"loglevel":"warning"},"inbounds":[],"outbounds":[{"protocol":"freedom","tag":"direct"}],"routing":{"rules":[{"type":"field","domain":["geosite:code-that-does-not-exist"],"outboundTag":"direct"}]}}`)
	if err := os.WriteFile(cfgPath, badCfg, 0o644); err != nil {
		t.Fatal(err)
	}
	// 用截断数据模拟损坏文件（结构不合法，必然校验失败）
	mux.HandleFunc("/bad.dat", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("not-a-proto")) })
	up.cfg.Assets = []Asset{{File: "geosite.dat", URL: srv.URL + "/bad.dat"}}
	if err := up.Update(t.Context()); err == nil {
		t.Fatal("坏数据应被 -test 拒绝")
	}
	got, _ := os.ReadFile(filepath.Join(geoDir, "geosite.dat"))
	if len(got) != len(geositeData) {
		t.Errorf("校验失败后旧文件被破坏: %d 字节", len(got))
	}
}
