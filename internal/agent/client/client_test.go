package client

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/acdc-awa/xpanel-node/internal/agent/certs"
	"github.com/acdc-awa/xpanel-node/pkg/protocol"
)

func testCert(t *testing.T) (certPEM, keyPEM string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test.local"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, _ := x509.MarshalECPrivateKey(key)
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})),
		string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}))
}

func mustMsg(t *testing.T, typ string, payload any) *protocol.Message {
	t.Helper()
	raw, err := protocol.Encode(typ, "req-1", payload)
	if err != nil {
		t.Fatal(err)
	}
	m, err := protocol.Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func setupClient(t *testing.T) *Client {
	t.Helper()
	return &Client{
		CertsDir: filepath.Join(t.TempDir(), "certs"),
	}
}

func TestDispatchPushCert(t *testing.T) {
	c := setupClient(t)
	certPEM, keyPEM := testCert(t)

	res := c.dispatch(mustMsg(t, protocol.MsgPushCert, protocol.PushCertPayload{Domain: "test.local", CertPEM: certPEM, KeyPEM: keyPEM}))
	if res == nil || !res.OK {
		t.Fatalf("push_cert 应成功: %+v", res)
	}
	for _, p := range []string{"fullchain.pem", "key.pem"} {
		f := filepath.Join(c.CertsDir, "test.local", p)
		if _, err := os.Stat(f); err != nil {
			t.Fatalf("缺少 %s: %v", f, err)
		}
	}
	if runtime.GOOS != "windows" {
		info, _ := os.Stat(filepath.Join(c.CertsDir, "test.local", "key.pem"))
		if info.Mode().Perm() != 0o600 {
			t.Errorf("key.pem 权限 = %o, want 600", info.Mode().Perm())
		}
	}
}

func TestDispatchPushCertRejectsBadPair(t *testing.T) {
	c := setupClient(t)
	certPEM, _ := testCert(t)
	_, keyPEM2 := testCert(t) // 不匹配的私钥
	res := c.dispatch(mustMsg(t, protocol.MsgPushCert, protocol.PushCertPayload{Domain: "test.local", CertPEM: certPEM, KeyPEM: keyPEM2}))
	if res == nil || res.OK {
		t.Fatalf("证书/私钥不匹配应失败: %+v", res)
	}
	if !strings.Contains(res.Error, "不匹配") {
		t.Errorf("错误信息应说明不匹配: %s", res.Error)
	}
	// 目录不应被创建
	if _, err := os.Stat(filepath.Join(c.CertsDir, "test.local")); !os.IsNotExist(err) {
		t.Error("失败时不应落盘")
	}
}

func TestDispatchPushCertRejectsTraversal(t *testing.T) {
	c := setupClient(t)
	certPEM, keyPEM := testCert(t)
	res := c.dispatch(mustMsg(t, protocol.MsgPushCert, protocol.PushCertPayload{Domain: "../evil", CertPEM: certPEM, KeyPEM: keyPEM}))
	if res == nil || res.OK {
		t.Fatalf("路径穿越 domain 应失败: %+v", res)
	}
}

func TestDispatchUnknownType(t *testing.T) {
	c := setupClient(t)
	if res := c.dispatch(mustMsg(t, "unknown_type", map[string]any{})); res != nil {
		t.Errorf("未知类型应返回 nil: %+v", res)
	}
}

// TestApplyAgentSettings 主控下发的运行时设置：clamp 到 5s–30min、远程值覆盖 yaml 兜底、
// 采集周期跟随（min(yaml, report)）防上报快于采集的粒度倒挂。
func TestApplyAgentSettings(t *testing.T) {
	c := setupClient(t)
	c.ReportInterval = 60 * time.Second
	c.Heartbeat = 30 * time.Second
	c.CollectInterval = 30 * time.Second
	c.reportReset = make(chan time.Duration, 1)
	c.heartbeatReset = make(chan time.Duration, 1)
	c.collectReset = make(chan time.Duration, 1)

	// 未下发时走 yaml 兜底
	if got := c.effectiveReport(); got != 60*time.Second {
		t.Fatalf("未下发时 effectiveReport = %s, want 60s", got)
	}

	// 正常下发：report 15s → 生效 15s，采集跟随 min(30s, 15s)=15s；心跳不变
	if res := c.dispatch(mustMsg(t, protocol.MsgAgentSettings, protocol.AgentSettingsPayload{ReportIntervalSec: 15})); res == nil || !res.OK {
		t.Fatalf("agent_settings 应成功: %+v", res)
	}
	if got := c.effectiveReport(); got != 15*time.Second {
		t.Fatalf("effectiveReport = %s, want 15s", got)
	}
	if got := c.effectiveCollect(); got != 15*time.Second {
		t.Fatalf("effectiveCollect = %s, want 15s（跟随上报）", got)
	}
	if got := c.effectiveHeartbeat(); got != 30*time.Second {
		t.Fatalf("effectiveHeartbeat = %s, want 30s（未下发不变）", got)
	}

	// clamp：心跳 1s 恰为新下限放行（主控内存路径 0 磁盘 I/O）；上报过小 → 3s；过大 → 30min
	c.dispatch(mustMsg(t, protocol.MsgAgentSettings, protocol.AgentSettingsPayload{HeartbeatIntervalSec: 1}))
	if got := c.effectiveHeartbeat(); got != minHeartbeatInterval {
		t.Fatalf("心跳 1s 应放行, got %s", got)
	}
	c.dispatch(mustMsg(t, protocol.MsgAgentSettings, protocol.AgentSettingsPayload{ReportIntervalSec: 1}))
	if got := c.effectiveReport(); got != minReportInterval {
		t.Fatalf("过小上报应 clamp 到 3s, got %s", got)
	}
	c.dispatch(mustMsg(t, protocol.MsgAgentSettings, protocol.AgentSettingsPayload{ReportIntervalSec: 999999}))
	if got := c.effectiveReport(); got != maxSettingsInterval {
		t.Fatalf("过大上报应 clamp 到 30min, got %s", got)
	}

	// 采集周期只跟随缩短不放大：yaml 30s < report 30min → 采集仍 30s
	if got := c.effectiveCollect(); got != 30*time.Second {
		t.Fatalf("effectiveCollect = %s, want 30s（min(yaml, report)）", got)
	}

	// 全 0 payload：保持现状（回执仍 OK）
	if res := c.dispatch(mustMsg(t, protocol.MsgAgentSettings, protocol.AgentSettingsPayload{})); res == nil || !res.OK {
		t.Fatalf("全 0 设置应成功无变更: %+v", res)
	}
	if got := c.effectiveReport(); got != maxSettingsInterval {
		t.Fatalf("全 0 设置不得改动现值, got %s", got)
	}
}

// testCA 生成自签 CA 证书（IsCA=true，push_ca 用例）。
func testCA(t *testing.T) string {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(9),
		Subject:               pkix.Name{CommonName: "Test Root CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
}

func TestDispatchPushCA(t *testing.T) {
	c := setupClient(t)
	caPEM := testCA(t)
	res := c.dispatch(mustMsg(t, protocol.MsgPushCA, protocol.PushCAPayload{ID: 3, Name: "root", CertPEM: caPEM}))
	if res == nil || !res.OK {
		t.Fatalf("push_ca 应成功: %+v", res)
	}
	f := filepath.Join(c.CertsDir, "ca", "3.crt")
	data, err := os.ReadFile(f)
	if err != nil {
		t.Fatalf("缺少 %s: %v", f, err)
	}
	if string(data) != caPEM {
		t.Error("CA 内容不一致")
	}
}

func TestDispatchPushCARejectsLeaf(t *testing.T) {
	c := setupClient(t)
	certPEM, _ := testCert(t)
	res := c.dispatch(mustMsg(t, protocol.MsgPushCA, protocol.PushCAPayload{ID: 4, CertPEM: certPEM}))
	if res == nil || res.OK {
		t.Fatalf("非 CA 证书应失败: %+v", res)
	}
	if _, err := os.Stat(filepath.Join(c.CertsDir, "ca")); !os.IsNotExist(err) {
		t.Error("失败时不应创建 CA 目录")
	}
}

func TestDispatchPushCARejectsZeroID(t *testing.T) {
	c := setupClient(t)
	res := c.dispatch(mustMsg(t, protocol.MsgPushCA, protocol.PushCAPayload{ID: 0, CertPEM: testCA(t)}))
	if res == nil || res.OK {
		t.Fatalf("id=0 应失败: %+v", res)
	}
}

func TestDispatchSyncCerts(t *testing.T) {
	c := setupClient(t)
	certPEM, keyPEM := testCert(t)
	for _, d := range []string{"keep.local", "stale.local"} {
		if err := certs.Write(c.CertsDir, d, certPEM, keyPEM); err != nil {
			t.Fatal(err)
		}
	}
	res := c.dispatch(mustMsg(t, protocol.MsgSyncCerts, protocol.SyncCertsPayload{Domains: []string{"keep.local"}}))
	if res == nil || !res.OK {
		t.Fatalf("sync_certs 应成功: %+v", res)
	}
	if _, err := os.Stat(filepath.Join(c.CertsDir, "stale.local")); !os.IsNotExist(err) {
		t.Error("stale.local 应被清理")
	}
	if _, err := os.Stat(filepath.Join(c.CertsDir, "keep.local", "fullchain.pem")); err != nil {
		t.Error("keep.local 应保留")
	}
}
