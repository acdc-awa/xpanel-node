package protocol

import (
	"strings"
	"testing"
)

func TestNewPayloadsEncodeDecode(t *testing.T) {
	// push_cert 编解码（含 PEM 多行内容）
	raw2, err := Encode(MsgPushCert, "req-2", PushCertPayload{
		Domain: "test.local", CertPEM: "-----BEGIN CERTIFICATE-----\nAAA\n-----END CERTIFICATE-----\n", KeyPEM: "-----BEGIN PRIVATE KEY-----\nBBB\n-----END PRIVATE KEY-----\n",
	})
	if err != nil {
		t.Fatal(err)
	}
	m2, _ := Decode(raw2)
	if m2.Type != MsgPushCert || m2.ID != "req-2" {
		t.Errorf("帧字段不符: %+v", m2)
	}
	var p2 PushCertPayload
	if err := m2.PayloadTo(&p2); err != nil {
		t.Fatal(err)
	}
	if p2.Domain != "test.local" || !strings.Contains(p2.CertPEM, "BEGIN CERTIFICATE") || !strings.Contains(p2.KeyPEM, "BEGIN PRIVATE KEY") {
		t.Errorf("push_cert 载荷不符: %+v", p2)
	}

	// result 回执 data 透传
	raw4, _ := Encode(MsgResult, "req-1", ResultPayload{OK: true, Data: map[string]any{"tag": "relay-a", "uuid": "u"}})
	m4, _ := Decode(raw4)
	var r ResultPayload
	if err := m4.PayloadTo(&r); err != nil {
		t.Fatal(err)
	}
	if !r.OK {
		t.Error("OK 应为 true")
	}
	data, ok := r.Data.(map[string]any)
	if !ok || data["tag"] != "relay-a" || data["uuid"] != "u" {
		t.Errorf("回执 data 不符: %v", r.Data)
	}
}
