package tlscert

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"strings"
	"testing"
	"time"
)

func TestGenerateSelfSigned(t *testing.T) {
	certPEM, keyPEM, err := GenerateSelfSigned("relay-01.example.com")
	if err != nil {
		t.Fatalf("GenerateSelfSigned: %v", err)
	}
	// 生成的证书/私钥必须过现有配对校验（agent 落盘前也走它）
	if err := ValidatePair(certPEM, keyPEM); err != nil {
		t.Fatalf("ValidatePair: %v", err)
	}
	leaf, err := ParseLeaf(certPEM)
	if err != nil {
		t.Fatalf("ParseLeaf: %v", err)
	}
	if !leaf.IsCA {
		t.Error("自签证书应为 IsCA 形态")
	}
	if leaf.KeyUsage&x509.KeyUsageCertSign != 0 {
		t.Error("自签 leaf 不应带 KeyUsageCertSign（它不签发任何证书）")
	}
	if len(leaf.DNSNames) != 1 || leaf.DNSNames[0] != "relay-01.example.com" {
		t.Errorf("SAN DNSNames = %v", leaf.DNSNames)
	}
	if d := time.Until(leaf.NotAfter); d < 9*365*24*time.Hour {
		t.Errorf("有效期应约 10 年, 剩 %v", d)
	}

	// IP 域名进 IPAddresses 而非 DNSNames
	ipCert, _, err := GenerateSelfSigned("10.0.0.1")
	if err != nil {
		t.Fatalf("GenerateSelfSigned IP: %v", err)
	}
	ipLeaf, _ := ParseLeaf(ipCert)
	if len(ipLeaf.IPAddresses) != 1 {
		t.Errorf("IP 证书 SAN IPAddresses = %v", ipLeaf.IPAddresses)
	}

	// 非法域名拒绝
	if _, _, err := GenerateSelfSigned("bad/domain"); err == nil {
		t.Error("非法域名应被拒绝")
	}
}

func TestPinSHA256Hex(t *testing.T) {
	certPEM, _, err := GenerateSelfSigned("pin.example.com")
	if err != nil {
		t.Fatalf("GenerateSelfSigned: %v", err)
	}
	pin1, err := PinSHA256Hex(certPEM)
	if err != nil {
		t.Fatalf("PinSHA256Hex: %v", err)
	}
	if len(pin1) != 64 {
		t.Fatalf("pin 应为 64 位 hex, got %d", len(pin1))
	}
	if strings.ToLower(pin1) != pin1 {
		t.Error("pin 应为小写 hex")
	}
	// 同证书幂等
	pin2, _ := PinSHA256Hex(certPEM)
	if pin1 != pin2 {
		t.Error("同证书 pin 应一致")
	}
	// 不同证书 pin 不同
	other, _, _ := GenerateSelfSigned("pin2.example.com")
	pin3, _ := PinSHA256Hex(other)
	if pin1 == pin3 {
		t.Error("不同证书 pin 应不同")
	}
	// 非法 PEM 报错
	if _, err := PinSHA256Hex("not a pem"); err == nil {
		t.Error("非法 PEM 应报错")
	}
}

// signingCertPEM 用任意 crypto.Signer 造一张自签证书，供跨算法配对测试。
func signingCertPEM(t *testing.T, key crypto.Signer) (certPEM, keyPEM string) {
	t.Helper()
	tmpl := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "pair-test.local"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, key.Public(), key)
	if err != nil {
		t.Fatalf("CreateCertificate: %v", err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("MarshalPKCS8PrivateKey: %v", err)
	}
	certPEM = string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	keyPEM = string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}))
	return certPEM, keyPEM
}

// TestValidatePair_AllKeyTypes 覆盖 RSA / ECDSA / Ed25519：
// 旧实现手工比 X/Y，Ed25519 会直接落到「不支持的私钥类型」。
func TestValidatePair_AllKeyTypes(t *testing.T) {
	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("rsa.GenerateKey: %v", err)
	}
	ecKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("ecdsa.GenerateKey: %v", err)
	}
	_, edKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("ed25519.GenerateKey: %v", err)
	}

	cases := []struct {
		name string
		key  crypto.Signer
	}{
		{"rsa2048", rsaKey},
		{"ecdsa_p256", ecKey},
		{"ed25519", edKey},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			certPEM, keyPEM := signingCertPEM(t, tc.key)
			if err := ValidatePair(certPEM, keyPEM); err != nil {
				t.Fatalf("ValidatePair(%s): %v", tc.name, err)
			}
		})
	}
}

// TestValidatePair_MismatchedKeys 确认配对失败仍能报错（不能因为换了比较方式就恒真）。
func TestValidatePair_MismatchedKeys(t *testing.T) {
	keyA, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	keyB, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	certPEM, _ := signingCertPEM(t, keyA)
	_, keyPEMB := signingCertPEM(t, keyB)
	if err := ValidatePair(certPEM, keyPEMB); err == nil {
		t.Fatal("不同密钥应报「证书与私钥不匹配」")
	}
}

// TestPublicKeysEqual_RSAExponent 确认 RSA 比较覆盖指数，而不是只比 N。
func TestPublicKeysEqual_RSAExponent(t *testing.T) {
	key, _ := rsa.GenerateKey(rand.Reader, 2048)
	same := &rsa.PublicKey{N: key.N, E: key.E}
	if !PublicKeysEqual(&key.PublicKey, same) {
		t.Fatal("同一 RSA 公钥应判等")
	}
	if PublicKeysEqual(&key.PublicKey, nil) {
		t.Fatal("nil 公钥应判不等")
	}
}
