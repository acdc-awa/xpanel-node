package certs

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
)

// genCert 生成自签证书（ECDSA P-256，测试用）。
func genCert(t *testing.T, cn string) (certPEM, keyPEM string, leaf *x509.Certificate) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    now.Add(-time.Hour),
		NotAfter:     now.Add(24 * time.Hour),
		IsCA:         false,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err = x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certPEM = string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	keyPEM = string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}))
	return certPEM, keyPEM, leaf
}

func TestValidatePair(t *testing.T) {
	certPEM, keyPEM, _ := genCert(t, "test.local")
	if err := ValidatePair(certPEM, keyPEM); err != nil {
		t.Fatalf("匹配的证书/私钥应通过: %v", err)
	}
}

func TestValidatePairMismatch(t *testing.T) {
	certPEM, _, _ := genCert(t, "a.local")
	_, keyPEM2, _ := genCert(t, "b.local")
	if err := ValidatePair(certPEM, keyPEM2); err == nil {
		t.Error("不匹配的证书/私钥应报错")
	}
}

func TestValidatePairBadPEM(t *testing.T) {
	if err := ValidatePair("not-pem", "not-key"); err == nil {
		t.Error("非法 PEM 应报错")
	}
}

func TestWrite(t *testing.T) {
	dir := t.TempDir()
	certPEM, keyPEM, _ := genCert(t, "test.local")

	if err := Write(dir, "test.local", certPEM, keyPEM); err != nil {
		t.Fatalf("Write: %v", err)
	}
	sub := filepath.Join(dir, "test.local")
	fullchain := filepath.Join(sub, "fullchain.pem")
	key := filepath.Join(sub, "key.pem")
	for _, p := range []string{fullchain, key} {
		if _, err := os.Stat(p); err != nil {
			t.Fatalf("缺少 %s: %v", p, err)
		}
	}
	// POSIX 权限位仅 Linux 有效（Windows 恒 0666）
	if runtime.GOOS != "windows" {
		if info, _ := os.Stat(key); info.Mode().Perm() != 0o600 {
			t.Errorf("key.pem 权限 = %o, want 600", info.Mode().Perm())
		}
		if info, _ := os.Stat(fullchain); info.Mode().Perm() != 0o644 {
			t.Errorf("fullchain.pem 权限 = %o, want 644", info.Mode().Perm())
		}
	}
	// 内容一致
	data, _ := os.ReadFile(fullchain)
	if string(data) != certPEM {
		t.Error("fullchain 内容不一致")
	}
}

func TestWriteRejectsTraversal(t *testing.T) {
	dir := t.TempDir()
	certPEM, keyPEM, _ := genCert(t, "x.local")
	for _, domain := range []string{"../evil", "a/b", "..", "/etc/passwd", ""} {
		if err := Write(dir, domain, certPEM, keyPEM); err == nil {
			t.Errorf("非法 domain %q 应报错", domain)
		}
	}
	// 未发生越界写入
	if _, err := os.Stat(filepath.Join(dir, "evil")); !os.IsNotExist(err) {
		t.Error("不应创建越界目录")
	}
}

func TestSanitizeDomain(t *testing.T) {
	if got := SanitizeDomain("a\nb"); strings.Contains(got, "\n") {
		t.Errorf("SanitizeDomain 未去除控制字符: %q", got)
	}
}

// genCA 生成自签 CA 根证书（IsCA=true）。
func genCA(t *testing.T, cn string) string {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(2),
		Subject:               pkix.Name{CommonName: cn},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(24 * time.Hour),
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

func TestWriteCA(t *testing.T) {
	dir := t.TempDir()
	caPEM := genCA(t, "Probe Root CA")
	path, err := WriteCA(dir, 7, caPEM)
	if err != nil {
		t.Fatalf("WriteCA: %v", err)
	}
	if path != filepath.Join(dir, "ca", "7.crt") {
		t.Errorf("路径 = %s, want %s", path, filepath.Join(dir, "ca", "7.crt"))
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("读取落盘 CA: %v", err)
	}
	if string(data) != caPEM {
		t.Error("CA 内容不一致")
	}
	if runtime.GOOS != "windows" {
		if info, _ := os.Stat(path); info.Mode().Perm() != 0o644 {
			t.Errorf("CA 权限 = %o, want 644", info.Mode().Perm())
		}
	}
}

func TestWriteCARejectsNonCA(t *testing.T) {
	dir := t.TempDir()
	certPEM, _, _ := genCert(t, "leaf.local")
	if _, err := WriteCA(dir, 1, certPEM); err == nil {
		t.Error("非 CA 证书应被拒绝")
	}
	if _, err := os.Stat(filepath.Join(dir, "ca")); !os.IsNotExist(err) {
		t.Error("校验失败时不应创建目录")
	}
}

func TestWriteCARejectsZeroID(t *testing.T) {
	dir := t.TempDir()
	if _, err := WriteCA(dir, 0, genCA(t, "x")); err == nil {
		t.Error("id=0 应被拒绝")
	}
}

func TestSyncManaged(t *testing.T) {
	dir := t.TempDir()
	certPEM, keyPEM, _ := genCert(t, "keep.local")
	for _, d := range []string{"keep.local", "stale.local"} {
		if err := Write(dir, d, certPEM, keyPEM); err != nil {
			t.Fatal(err)
		}
	}
	// 非托管内容：CA 目录、无 fullchain 的人工目录、根目录散文件
	if err := os.MkdirAll(filepath.Join(dir, "ca"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "ca", "1.crt"), []byte("ca"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "manual-dir"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "manual-dir", "notes.txt"), []byte("hand made"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "loose.pem"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}

	removed, err := SyncManaged(dir, []string{"keep.local"})
	if err != nil {
		t.Fatalf("SyncManaged: %v", err)
	}
	if len(removed) != 1 || removed[0] != "stale.local" {
		t.Errorf("removed = %v, want [stale.local]", removed)
	}
	if _, err := os.Stat(filepath.Join(dir, "stale.local")); !os.IsNotExist(err) {
		t.Error("stale.local 应被删除")
	}
	for _, keep := range []string{"keep.local", "ca", "manual-dir"} {
		if _, err := os.Stat(filepath.Join(dir, keep)); err != nil {
			t.Errorf("%s 不应被删除: %v", keep, err)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "loose.pem")); err != nil {
		t.Error("根目录散文件不应被删除")
	}
}

func TestSyncManagedEmptySetRemovesAllManaged(t *testing.T) {
	dir := t.TempDir()
	certPEM, keyPEM, _ := genCert(t, "a.local")
	if err := Write(dir, "a.local", certPEM, keyPEM); err != nil {
		t.Fatal(err)
	}
	removed, err := SyncManaged(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(removed) != 1 || removed[0] != "a.local" {
		t.Errorf("removed = %v, want [a.local]", removed)
	}
}
