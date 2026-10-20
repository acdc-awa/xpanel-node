// Package certs 处理主控 push_cert 下发的 TLS 证书落盘。
//
// 约定（05 号文档 §4）：落盘 `/etc/xray/certs/<domain>/{fullchain.pem,key.pem}`，
// fullchain 0644、key 0600，原子写；xray 默认每小时热重载证书文件，换证不重启。
// 证书/私钥校验与解析在共享包 pkg/tlscert（主控 certs API 同源）。
package certs

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/acdc-awa/xpanel-node/pkg/tlscert"
)

// ValidatePair 校验证书与私钥（委托共享包）。
func ValidatePair(certPEM, keyPEM string) error {
	return tlscert.ValidatePair(certPEM, keyPEM)
}

// Write 校验并落盘 <dir>/<domain>/{fullchain.pem,key.pem}（原子写；key 600）。
func Write(dir, domain, certPEM, keyPEM string) error {
	if !tlscert.DomainRe.MatchString(domain) {
		return fmt.Errorf("非法 domain %q（仅允许字母数字 . -）", domain)
	}
	if err := ValidatePair(certPEM, keyPEM); err != nil {
		return err
	}
	sub := filepath.Join(dir, domain)
	if err := os.MkdirAll(sub, 0o755); err != nil {
		return fmt.Errorf("创建证书目录失败: %w", err)
	}
	if err := atomicWrite(filepath.Join(sub, "fullchain.pem"), certPEM, 0o644); err != nil {
		return fmt.Errorf("写 fullchain.pem 失败: %w", err)
	}
	if err := atomicWrite(filepath.Join(sub, "key.pem"), keyPEM, 0o600); err != nil {
		return fmt.Errorf("写 key.pem 失败: %w", err)
	}
	return nil
}

// atomicWrite 临时文件 + rename 原子替换。
func atomicWrite(path, content string, perm os.FileMode) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(content), perm); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

// CAPath CA 根证书的固定落盘路径：<certs_dir>/ca/<id>.crt。
// 主控按同一约定生成出站配置的 certificateFile（见 contracts.CACertPath）。
func CAPath(dir string, id uint64) string {
	return filepath.Join(dir, "ca", fmt.Sprintf("%d.crt", id))
}

// WriteCA 校验并落盘 CA 根证书（只有公钥；CA 私钥永不下发）。
// 返回落盘路径供回执/日志使用。
func WriteCA(dir string, id uint64, certPEM string) (string, error) {
	if id == 0 {
		return "", fmt.Errorf("CA id 不能为 0")
	}
	if _, err := tlscert.ParseCA(certPEM); err != nil {
		return "", err
	}
	sub := filepath.Join(dir, "ca")
	if err := os.MkdirAll(sub, 0o755); err != nil {
		return "", fmt.Errorf("创建 CA 目录失败: %w", err)
	}
	path := CAPath(dir, id)
	if err := atomicWrite(path, certPEM, 0o644); err != nil {
		return "", fmt.Errorf("写 CA 证书失败: %w", err)
	}
	return path, nil
}

// SanitizeDomain 供日志使用（委托共享包）。
func SanitizeDomain(domain string) string {
	return tlscert.SanitizeDomain(domain)
}
