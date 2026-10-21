package upgrade

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// 本文件：geoip.dat / geosite.dat 的落盘。与内核替换共用「sha256 必检 + 原子替换 + .prev 回滚」
// 语义，但数据文件不需要可执行位，且内容未变时不重写（避免无意义地触发 xray 重载）。

// DefaultGeoDir xray 在 Linux 上的标准资产目录（搜索顺序中的 /usr/local/share/xray）。
const DefaultGeoDir = "/usr/local/share/xray"

// GeoFileNames 面板目前用到的两个规则库文件名。
var GeoFileNames = []string{"geoip.dat", "geosite.dat"}

// ValidateGeoFileName 只允许落盘到 geo 目录下的普通文件名，防路径穿越。
func ValidateGeoFileName(name string) error {
	name = strings.TrimSpace(name)
	if name == "" || name != filepath.Base(name) || strings.HasPrefix(name, ".") ||
		strings.ContainsAny(name, `/\`) {
		return fmt.Errorf("非法的 geo 数据文件名: %q", name)
	}
	return nil
}

// ReplaceGeoFile 原子替换 geo 数据文件。
// 返回 changed=false 表示内容与现有文件逐字节相同（不备份、不重写）。
// 返回的 prevPath 是替换前的备份路径（无旧文件时为空）。
func ReplaceGeoFile(dir, name string, data []byte) (changed bool, prevPath string, err error) {
	if err := ValidateGeoFileName(name); err != nil {
		return false, "", err
	}
	if len(data) == 0 {
		return false, "", fmt.Errorf("geo 数据为空: %s", name)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return false, "", fmt.Errorf("创建 geo 目录失败: %w", err)
	}
	dst := filepath.Join(dir, name)

	if old, err := os.ReadFile(dst); err == nil && Sha256Hex(old) == Sha256Hex(data) {
		return false, "", nil
	}

	prevPath = dst + ".prev"
	if _, statErr := os.Stat(dst); statErr == nil {
		if err := copyFile(dst, prevPath); err != nil {
			return false, "", fmt.Errorf("备份现有 %s 失败: %w", name, err)
		}
	} else {
		prevPath = ""
	}

	tmp := dst + ".tmp"
	out, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return false, "", err
	}
	if _, err := out.Write(data); err != nil {
		out.Close()
		_ = os.Remove(tmp)
		return false, "", err
	}
	if err := out.Sync(); err != nil {
		out.Close()
		_ = os.Remove(tmp)
		return false, "", err
	}
	if err := out.Close(); err != nil {
		_ = os.Remove(tmp)
		return false, "", err
	}
	if err := os.Rename(tmp, dst); err != nil {
		_ = os.Remove(tmp)
		return false, "", err
	}
	return true, prevPath, nil
}
