package upgrade

import (
	"archive/zip"
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// 本文件：Xray 内核（xray 可执行文件）的下载/校验/原子替换。
//
// 与 agent 自升级的区别（刻意不复用同一份流水线）：
//   - 发布仓库不同：内核来自 XTLS/Xray-core，agent 来自 acdc-awa/XPanel-Node；
//   - 资产形态不同：内核是含 xray + geo 数据的 zip，agent 是裸二进制；
//   - 校验格式不同：内核每个资产配一份 `<asset>.dgst`（键值格式 `SHA2-256= <hex>`），
//     agent 用 `checksums.txt`（`<hex>  <name>`）。
//
// 共同点是镜像候选链与 sha256 强制校验语义——两者都复用 Fetcher 的 plumbing。

// XrayRepo Xray 内核的默认发布仓库。
const XrayRepo = "XTLS/Xray-core"

// XrayAssetName 当前平台的 Xray 发布资产名。
// 命名规则来自 XTLS 的 release 脚本：amd64 用 "64"、arm64 用 "arm64-v8a"。
func XrayAssetName() (string, error) {
	switch runtime.GOARCH {
	case "amd64":
		if runtime.GOOS == "windows" {
			return "Xray-windows-64.zip", nil
		}
		return "Xray-linux-64.zip", nil
	case "arm64":
		if runtime.GOOS == "windows" {
			return "Xray-windows-arm64-v8a.zip", nil
		}
		return "Xray-linux-arm64-v8a.zip", nil
	default:
		return "", fmt.Errorf("不支持的架构 %s/%s（Xray 内核替换仅支持 amd64 / arm64）", runtime.GOOS, runtime.GOARCH)
	}
}

// XrayFetcher 下载 Xray 内核压缩包。复用 Fetcher 的镜像候选链与 HTTP 客户端。
type XrayFetcher struct {
	Fetcher
}

// NewXrayFetcher 供 main 装配：repo 恒为 XTLS/Xray-core（内核来源不随 agent 的 update.repo 变化），
// 但镜像候选链与下载超时沿用节点 update 段的配置。
func NewXrayFetcher(mirror string, timeout time.Duration) *XrayFetcher {
	return &XrayFetcher{Fetcher: Fetcher{Repo: XrayRepo, Mirror: mirror, DownloadTimeout: timeout}}
}

// XrayDownload 按候选链下载指定版本的 Xray 压缩包，校验 sha256 后返回其中 xray 可执行文件的字节。
// 返回的 expected 摘要仅用于日志/回执展示（校验已在内部完成）。
func (f *XrayFetcher) XrayDownload(tag string) (bin []byte, sha string, err error) {
	tag = strings.TrimSpace(tag)
	if !TagPattern.MatchString(tag) {
		return nil, "", fmt.Errorf("无效的 Xray 版本标签: %s", tag)
	}
	asset, err := XrayAssetName()
	if err != nil {
		return nil, "", err
	}
	cands := f.candidates()
	var lastErr error
	for _, base := range cands {
		bin, sha, err = f.downloadXrayFrom(base, tag, asset)
		if err == nil {
			return bin, sha, nil
		}
		if errors.Is(err, errBadRelease) {
			// .dgst 已取到但解析不出摘要：release 本身有问题，换镜像无意义
			return nil, "", err
		}
		lastErr = err
		log.Printf("xray-upgrade: 下载失败 %s: %v", base, err)
	}
	return nil, "", fmt.Errorf("所有下载源均下载失败（共 %d 个）: %w", len(cands), lastErr)
}

// downloadXrayFrom 从单个镜像取压缩包 + .dgst，完成校验与解包。
func (f *XrayFetcher) downloadXrayFrom(base, tag, asset string) ([]byte, string, error) {
	_, dl := f.clients()
	urlBase := base + "/" + f.repo() + "/releases/download/" + tag

	zipData, err := f.getBytes(dl, urlBase+"/"+asset)
	if err != nil {
		return nil, "", fmt.Errorf("下载 %s 失败: %w", asset, err)
	}
	digest, err := f.getBytes(dl, urlBase+"/"+asset+".dgst")
	if err != nil {
		return nil, "", fmt.Errorf("下载 %s.dgst 失败（拒绝无校验升级）: %w", asset, err)
	}
	want := parseDigestSHA256(digest)
	if want == "" {
		return nil, "", fmt.Errorf("%w: %s.dgst 中找不到 SHA2-256 摘要（拒绝无校验升级）", errBadRelease, asset)
	}
	got := Sha256Hex(zipData)
	if !strings.EqualFold(want, got) {
		return nil, "", fmt.Errorf("sha256 校验失败: 声明 %s 实际 %s", want, got)
	}
	bin, err := extractFileFromZip(zipData, "xray")
	if err != nil {
		return nil, "", fmt.Errorf("从 %s 解出 xray 失败: %w", asset, err)
	}
	if len(bin) == 0 {
		return nil, "", fmt.Errorf("%w: %s 中的 xray 为空", errBadRelease, asset)
	}
	return bin, got, nil
}

// parseDigestSHA256 从 .dgst 里取 SHA-256 摘要。
// XTLS 历史上用过三种格式，这里统一按「含 sha256 字样的行里第一个 64 位 hex」抓取：
//
//	v26+     SHA2-256= <hex>
//	BSD      SHA256 (file) = <hex>
//	sha256sum <hex>  <file>
//
// MD5/SHA1/SHA2-512 的摘要位数不同，不会被 64 位模式命中。
func parseDigestSHA256(digest []byte) string {
	for _, line := range strings.Split(string(digest), "\n") {
		lower := strings.ToLower(line)
		if !strings.Contains(lower, "sha256") && !strings.Contains(lower, "sha2-256") {
			continue
		}
		for _, field := range strings.Fields(line) {
			candidate := strings.ToLower(strings.TrimSpace(field))
			if len(candidate) == 64 && isHex(candidate) {
				return candidate
			}
		}
	}
	// 兜底：某些镜像会把 .dgst 改成裸摘要或首行摘要
	for _, field := range strings.Fields(string(digest)) {
		candidate := strings.ToLower(strings.TrimSpace(field))
		if len(candidate) == 64 && isHex(candidate) {
			return candidate
		}
	}
	return ""
}

func isHex(s string) bool {
	for _, c := range s {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// extractFileFromZip 从内存中的 zip 里取出指定文件名（按 basename 匹配，兼容子目录）。
// Windows 发布包内是 xray.exe，Linux 是 xray——两者都映射到 base 名比较。
func extractFileFromZip(data []byte, baseName string) ([]byte, error) {
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, err
	}
	for _, f := range zr.File {
		name := filepath.Base(f.Name)
		if name != baseName && name != baseName+".exe" {
			continue
		}
		rc, err := f.Open()
		if err != nil {
			return nil, err
		}
		defer rc.Close()
		return io.ReadAll(rc)
	}
	return nil, fmt.Errorf("压缩包内未找到 %s", baseName)
}

// XrayVersionOf 解析 `xray version` 输出的版本号（第二列，如 "26.10.17"）。
func XrayVersionOf(output string) string {
	fields := strings.Fields(strings.TrimSpace(output))
	if len(fields) < 2 {
		return ""
	}
	// 期望形如 "Xray 26.10.17 (Xray, Penetrates Everything.) ..."
	for i, f := range fields {
		if strings.EqualFold(f, "xray") && i+1 < len(fields) {
			v := fields[i+1]
			if _, err := strconv.Atoi(strings.Split(v, ".")[0]); err == nil {
				return v
			}
		}
	}
	return ""
}

// ReplaceXrayBinary 原子替换 xray 可执行文件，并保留一份 .prev 供失败回滚。
// 与 ReplaceBinary 的差异：xray 正在运行时其可执行文件在 Linux 上仍可替换（inode 无关），
// 但调用方仍应先 Stop，避免新进程与旧进程混跑。
func ReplaceXrayBinary(binPath string, data []byte) (prevPath string, err error) {
	if binPath == "" {
		return "", fmt.Errorf("xray 二进制路径为空")
	}
	prevPath = binPath + ".prev"

	// 备份现有二进制（首次升级时可能不存在，忽略）
	if _, statErr := os.Stat(binPath); statErr == nil {
		if err := copyFile(binPath, prevPath); err != nil {
			return "", fmt.Errorf("备份现有 xray 失败: %w", err)
		}
	}

	tmp := binPath + ".tmp"
	if err := os.WriteFile(tmp, data, 0o755); err != nil {
		_ = os.Remove(tmp)
		return "", err
	}
	if err := os.Chmod(tmp, 0o755); err != nil {
		_ = os.Remove(tmp)
		return "", err
	}
	if err := os.Rename(tmp, binPath); err != nil {
		_ = os.Remove(tmp)
		return "", err
	}
	return prevPath, nil
}

// RollbackXrayBinary 用 .prev 覆盖回 binPath（替换后自检失败时使用）。
func RollbackXrayBinary(binPath, prevPath string) error {
	if prevPath == "" {
		return fmt.Errorf("没有可回滚的备份")
	}
	data, err := os.ReadFile(prevPath)
	if err != nil {
		return err
	}
	tmp := binPath + ".rollback"
	if err := os.WriteFile(tmp, data, 0o755); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Chmod(tmp, 0o755); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, binPath); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	tmp := dst + ".tmp"
	out, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o755)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := out.Sync(); err != nil {
		out.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := out.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, dst)
}
