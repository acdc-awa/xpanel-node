// Package upgrade 提供 agent 自升级：版本查询/比较/下载/校验/替换（CLI 与未来 WS 推送共用）。
//
// 下载源：XPanel-Node 仓库的 GitHub Releases。版本查询走 /releases/latest 的
// 重定向 Location（不调 REST API，规避匿名速率限制且兼容镜像站）；二进制与
// checksums.txt 从 /releases/download/<tag>/ 拉取，sha256 必检。
package upgrade

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// Version 当前 agent 版本（构建期 -ldflags -X 注入；"dev" 为默认开发值）。
var Version = "dev"

// DefaultRepo 默认发布仓库（XPanel-Node on GitHub）。
const DefaultRepo = "acdc-awa/XPanel-Node"

// CurrentVersion 返回当前 agent 版本。
func CurrentVersion() string { return Version }

// normVersion 归一化版本串：去掉 v 前缀，截断第一个 - 之前的部分（git describe 输出
// 如 v1.2.3-5-gabc1234 的 pre-release 后缀忽略），非法段视为 0。
func normVersion(s string) string {
	s = strings.TrimSpace(strings.TrimPrefix(s, "v"))
	if i := strings.Index(s, "-"); i >= 0 {
		s = s[:i]
	}
	if s == "" {
		return "0"
	}
	for _, seg := range strings.Split(s, ".") {
		if seg == "" {
			return "0"
		}
		if _, err := strconv.Atoi(seg); err != nil {
			return "0"
		}
	}
	return s
}

// Compare 语义化比较版本：-1 a<b / 0 相等 / 1 a>b。非法版本归一为 "0"。
func Compare(a, b string) int {
	na := normVersion(a)
	nb := normVersion(b)
	as := strings.Split(na, ".")
	bs := strings.Split(nb, ".")
	n := len(as)
	if len(bs) > n {
		n = len(bs)
	}
	for i := 0; i < n; i++ {
		ai, bi := 0, 0
		if i < len(as) {
			ai, _ = strconv.Atoi(as[i])
		}
		if i < len(bs) {
			bi, _ = strconv.Atoi(bs[i])
		}
		if ai < bi {
			return -1
		}
		if ai > bi {
			return 1
		}
	}
	return 0
}

// Fetcher 从 GitHub Releases 查询版本并下载 agent 发布资产。
type Fetcher struct {
	Repo   string // owner/repo，空 = DefaultRepo
	Mirror string // 可选：github.com 的替代基址或代理前缀（如 https://ghproxy.net/https://github.com）
	Client *http.Client
}

func (f *Fetcher) repo() string {
	if f.Repo != "" {
		return f.Repo
	}
	return DefaultRepo
}

// base 下载基址（Mirror 为空时为 https://github.com）。
func (f *Fetcher) base() string {
	if f.Mirror != "" {
		return strings.TrimSuffix(f.Mirror, "/")
	}
	return "https://github.com"
}

func (f *Fetcher) httpClient() *http.Client {
	if f.Client != nil {
		return f.Client
	}
	return &http.Client{Timeout: 60 * time.Second}
}

// AssetName 当前平台的发布资产名（agent 运行于 Linux 节点，恒 linux-<arch>）。
func AssetName() string { return "xray-agent-linux-" + runtime.GOARCH }

// Latest 解析最新 release tag：GET /releases/latest 不跟随重定向，从 Location 取 tag。
func (f *Fetcher) Latest() (string, error) {
	cli := f.httpClient()
	cli.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	}
	url := f.base() + "/" + f.repo() + "/releases/latest"
	resp, err := cli.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	loc := resp.Header.Get("Location")
	if loc == "" {
		return "", fmt.Errorf("未找到最新 release（HTTP %d，无重定向）", resp.StatusCode)
	}
	const marker = "/releases/tag/"
	i := strings.LastIndex(loc, marker)
	if i < 0 {
		return "", fmt.Errorf("无法从重定向解析版本: %s", loc)
	}
	tag := strings.TrimSpace(loc[i+len(marker):])
	if tag == "" {
		return "", fmt.Errorf("无法从重定向解析版本: %s", loc)
	}
	return tag, nil
}

// Download 下载指定 tag 的资产与 checksums.txt，返回数据与期望 sha256（hex）。
func (f *Fetcher) Download(tag string) ([]byte, string, error) {
	base := f.base() + "/" + f.repo() + "/releases/download/" + tag
	asset := AssetName()

	data, err := f.getBytes(base + "/" + asset)
	if err != nil {
		return nil, "", fmt.Errorf("下载 %s 失败: %w", asset, err)
	}
	sumsData, err := f.getBytes(base + "/checksums.txt")
	if err != nil {
		return nil, "", fmt.Errorf("下载 checksums.txt 失败（拒绝无校验升级）: %w", err)
	}
	want := ""
	for line := range strings.Lines(string(sumsData)) {
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[1] == asset {
			want = fields[0]
			break
		}
	}
	if want == "" {
		return nil, "", fmt.Errorf("checksums.txt 缺少 %s 条目（拒绝无校验升级）", asset)
	}
	return data, want, nil
}

func (f *Fetcher) getBytes(url string) ([]byte, error) {
	resp, err := f.httpClient().Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return io.ReadAll(resp.Body)
}

// Sha256Hex 计算数据 sha256 的 hex 串。
func Sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// ErrUpToDate 已是最新版本。
var ErrUpToDate = errors.New("已是最新版本")

// ReplaceBinary 原子替换二进制文件：写入同目录 .tmp → chmod 0755 → rename 覆盖。
// 失败时清理 .tmp。CLI 升级与 WS 触发升级共用。
func ReplaceBinary(exePath string, data []byte) error {
	tmp := exePath + ".tmp"
	if err := os.WriteFile(tmp, data, 0o755); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Chmod(tmp, 0o755); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, exePath); err != nil {
		os.Remove(tmp)
		return err
	}
	return nil
}

// Apply 完整升级流程：查版本 → 比较 → 下载 → sha256 强制校验 → 原子替换 → 重启。
// exePath 为目标二进制路径（通常 os.Executable()）；restart 由调用方注入（systemd 重启或手动提示）。
func Apply(f *Fetcher, exePath string, restart func() error, out io.Writer) error {
	latest, err := f.Latest()
	if err != nil {
		return err
	}
	if Compare(CurrentVersion(), latest) >= 0 {
		fmt.Fprintf(out, "当前版本 %s，已是最新（远端: %s）\n", CurrentVersion(), latest)
		return ErrUpToDate
	}
	fmt.Fprintf(out, "发现新版本 %s（当前 %s），开始升级...\n", latest, CurrentVersion())

	data, wantSum, err := f.Download(latest)
	if err != nil {
		return err
	}
	if !strings.EqualFold(wantSum, Sha256Hex(data)) {
		return fmt.Errorf("sha256 校验失败: 声明 %s 实际 %s", wantSum, Sha256Hex(data))
	}

	if err := ReplaceBinary(exePath, data); err != nil {
		return err
	}
	fmt.Fprintf(out, "二进制已替换（%s）\n", exePath)

	if err := restart(); err != nil {
		return fmt.Errorf("重启失败（新二进制已就位，可手动重启）: %w", err)
	}
	fmt.Fprintln(out, "重启完成，升级成功")
	return nil
}
