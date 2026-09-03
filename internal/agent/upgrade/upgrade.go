// Package upgrade 提供 agent 自升级：版本查询/比较/下载/校验/替换（CLI 与 WS 推送共用）。
//
// 下载源：XPanel-Node 仓库的 GitHub Releases。版本查询走 /releases/latest 的
// 重定向 Location（不调 REST API，规避匿名速率限制且兼容镜像站）；二进制与
// checksums.txt 从 /releases/download/<tag>/ 拉取，sha256 必检。
//
// 镜像候选链：配置的 update.mirror 置顶，其后自动落回内置候选（与 install-agent.sh
// 的 DEFAULT_MIRRORS 同源）。逐个尝试，网络/HTTP 级失败切下一个；checksums.txt
// 拿到了但内容不对（缺条目）视为 release 本身损坏，换镜像无意义，立即报错。
// 资产与 checksums.txt 始终取自同一镜像，保证校验可信。
package upgrade

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Version 当前 agent 版本（构建期 -ldflags -X 注入；"dev" 为默认开发值）。
var Version = "dev"

// DefaultRepo 默认发布仓库（XPanel-Node on GitHub）。
const DefaultRepo = "acdc-awa/XPanel-Node"

// builtinMirrors 内置镜像候选（与 deploy/install-agent.sh DEFAULT_MIRRORS 保持同序）。
var builtinMirrors = []string{
	"https://github.com",
	"https://ghproxy.net/https://github.com",
	"https://gh-proxy.com/https://github.com",
	"https://github.moeyy.xyz/https://github.com",
}

const (
	// queryTimeout 版本查询/checksums 等轻请求的整请求超时。
	queryTimeout = 30 * time.Second
	// defaultDownloadTimeout 单镜像单次资产下载超时（慢链路可经 update.download_timeout 调大）。
	defaultDownloadTimeout = 10 * time.Minute
	// 活性探测超时：拨号/TLS/响应头。死镜像在此阶段快速失败，不消耗整段下载超时。
	dialTimeout   = 10 * time.Second
	tlsTimeout    = 10 * time.Second
	headerTimeout = 20 * time.Second
)

// errBadRelease checksums.txt 已取到但内容不指向本资产：release 损坏，换镜像无意义。
var errBadRelease = errors.New("checksums.txt 与资产不匹配")

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

// Fetcher 从 GitHub Releases 查询版本并下载 agent 发布资产，按镜像候选链逐个尝试。
type Fetcher struct {
	Repo   string // owner/repo，空 = DefaultRepo
	Mirror string // 可选：首选镜像/代理前缀（如 https://ghproxy.net/https://github.com），置顶尝试
	// Mirrors 显式完整候选列表（测试注入用）；非空时忽略 Mirror 与内置列表。
	Mirrors []string
	// DownloadTimeout 单镜像单次资产下载超时，0 = defaultDownloadTimeout。
	DownloadTimeout time.Duration
	// Client 测试注入用；非空时两个角色共用（注入方自管重定向策略）。
	Client *http.Client

	once      sync.Once
	queryCli  *http.Client // 轻请求：30s 超时，不跟随重定向（/releases/latest 靠 Location）
	downloadC *http.Client // 资产下载：整请求超时 = DownloadTimeout，跟随重定向（GitHub 302 → S3）
}

func (f *Fetcher) repo() string {
	if f.Repo != "" {
		return f.Repo
	}
	return DefaultRepo
}

// candidates 镜像候选链：显式列表优先；否则 [Mirror] + builtinMirrors，去空去重。
func (f *Fetcher) candidates() []string {
	var list []string
	if len(f.Mirrors) > 0 {
		list = f.Mirrors
	} else if f.Mirror != "" {
		list = make([]string, 0, len(builtinMirrors)+1)
		list = append(list, f.Mirror)
		list = append(list, builtinMirrors...)
	} else {
		list = builtinMirrors
	}
	seen := make(map[string]bool, len(list))
	out := make([]string, 0, len(list))
	for _, m := range list {
		m = strings.TrimSuffix(strings.TrimSpace(m), "/")
		if m == "" || seen[m] {
			continue
		}
		seen[m] = true
		out = append(out, m)
	}
	return out
}

// clients 惰性构建两个角色的 client，共享同一 Transport（活性超时让死镜像在
// 拨号/TLS/响应头阶段快速出局，不消耗整段下载超时）。
func (f *Fetcher) clients() (query, dl *http.Client) {
	if f.Client != nil {
		return f.Client, f.Client
	}
	f.once.Do(func() {
		tr := &http.Transport{
			DialContext: (&net.Dialer{
				Timeout:   dialTimeout,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			TLSHandshakeTimeout:   tlsTimeout,
			ResponseHeaderTimeout: headerTimeout,
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          4,
			IdleConnTimeout:       90 * time.Second,
		}
		dlTimeout := f.DownloadTimeout
		if dlTimeout <= 0 {
			dlTimeout = defaultDownloadTimeout
		}
		f.queryCli = &http.Client{
			Timeout:   queryTimeout,
			Transport: tr,
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return http.ErrUseLastResponse
			},
		}
		f.downloadC = &http.Client{Timeout: dlTimeout, Transport: tr}
	})
	return f.queryCli, f.downloadC
}

// AssetName 当前平台的发布资产名（agent 运行于 Linux 节点，恒 linux-<arch>）。
func AssetName() string { return "xray-agent-linux-" + runtime.GOARCH }

// Latest 按候选链解析最新 release tag：GET /releases/latest 不跟随重定向，从 Location 取 tag。
func (f *Fetcher) Latest() (string, error) {
	query, _ := f.clients()
	cands := f.candidates()
	var lastErr error
	for _, base := range cands {
		url := base + "/" + f.repo() + "/releases/latest"
		resp, err := query.Get(url)
		if err != nil {
			lastErr = err
			log.Printf("agent-upgrade: 版本查询失败 %s: %v", base, err)
			continue
		}
		loc := resp.Header.Get("Location")
		resp.Body.Close()
		if loc == "" {
			lastErr = fmt.Errorf("未找到最新 release（HTTP %d，无重定向）", resp.StatusCode)
			log.Printf("agent-upgrade: 版本查询失败 %s: %v", base, lastErr)
			continue
		}
		const marker = "/releases/tag/"
		i := strings.LastIndex(loc, marker)
		if i < 0 || strings.TrimSpace(loc[i+len(marker):]) == "" {
			lastErr = fmt.Errorf("无法从重定向解析版本: %s", loc)
			log.Printf("agent-upgrade: 版本查询失败 %s: %v", base, lastErr)
			continue
		}
		return strings.TrimSpace(loc[i+len(marker):]), nil
	}
	return "", fmt.Errorf("所有下载源均查询版本失败（共 %d 个）: %w", len(cands), lastErr)
}

// Download 按候选链下载指定 tag 的资产与 checksums.txt（同一镜像取齐），返回数据与期望 sha256（hex）。
func (f *Fetcher) Download(tag string) ([]byte, string, error) {
	asset := AssetName()
	cands := f.candidates()
	var lastErr error
	for _, base := range cands {
		data, want, err := f.downloadFrom(base, tag, asset)
		if err == nil {
			return data, want, nil
		}
		if errors.Is(err, errBadRelease) {
			// checksums 已取到但缺条目：release 本身损坏，镜像间内容一致，重试无意义
			return nil, "", err
		}
		lastErr = err
		log.Printf("agent-upgrade: 下载失败 %s: %v", base, err)
	}
	return nil, "", fmt.Errorf("所有下载源均下载失败（共 %d 个）: %w", len(cands), lastErr)
}

// downloadFrom 从单个镜像基址取资产 + checksums.txt 并解析期望摘要。
func (f *Fetcher) downloadFrom(base, tag, asset string) ([]byte, string, error) {
	_, dl := f.clients()
	urlBase := base + "/" + f.repo() + "/releases/download/" + tag

	data, err := f.getBytes(dl, urlBase+"/"+asset)
	if err != nil {
		return nil, "", fmt.Errorf("下载 %s 失败: %w", asset, err)
	}
	sumsData, err := f.getBytes(dl, urlBase+"/checksums.txt")
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
		return nil, "", fmt.Errorf("%w: checksums.txt 缺少 %s 条目（拒绝无校验升级）", errBadRelease, asset)
	}
	return data, want, nil
}

func (f *Fetcher) getBytes(cli *http.Client, url string) ([]byte, error) {
	resp, err := cli.Get(url)
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
