// Package geo 负责节点上的 Xray 规则库（geoip.dat / geosite.dat）自动更新。
//
// 策略（与面板侧约定一致）：
//   - 配置了 URL 的资产优先按 URL 下载；下载失败或未配置 URL 时，回退到「与当前内核
//     同版本」的 XTLS 官方发布包（同一次已校验的 zip 里解出），不额外信任第三方镜像；
//   - 新文件先落到临时目录，用 `XRAY_LOCATION_ASSET=<临时目录> xray -test` 做语义校验
//     （比 sha256 更能发现「文件是好的但缺了配置引用的 code」），通过后才原子替换；
//   - 落盘前保留 `.prev`，内容未变不重写；
//   - 运行期是否生效取决于 xray 配置里的顶层 geodata.cron：有则由 xray 自己按周期重读，
//     没有则由调用方注入的回调重启 xray（保证旧配置节点也能用上新数据）。
package geo

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/acdc-awa/xpanel-node/internal/agent/upgrade"
)

const (
	// DefaultDir xray 在 Linux 上的标准资产目录（搜索顺序中的 /usr/local/share/xray）。
	DefaultDir = upgrade.DefaultGeoDir
	// DefaultInterval 默认一周更新一次（用户可在 geo.interval 覆盖）。
	DefaultInterval = 168 * time.Hour
	// retryDelay 下载/校验失败后的重试间隔：不为一周，避免一次网络抖动等一周。
	retryDelay = time.Hour
	// maxAssetSize 单个资产体积上限，防止异常响应打爆内存/磁盘。
	maxAssetSize = 256 << 20
)

// Asset 一个规则库文件的来源。URL 为空表示只用官方内核包兜底。
type Asset struct {
	File   string `yaml:"file"`   // 落盘文件名，如 geoip.dat
	URL    string `yaml:"url"`    // 下载地址；空 = 不单独下载
	SHA256 string `yaml:"sha256"` // 期望摘要；空则尝试 <url>.sha256sum，仍取不到时靠 -test 兜底
}

// Config agent.yaml 的 geo 段。
type Config struct {
	Dir       string        `yaml:"dir"`        // 落盘目录，空 = DefaultDir
	Interval  time.Duration `yaml:"interval"`   // 更新周期，0 = DefaultInterval；负数 = 关闭自动更新
	StatePath string        `yaml:"state_path"` // 状态文件，空 = /etc/xray-agent/geo_state.json
	Assets    []Asset       `yaml:"assets"`     // 为空 = 只用官方内核包
}

// DefaultConfig 返回内置默认值（仅目录与周期；资产列表由安装脚本写入）。
func DefaultConfig() Config {
	return Config{Dir: DefaultDir, Interval: DefaultInterval}
}

// State 持久化到磁盘，避免每次 agent 重启都重新下载。
type State struct {
	LastAttempt time.Time `json:"last_attempt"`
	LastSuccess time.Time `json:"last_success"`
	LastError   string    `json:"last_error"`
}

// Updater 定时更新规则库。零值不可用，请用 New。
type Updater struct {
	cfg     Config
	binPath string
	cfgPath string
	mirror  string
	nodeID  string
	fetcher *upgrade.XrayFetcher
	client  *http.Client
	Reload  func() error // 配置里没有 geodata.cron 时，落盘后调用它让新数据生效
	Logf    func(format string, args ...any)
}

// New 构造更新器。fetcher 复用内核升级的镜像候选链与下载超时。
func New(cfg Config, binPath, configPath, mirror, nodeID string, fetcher *upgrade.XrayFetcher) *Updater {
	if cfg.Dir == "" {
		cfg.Dir = DefaultDir
	}
	if cfg.Interval == 0 {
		cfg.Interval = DefaultInterval
	}
	return &Updater{
		cfg:     cfg,
		binPath: binPath,
		cfgPath: configPath,
		mirror:  mirror,
		nodeID:  nodeID,
		fetcher: fetcher,
		client: &http.Client{
			Timeout: 10 * time.Minute,
			Transport: &http.Transport{
				Proxy:                 http.ProxyFromEnvironment,
				ResponseHeaderTimeout: 30 * time.Second,
				IdleConnTimeout:       90 * time.Second,
			},
		},
	}
}

func (u *Updater) logf(format string, args ...any) {
	if u.Logf != nil {
		u.Logf(format, args...)
		return
	}
	log.Printf(format, args...)
}

func (u *Updater) statePath() string {
	if u.cfg.StatePath != "" {
		return u.cfg.StatePath
	}
	return "/etc/xray-agent/geo_state.json"
}

// Enabled 是否启用自动更新。
func (u *Updater) Enabled() bool { return u.cfg.Interval > 0 }

// Run 常驻循环；ctx 取消即退出。
func (u *Updater) Run(ctx context.Context) {
	if !u.Enabled() {
		u.logf("geo-update: 自动更新已关闭（geo.interval <= 0）")
		return
	}
	for {
		delay := u.nextDelay(u.loadState())
		u.logf("geo-update: 下次检查在 %s 后", delay.Round(time.Second))
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
		if err := u.Update(ctx); err != nil {
			u.logf("geo-update: 更新失败: %v", err)
		}
	}
}

// nextDelay 依据持久化状态计算下次执行时间，并加抖动避免集群同时拉取。
func (u *Updater) nextDelay(st State) time.Duration {
	var d time.Duration
	switch {
	case st.LastSuccess.IsZero() && st.LastAttempt.IsZero():
		d = time.Minute // 首次运行稍等，给 xray 启动与主控下发配置留时间
	case st.LastError != "":
		d = time.Until(st.LastAttempt.Add(retryDelay))
	default:
		d = time.Until(st.LastSuccess.Add(u.cfg.Interval))
	}
	if d < 0 {
		d = 0
	}
	return d + time.Duration(rand.Int63n(int64(5*time.Minute)))
}

// Update 执行一轮更新。所有资产都未变化时也返回 nil。
func (u *Updater) Update(ctx context.Context) error {
	st := u.loadState()
	st.LastAttempt = time.Now()
	err := u.update(ctx)
	if err != nil {
		st.LastError = err.Error()
	} else {
		st.LastError = ""
		st.LastSuccess = time.Now()
	}
	if serr := u.saveState(st); serr != nil {
		u.logf("geo-update: 写入状态失败: %v", serr)
	}
	return err
}

func (u *Updater) update(ctx context.Context) error {
	if err := os.MkdirAll(u.cfg.Dir, 0o755); err != nil {
		return fmt.Errorf("创建 geo 目录失败: %w", err)
	}
	stage, err := os.MkdirTemp(u.cfg.Dir, ".geo-stage-")
	if err != nil {
		return fmt.Errorf("创建暂存目录失败: %w", err)
	}
	defer os.RemoveAll(stage)

	// 1. 先按 URL 拉取配置里列出的资产。
	files := make(map[string][]byte)
	var urlErrs []string
	for _, a := range u.cfg.Assets {
		if err := upgrade.ValidateGeoFileName(a.File); err != nil {
			urlErrs = append(urlErrs, err.Error())
			u.logf("geo-update: 跳过非法资产: %v", err)
			continue
		}
		if strings.TrimSpace(a.URL) == "" {
			continue
		}
		data, err := u.downloadAsset(ctx, a)
		if err != nil {
			urlErrs = append(urlErrs, fmt.Sprintf("%s: %v", a.File, err))
			u.logf("geo-update: %s 按 URL 更新失败（将回退官方内核包）: %v", a.File, err)
			continue
		}
		files[a.File] = data
	}

	// 2. 需要更新的文件名集合：配置了资产的按配置，否则两个默认文件都从官方包取。
	wanted := make([]string, 0, len(u.cfg.Assets))
	if len(u.cfg.Assets) == 0 {
		wanted = append(wanted, upgrade.GeoFileNames...)
	} else {
		for _, a := range u.cfg.Assets {
			if upgrade.ValidateGeoFileName(a.File) == nil {
				wanted = append(wanted, a.File)
			}
		}
	}

	missing := make([]string, 0, len(wanted))
	for _, name := range wanted {
		if _, ok := files[name]; !ok {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		bundle, err := u.officialBundle()
		if err != nil {
			if len(urlErrs) > 0 {
				return fmt.Errorf("URL 与官方内核包均不可用: %s；%w", strings.Join(urlErrs, "；"), err)
			}
			return fmt.Errorf("下载官方内核包失败: %w", err)
		}
		for _, name := range missing {
			data := bundle.Geo[name]
			if len(data) == 0 {
				return fmt.Errorf("官方内核包 %s 中没有 %s", bundle.Asset, name)
			}
			files[name] = data
		}
	}

	if len(files) == 0 {
		return fmt.Errorf("没有可更新的规则库")
	}

	// 3. 暂存 + 语义校验：把未更新的既有文件一并复制进去，否则 -test 会因缺文件失败。
	for _, name := range upgrade.GeoFileNames {
		if _, ok := files[name]; ok {
			continue
		}
		if data, err := os.ReadFile(filepath.Join(u.cfg.Dir, name)); err == nil {
			if werr := os.WriteFile(filepath.Join(stage, name), data, 0o644); werr != nil {
				return fmt.Errorf("准备暂存文件失败: %w", werr)
			}
		}
	}
	for name, data := range files {
		if err := os.WriteFile(filepath.Join(stage, name), data, 0o644); err != nil {
			return fmt.Errorf("写入暂存文件 %s 失败: %w", name, err)
		}
	}
	if err := u.validate(ctx, stage); err != nil {
		return fmt.Errorf("规则库校验失败（保留旧文件）: %w", err)
	}

	// 4. 原子替换（失败时按备份回滚已替换的文件）。
	type applied struct{ name, prev string }
	var done []applied
	changed := false
	for name, data := range files {
		ch, prev, err := upgrade.ReplaceGeoFile(u.cfg.Dir, name, data)
		if err != nil {
			for i := len(done) - 1; i >= 0; i-- {
				u.rollback(done[i].name, done[i].prev)
			}
			return fmt.Errorf("替换 %s 失败: %w", name, err)
		}
		if ch {
			changed = true
			done = append(done, applied{name: name, prev: prev})
			u.logf("geo-update: %s 已更新（%d 字节）", name, len(data))
		}
	}
	if !changed {
		u.logf("geo-update: 规则库已是最新，无需替换")
		return nil
	}

	// 5. 让运行中的 xray 看到新数据。
	if u.hasGeoDataCron() {
		u.logf("geo-update: 配置含 geodata.cron，xray 将按周期自行重读")
	} else if u.Reload != nil {
		u.logf("geo-update: 配置无 geodata.cron，重启 xray 以加载新规则库")
		if err := u.Reload(); err != nil {
			u.logf("geo-update: 重启 xray 失败（新数据仍在磁盘，下次启动生效）: %v", err)
		}
	}
	return nil
}

// downloadAsset 下载单个资产并按需校验 sha256。
func (u *Updater) downloadAsset(ctx context.Context, a Asset) ([]byte, error) {
	url := ApplyMirror(strings.TrimSpace(a.URL), u.mirror)
	data, err := u.get(ctx, url)
	if err != nil {
		return nil, err
	}
	want := strings.ToLower(strings.TrimSpace(a.SHA256))
	if want == "" {
		// 第三方发布页通常把摘要放在 <文件>.sha256sum；取不到就交给 -test 语义校验。
		if side, err := u.get(ctx, url+".sha256sum"); err == nil {
			want = parseSha256Sum(side, filepath.Base(url))
		} else {
			u.logf("geo-update: %s 未取到校验和（将以 -test 校验兜底）", a.File)
		}
	}
	if want != "" {
		got := upgrade.Sha256Hex(data)
		if !strings.EqualFold(want, got) {
			return nil, fmt.Errorf("sha256 校验失败: 声明 %s 实际 %s", want, got)
		}
	}
	return data, nil
}

func (u *Updater) get(ctx context.Context, url string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := u.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxAssetSize+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxAssetSize {
		return nil, fmt.Errorf("响应超过 %d 字节上限", maxAssetSize)
	}
	return data, nil
}

// officialBundle 下载与当前内核同版本的官方发布包（取不到当前版本时退到最新版）。
func (u *Updater) officialBundle() (*upgrade.XrayBundle, error) {
	tag := ""
	if u.binPath != "" {
		if v, err := upgrade.XrayBinVersion(u.binPath); err == nil && v != "" {
			tag = v
			if !strings.HasPrefix(tag, "v") {
				tag = "v" + tag
			}
		}
	}
	if tag == "" {
		latest, err := u.fetcher.Latest()
		if err != nil {
			return nil, fmt.Errorf("查询官方版本失败: %w", err)
		}
		tag = latest
	}
	if !upgrade.TagPattern.MatchString(tag) {
		return nil, fmt.Errorf("官方版本号不合法: %s", tag)
	}
	return u.fetcher.XrayDownloadBundle(tag)
}

// validate 用暂存目录做资产根跑一次 `xray -test`。
// 配置不存在（首次启动、主控尚未下发）时跳过——没有可校验的引用，写盘也安全。
func (u *Updater) validate(ctx context.Context, stage string) error {
	if u.binPath == "" || u.cfgPath == "" {
		return nil
	}
	if _, err := os.Stat(u.cfgPath); err != nil {
		u.logf("geo-update: 未找到 xray 配置，跳过 -test 校验")
		return nil
	}
	if _, err := os.Stat(u.binPath); err != nil {
		u.logf("geo-update: 未找到 xray 二进制，跳过 -test 校验")
		return nil
	}
	vctx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	cmd := exec.CommandContext(vctx, u.binPath, "-test", "-c", u.cfgPath)
	cmd.Env = append(os.Environ(), "XRAY_LOCATION_ASSET="+stage)
	out, err := cmd.CombinedOutput()
	if err != nil {
		msg := strings.TrimSpace(string(out))
		if len(msg) > 800 {
			msg = msg[len(msg)-800:]
		}
		return fmt.Errorf("%v: %s", err, msg)
	}
	return nil
}

// hasGeoDataCron 检查当前配置是否有顶层 geodata.cron（有则 xray 会自行重读磁盘）。
func (u *Updater) hasGeoDataCron() bool {
	data, err := os.ReadFile(u.cfgPath)
	if err != nil {
		return false
	}
	var cfg struct {
		GeoData struct {
			Cron string `json:"cron"`
		} `json:"geodata"`
	}
	if err := json.Unmarshal(data, &cfg); err != nil {
		return false
	}
	return strings.TrimSpace(cfg.GeoData.Cron) != ""
}

func (u *Updater) rollback(name, prev string) {
	if prev == "" {
		_ = os.Remove(filepath.Join(u.cfg.Dir, name))
		return
	}
	data, err := os.ReadFile(prev)
	if err != nil {
		u.logf("geo-update: 回滚 %s 失败: %v", name, err)
		return
	}
	if _, _, err := upgrade.ReplaceGeoFile(u.cfg.Dir, name, data); err != nil {
		u.logf("geo-update: 回滚 %s 失败: %v", name, err)
	}
}

func (u *Updater) loadState() State {
	var st State
	data, err := os.ReadFile(u.statePath())
	if err != nil {
		return State{}
	}
	_ = json.Unmarshal(data, &st)
	return st
}

func (u *Updater) saveState(st State) error {
	path := u.statePath()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// ApplyMirror 把 github.com 直链套上配置的镜像前缀；其他域名原样返回。
func ApplyMirror(url, mirror string) string {
	mirror = strings.TrimRight(strings.TrimSpace(mirror), "/")
	if mirror == "" {
		return url
	}
	const gh = "https://github.com"
	if strings.HasPrefix(url, gh+"/") {
		return mirror + url[len(gh):]
	}
	return url
}

// parseSha256Sum 从 sha256sum 文件里取目标文件的摘要（兼容 `hash  name` 与裸摘要）。
func parseSha256Sum(data []byte, name string) string {
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		if len(fields) == 1 && len(fields[0]) == 64 && isHex(fields[0]) {
			return strings.ToLower(fields[0])
		}
		if len(fields) >= 2 && strings.TrimPrefix(fields[1], "*") == name && len(fields[0]) == 64 && isHex(fields[0]) {
			return strings.ToLower(fields[0])
		}
	}
	return ""
}

func isHex(s string) bool {
	if len(s) != 64 {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil
}
