package upgrade

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestCompare(t *testing.T) {
	cases := []struct {
		a, b string
		want int
	}{
		{"v1.0.0", "v1.0.0", 0},
		{"v1.0.1", "v1.0.0", 1},
		{"v1.1.0", "v1.0.9", 1},
		{"v2.0.0", "v1.9.9", 1},
		{"v1.0.0", "v1.0.1", -1},
		{"1.0.0", "v1.0.0", 0},
		{"dev", "v1.0.0", -1}, // 非法归一为 "0"，0 < 1.0.0 → -1
		// git describe 输出：截断第一个 - 前部分再比较（pre-release 忽略）
		{"v1.2.3-5-gabc1234", "v1.2.3-8-gdef5678", 0},
		{"v1.3.0-1-gaaa", "v1.2.9-99-gbbb", 1},
	}
	for _, c := range cases {
		if got := Compare(c.a, c.b); got != c.want {
			t.Errorf("Compare(%q,%q) = %d, want %d", c.a, c.b, got, c.want)
		}
	}
}

// fakeRelease 描述一个模拟的 GitHub Release。
type fakeRelease struct {
	Tag     string // release tag
	Data    []byte // 资产内容
	Asset   string // 资产名（空 = AssetName()）
	Sums    string // checksums.txt 内容（空 = 按 Data 自动生成正确条目）
	NoSums  bool   // 不提供 checksums.txt（404）
	BadSums bool   // checksums.txt 给出错误摘要
}

// newFakeGitHub 起一个模拟 GitHub Releases 布局的 httptest 服务：
// /<repo>/releases/latest 302 重定向到 tag 页；/releases/download/<tag>/ 下提供资产与 checksums.txt。
// 返回的 Fetcher 以该服务为 Mirror（同时覆盖镜像语义）。
func newFakeGitHub(t *testing.T, fr fakeRelease) (*httptest.Server, *Fetcher) {
	t.Helper()
	const repo = "o/r"
	asset := fr.Asset
	if asset == "" {
		asset = AssetName()
	}
	sums := fr.Sums
	if sums == "" {
		sums = fmt.Sprintf("%s  %s\n", Sha256Hex(fr.Data), asset)
	}
	if fr.BadSums {
		sums = fmt.Sprintf("%s  %s\n", "deadbeef", asset)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/"+repo+"/releases/latest", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", "/"+repo+"/releases/tag/"+fr.Tag)
		w.WriteHeader(http.StatusFound)
	})
	mux.HandleFunc("/"+repo+"/releases/download/"+fr.Tag+"/"+asset, func(w http.ResponseWriter, r *http.Request) {
		w.Write(fr.Data)
	})
	mux.HandleFunc("/"+repo+"/releases/download/"+fr.Tag+"/checksums.txt", func(w http.ResponseWriter, r *http.Request) {
		if fr.NoSums {
			http.NotFound(w, r)
			return
		}
		w.Write([]byte(sums))
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	// 候选列表封闭为本服务，避免 fallback 链打到真实 github.com
	return srv, &Fetcher{Repo: repo, Mirrors: []string{srv.URL}}
}

func TestFetcherLatest(t *testing.T) {
	_, f := newFakeGitHub(t, fakeRelease{Tag: "v1.2.3"})
	v, err := f.Latest()
	if err != nil || v != "v1.2.3" {
		t.Fatalf("Latest = %q, %v", v, err)
	}
}

func TestFetcherLatestNoRedirect(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok")) // 200 且无 Location
	}))
	defer srv.Close()
	f := &Fetcher{Repo: "o/r", Mirrors: []string{srv.URL}} // 候选封闭，避免 fallback 到真实 github.com
	if _, err := f.Latest(); err == nil {
		t.Error("无重定向应报错")
	}
}

func TestFetcherDownload(t *testing.T) {
	_, f := newFakeGitHub(t, fakeRelease{Tag: "v1.2.3", Data: []byte("agent-binary")})
	data, sum, err := f.Download("v1.2.3")
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "agent-binary" {
		t.Errorf("data = %q", data)
	}
	if sum != Sha256Hex(data) {
		t.Errorf("checksums 摘要与数据不匹配")
	}
}

func TestFetcherDownloadNoChecksums(t *testing.T) {
	_, f := newFakeGitHub(t, fakeRelease{Tag: "v1.2.3", Data: []byte("x"), NoSums: true})
	if _, _, err := f.Download("v1.2.3"); err == nil || !strings.Contains(err.Error(), "checksums") {
		t.Fatalf("缺 checksums.txt 应拒绝, err = %v", err)
	}
}

func TestFetcherDownloadMissingEntry(t *testing.T) {
	_, f := newFakeGitHub(t, fakeRelease{Tag: "v1.2.3", Data: []byte("x"), Sums: "abc  other-asset\n"})
	if _, _, err := f.Download("v1.2.3"); err == nil || !strings.Contains(err.Error(), "缺少") {
		t.Fatalf("checksums 缺资产条目应拒绝, err = %v", err)
	}
}

func TestSha256Hex(t *testing.T) {
	sum := Sha256Hex([]byte("abc"))
	if !strings.HasPrefix(sum, "ba7816bf") {
		t.Errorf("sha256(abc) = %s", sum)
	}
}

func TestFetcherCandidates(t *testing.T) {
	cases := []struct {
		name    string
		mirror  string
		mirrors []string
		want    []string
	}{
		{"默认内置列表", "", nil, builtinMirrors},
		{"配置镜像置顶", "https://m.example.com", nil, append([]string{"https://m.example.com"}, builtinMirrors...)},
		{"配置镜像与内置重复去重", "https://github.com", nil, builtinMirrors},
		{"显式列表覆盖一切", "https://x.example.com", []string{"https://a/", "https://a", ""}, []string{"https://a"}},
	}
	for _, c := range cases {
		f := &Fetcher{Mirror: c.mirror, Mirrors: c.mirrors}
		got := f.candidates()
		if len(got) != len(c.want) {
			t.Errorf("%s: candidates = %v, want %v", c.name, got, c.want)
			continue
		}
		for i := range got {
			if got[i] != c.want[i] {
				t.Errorf("%s: candidates[%d] = %q, want %q", c.name, i, got[i], c.want[i])
			}
		}
	}
}

// deadServer 起一个随即关闭的服务，作为链首坏镜像（连接拒绝，快速失败）。
func deadServer(t *testing.T) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	url := srv.URL
	srv.Close()
	return url
}

func TestFetcherLatestFallsBack(t *testing.T) {
	_, good := newFakeGitHub(t, fakeRelease{Tag: "v1.2.3"})
	f := &Fetcher{Repo: good.Repo, Mirrors: []string{deadServer(t), good.Mirrors[0]}}
	v, err := f.Latest()
	if err != nil || v != "v1.2.3" {
		t.Fatalf("Latest = %q, %v（坏镜像后应切到好镜像）", v, err)
	}
}

func TestFetcherLatestAllFail(t *testing.T) {
	_, good := newFakeGitHub(t, fakeRelease{Tag: "v1.2.3"})
	f := &Fetcher{Repo: good.Repo, Mirrors: []string{deadServer(t), deadServer(t)}}
	if _, err := f.Latest(); err == nil || !strings.Contains(err.Error(), "所有下载源") {
		t.Fatalf("err = %v, want 全部失败聚合错误", err)
	}
}

func TestFetcherDownloadFallsBack(t *testing.T) {
	_, good := newFakeGitHub(t, fakeRelease{Tag: "v1.2.3", Data: []byte("agent-binary")})
	// 链首坏镜像连接拒绝；资产与 checksums.txt 均从好镜像取齐
	f := &Fetcher{Repo: good.Repo, Mirrors: []string{deadServer(t), good.Mirrors[0]}}
	data, sum, err := f.Download("v1.2.3")
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "agent-binary" || sum != Sha256Hex(data) {
		t.Errorf("data/sum 不正确: %q %q", data, sum)
	}
}

func TestFetcherDownloadBadChecksumsNoFallback(t *testing.T) {
	// 链首 release 损坏（checksums 缺条目）：换镜像无意义，应立即报错而非耗尽候选链
	_, broken := newFakeGitHub(t, fakeRelease{Tag: "v1.2.3", Data: []byte("x"), Sums: "abc  other-asset\n"})
	_, good := newFakeGitHub(t, fakeRelease{Tag: "v1.2.3", Data: []byte("x")})
	f := &Fetcher{Repo: broken.Repo, Mirrors: []string{broken.Mirrors[0], good.Mirrors[0]}}
	_, _, err := f.Download("v1.2.3")
	if err == nil || !strings.Contains(err.Error(), "缺少") {
		t.Fatalf("err = %v, want 缺条目错误", err)
	}
	if strings.Contains(err.Error(), "所有下载源") {
		t.Errorf("release 损坏不应继续耗尽候选链: %v", err)
	}
}

func TestFetcherDownloadTimeout(t *testing.T) {
	_, good := newFakeGitHub(t, fakeRelease{Tag: "v1.2.3"})
	// 默认值
	f := &Fetcher{Repo: good.Repo, Mirrors: good.Mirrors}
	_, dl := f.clients()
	if dl.Timeout != defaultDownloadTimeout {
		t.Errorf("默认下载超时 = %v, want %v", dl.Timeout, defaultDownloadTimeout)
	}
	// 显式配置
	f = &Fetcher{Repo: good.Repo, Mirrors: good.Mirrors, DownloadTimeout: 2 * time.Minute}
	_, dl = f.clients()
	if dl.Timeout != 2*time.Minute {
		t.Errorf("配置下载超时 = %v, want 2m", dl.Timeout)
	}
	// 轻请求 client 不跟随重定向
	q, _ := f.clients()
	if q.CheckRedirect == nil {
		t.Error("query client 应设置 CheckRedirect（不跟随重定向）")
	}
}
