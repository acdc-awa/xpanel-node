package upgrade

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
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
	return srv, &Fetcher{Repo: repo, Mirror: srv.URL}
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
	f := &Fetcher{Repo: "o/r", Mirror: srv.URL}
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
