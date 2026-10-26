package i18n

import (
	"os"
	"path/filepath"
	"testing"
)

// 每个用例都换一个临时状态文件，避免互相影响。
func withState(t *testing.T) string {
	t.Helper()
	old := StatePath
	StatePath = filepath.Join(t.TempDir(), "lang")
	t.Cleanup(func() {
		StatePath = old
		override, cached, loaded = "", "", false
	})
	override, cached, loaded = "", "", false
	return StatePath
}

func TestNormalize(t *testing.T) {
	cases := map[string]string{
		"en": EN, "EN": EN, "en-US": EN, "En_us": EN,
		"zh": ZH, "ZH-CN": ZH, "zh_CN": ZH,
		"": "", "fr": "",
	}
	for in, want := range cases {
		got, ok := Normalize(in)
		if want == "" {
			if ok {
				t.Errorf("Normalize(%q) 应判为不支持", in)
			}
			continue
		}
		if !ok || got != want {
			t.Errorf("Normalize(%q) = %q,%v want %q", in, got, ok, want)
		}
	}
}

func TestDefaultIsChinese(t *testing.T) {
	withState(t)
	t.Setenv(EnvKey, "")
	if got := Lang(); got != ZH {
		t.Fatalf("默认语言应为中文，实际 %q", got)
	}
	if got := T("中文", "English"); got != "中文" {
		t.Fatalf("默认 T 应返回中文，实际 %q", got)
	}
}

func TestSetWritesStateAndApplies(t *testing.T) {
	path := withState(t)
	t.Setenv(EnvKey, "")
	lang, err := Set("en")
	if err != nil {
		t.Fatal(err)
	}
	if lang != EN {
		t.Fatalf("Set 返回 %q", lang)
	}
	if data, err := os.ReadFile(path); err != nil || stringsTrim(string(data)) != EN {
		t.Fatalf("状态文件应写入 en，实际 %q err=%v", string(data), err)
	}
	if got := T("中文", "English"); got != "English" {
		t.Fatalf("切换后 T 应返回英文，实际 %q", got)
	}
	if err := Clear(); err != nil {
		t.Fatal(err)
	}
	if got := Lang(); got != ZH {
		t.Fatalf("Clear 后应回到中文，实际 %q", got)
	}
}

// 环境变量是单次覆盖：即使状态文件是 en，也应优先用 env。
func TestEnvOverridesState(t *testing.T) {
	withState(t)
	if _, err := Set("en"); err != nil {
		t.Fatal(err)
	}
	t.Setenv(EnvKey, "zh")
	if got := Lang(); got != ZH {
		t.Fatalf("环境变量应优先，实际 %q", got)
	}
	if got := EnvOverride(); got != ZH {
		t.Fatalf("EnvOverride = %q", got)
	}
}

func TestSetRejectsUnknown(t *testing.T) {
	withState(t)
	if _, err := Set("klingon"); err == nil {
		t.Fatal("不支持的语言应报错")
	}
}

func stringsTrim(s string) string {
	for len(s) > 0 && (s[len(s)-1] == '\n' || s[len(s)-1] == '\r' || s[len(s)-1] == ' ') {
		s = s[:len(s)-1]
	}
	return s
}
