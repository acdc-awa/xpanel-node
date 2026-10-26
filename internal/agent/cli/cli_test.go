package cli

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/acdc-awa/xpanel-node/internal/agent/i18n"
	"github.com/acdc-awa/xpanel-node/internal/agent/logfile"
)

func run(t *testing.T, args ...string) (code int, out, errOut string) {
	t.Helper()
	t.Setenv(i18n.EnvKey, i18n.ZH)
	var outBuf, errBuf bytes.Buffer
	code = Run(args, nil, &outBuf, &errBuf)
	return code, outBuf.String(), errBuf.String()
}

func TestHelpListsAllCommands(t *testing.T) {
	code, out, _ := run(t, "help")
	if code != 0 {
		t.Fatalf("help exit code = %d, want 0", code)
	}
	for _, c := range []string{"run", "status", "restart", "logs", "uninstall", "upgrade", "rollback", "start", "stop", "lang", "help"} {
		if !strings.Contains(out, c) {
			t.Errorf("help 输出未包含子命令 %q", c)
		}
	}
	if !strings.Contains(out, "用法") {
		t.Error("help 输出缺少用法说明")
	}
}

func TestHelpAliases(t *testing.T) {
	for _, a := range []string{"-h", "-help", "--help"} {
		code, out, _ := run(t, a)
		if code != 0 {
			t.Errorf("%s exit code = %d, want 0", a, code)
		}
		if !strings.Contains(out, "用法") {
			t.Errorf("%s 输出缺少用法说明", a)
		}
	}
}

func TestHelpSubcommand(t *testing.T) {
	code, out, _ := run(t, "help", "logs")
	if code != 0 {
		t.Fatalf("help logs exit code = %d, want 0", code)
	}
	if !strings.Contains(out, "用法") {
		t.Error("help logs 输出缺少用法说明")
	}

	code, out, _ = run(t, "help", "rollback")
	if code != 0 {
		t.Fatalf("help rollback exit code = %d, want 0", code)
	}
	if !strings.Contains(out, "xray-agent rollback") || !strings.Contains(out, "回滚") {
		t.Errorf("help rollback 输出不符合预期: %s", out)
	}
}

func TestRollbackMissingTargetVersion(t *testing.T) {
	code, _, errOut := run(t, "rollback")
	if code != 2 {
		t.Fatalf("rollback 缺少参数 exit code = %d, want 2", code)
	}
	if !strings.Contains(errOut, "缺少目标版本号") {
		t.Errorf("errOut 应提示缺少目标版本号，实际: %s", errOut)
	}
}

func TestHelpUnknownSubcommand(t *testing.T) {
	code, out, errOut := run(t, "help", "frobnicate")
	if code != 0 {
		t.Fatalf("help frobnicate exit code = %d, want 0", code)
	}
	if !strings.Contains(errOut, "未知命令") {
		t.Errorf("stderr 应提示未知命令，实际: %q", errOut)
	}
	if !strings.Contains(out, "用法") {
		t.Error("应回退到总帮助")
	}
}

func TestUnknownCommand(t *testing.T) {
	code, _, errOut := run(t, "frobnicate")
	if code != 2 {
		t.Fatalf("exit code = %d, want 2", code)
	}
	if !strings.Contains(errOut, "未知命令") || !strings.Contains(errOut, "用法") {
		t.Errorf("stderr 应提示未知命令与用法，实际: %q", errOut)
	}
}

func TestResolveConfigPath(t *testing.T) {
	old := configCandidates
	defer func() { configCandidates = old }()

	dir := t.TempDir()
	f1 := filepath.Join(dir, "a.yml")
	f2 := filepath.Join(dir, "b.yml")
	os.WriteFile(f1, []byte("x"), 0o644)
	os.WriteFile(f2, []byte("x"), 0o644)

	configCandidates = []string{f1, f2}
	if got := resolveConfigPath(""); got != f1 {
		t.Errorf("探测顺序错误: got %q want %q", got, f1)
	}
	if err := os.Remove(f1); err != nil {
		t.Fatal(err)
	}
	if got := resolveConfigPath(""); got != f2 {
		t.Errorf("第一候选缺失时应回退到第二候选: got %q want %q", got, f2)
	}
	if got := resolveConfigPath("explicit.yml"); got != "explicit.yml" {
		t.Errorf("显式路径应优先: got %q", got)
	}
	configCandidates = []string{filepath.Join(dir, "nope.yml")}
	if got := resolveConfigPath(""); got != "" {
		t.Errorf("全部缺失应返回空: got %q", got)
	}
}

func TestIsSubcommand(t *testing.T) {
	for _, c := range []string{"status", "restart", "logs", "uninstall", "help", "run", "-h", "-help", "--help"} {
		if !IsSubcommand(c) {
			t.Errorf("IsSubcommand(%q) = false, want true", c)
		}
	}
	for _, c := range []string{"frobnicate", "", "-config"} {
		if IsSubcommand(c) {
			t.Errorf("IsSubcommand(%q) = true, want false", c)
		}
	}
}

func TestLogsWithoutConfig(t *testing.T) {
	old := configCandidates
	defer func() { configCandidates = old }()
	configCandidates = []string{filepath.Join(t.TempDir(), "nope.yml")}

	code, _, errOut := run(t, "logs")
	if code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	if !strings.Contains(errOut, "未找到配置文件") {
		t.Errorf("应提示未找到配置文件，实际: %q", errOut)
	}
}

func TestStatusWithoutConfig(t *testing.T) {
	old := configCandidates
	defer func() { configCandidates = old }()
	configCandidates = []string{filepath.Join(t.TempDir(), "nope.yml")}

	code, _, errOut := run(t, "status")
	if code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	if !strings.Contains(errOut, "未找到配置文件") {
		t.Errorf("应提示未找到配置文件，实际: %q", errOut)
	}
}

func TestConfirm(t *testing.T) {
	cases := []struct {
		input string
		want  bool
	}{
		{"y\n", true},
		{"Y\n", true},
		{"yes\n", true},
		{"n\n", false},
		{"", false},
		{"\n", false},
	}
	for _, c := range cases {
		if got := confirm(strings.NewReader(c.input), io.Discard, "p"); got != c.want {
			t.Errorf("confirm(%q) = %v, want %v", c.input, got, c.want)
		}
	}
}

func TestRemoveTargets(t *testing.T) {
	dir := t.TempDir()

	binDir := filepath.Join(dir, "bin")
	xrayBin := filepath.Join(binDir, "xray")
	os.MkdirAll(binDir, 0o755)
	os.WriteFile(xrayBin, []byte("x"), 0o755)

	etcDir := filepath.Join(dir, "etc")
	os.MkdirAll(filepath.Join(etcDir, "sub"), 0o755)
	os.WriteFile(filepath.Join(etcDir, "sub", "config.json"), []byte("{}"), 0o644)

	emptyDir := filepath.Join(dir, "empty")
	os.MkdirAll(emptyDir, 0o755)

	pidFile := filepath.Join(dir, "xray.pid")
	os.WriteFile(pidFile, []byte("1"), 0o644)

	targets := uninstallTargets{
		files:   []string{xrayBin},
		dirs:    []string{etcDir, emptyDir},
		pidFile: pidFile,
	}
	warns := removeTargets(targets, io.Discard)
	if len(warns) != 0 {
		t.Fatalf("不应有警告: %v", warns)
	}
	for _, p := range []string{xrayBin, etcDir, emptyDir, pidFile} {
		if _, err := os.Stat(p); err == nil {
			t.Errorf("应已删除 %s", p)
		}
	}
	if _, err := os.Stat(binDir); err != nil {
		t.Errorf("父目录不应被删除: %v", err)
	}
}

func TestRemoveTargetsMissingIsNotWarning(t *testing.T) {
	targets := uninstallTargets{
		files: []string{filepath.Join(t.TempDir(), "nope")},
		dirs:  []string{filepath.Join(t.TempDir(), "nodir")},
	}
	if warns := removeTargets(targets, io.Discard); len(warns) != 0 {
		t.Fatalf("删除不存在的路径不应告警: %v", warns)
	}
}

func TestHelpListsUpgrade(t *testing.T) {
	code, out, _ := run(t, "help")
	if code != 0 {
		t.Fatalf("exit code = %d", code)
	}
	if !strings.Contains(out, "upgrade") {
		t.Error("help 输出应包含 upgrade")
	}
}

func TestUpgradeUnknownFlag(t *testing.T) {
	code, _, errOut := run(t, "upgrade", "--bogus")
	if code != 2 {
		t.Fatalf("exit code = %d, want 2", code)
	}
	if !strings.Contains(errOut, "参数错误") {
		t.Errorf("stderr = %q", errOut)
	}
}

func TestFormatDur(t *testing.T) {
	cases := []struct {
		sec  int64
		want string
	}{
		{5, "5s"},
		{65, "1m5s"},
		{3600, "1h0m"},
		{3661, "1h1m"},
	}
	for _, c := range cases {
		if got := formatDur(c.sec); got != c.want {
			t.Errorf("formatDur(%d) = %q, want %q", c.sec, got, c.want)
		}
	}
}

func TestIsSubcommandIncludesStartStop(t *testing.T) {
	for _, c := range []string{"start", "stop"} {
		if !IsSubcommand(c) {
			t.Errorf("IsSubcommand(%q) = false, want true", c)
		}
	}
}

// 裸执行时的命令面板必须同时给出状态头与分组命令表（旧实现只打一句"已由 systemd 托管"）。
func TestDashboardShowsStatusAndCommands(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yml")
	cfg := "master:\n  url: ws://127.0.0.1:18082/node/ws\n  node_id: n1\n  secret: s\ntest_only: true\n"
	if err := os.WriteFile(path, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}
	old := configCandidates
	defer func() { configCandidates = old }()
	configCandidates = []string{path}

	var buf bytes.Buffer
	if code := Dashboard(&buf); code != 0 {
		t.Fatalf("Dashboard exit code = %d, want 0", code)
	}
	out := buf.String()
	for _, want := range []string{"服务状态", "服务管理", "运行与诊断", "升级与维护", "start", "stop", "restart", "status", "日志"} {
		if !strings.Contains(out, want) {
			t.Errorf("命令面板缺少 %q:\n%s", want, out)
		}
	}
	for _, g := range commandGroups {
		if !strings.Contains(out, groupTitle(g)) {
			t.Errorf("命令面板缺少分组 %q", groupTitle(g))
		}
	}
}

func TestLogsSizeAndVacuum(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "xray.log")
	path := filepath.Join(dir, "config.yml")
	cfg := "master:\n  url: ws://127.0.0.1:18082/node/ws\n  node_id: n1\n  secret: s\n" +
		"xray:\n  log_path: " + filepath.ToSlash(logPath) + "\n  log_max_size_mb: 1\n"
	if err := os.WriteFile(path, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}
	old := configCandidates
	defer func() { configCandidates = old }()
	configCandidates = []string{path}

	// 1MB 上限：写入 800KB 后 --size 应报出占用与上限。
	blob := strings.Repeat("x", 1023) + "\n"
	var sb strings.Builder
	for sb.Len() < 800<<10 {
		sb.WriteString(blob)
	}
	if err := os.WriteFile(logPath, []byte(sb.String()), 0o644); err != nil {
		t.Fatal(err)
	}

	code, out, errOut := run(t, "logs", "--size")
	if code != 0 {
		t.Fatalf("logs --size exit code = %d, err=%q", code, errOut)
	}
	if !strings.Contains(out, "上限") || !strings.Contains(out, "当前占用") {
		t.Errorf("logs --size 输出缺少占用信息:\n%s", out)
	}

	code, out, errOut = run(t, "logs", "--vacuum")
	if code != 0 {
		t.Fatalf("logs --vacuum exit code = %d, err=%q", code, errOut)
	}
	if !strings.Contains(out, "已裁剪") {
		t.Errorf("logs --vacuum 应报告释放空间:\n%s", out)
	}
	if size := logfile.Size(logPath); size > 1<<20 {
		t.Errorf("裁剪后应不超过上限，实际 %d", size)
	}
}

// 语言切换：状态写在 /run 的内存盘文件里（测试指向临时目录），不落库。
func TestLangSwitch(t *testing.T) {
	oldPath := i18n.StatePath
	i18n.StatePath = filepath.Join(t.TempDir(), "lang")
	defer func() { i18n.StatePath = oldPath; _ = i18n.Clear() }()
	// 这些用例要观察状态文件，不能让 run() 里的强制 zh 生效。
	t.Setenv(i18n.EnvKey, "")
	call := func(args ...string) (int, string, string) {
		var out, errBuf bytes.Buffer
		code := Run(args, nil, &out, &errBuf)
		return code, out.String(), errBuf.String()
	}

	// 默认中文
	if code, out, _ := call("help"); code != 0 || !strings.Contains(out, "用法") {
		t.Fatalf("默认应为中文 help，实际 code=%d out=%s", code, out)
	}

	// 切到英文
	if code, out, errOut := call("lang", "en"); code != 0 || !strings.Contains(out, "English") {
		t.Fatalf("lang en 失败 code=%d out=%q err=%q", code, out, errOut)
	}
	if code, out, _ := call("help"); code != 0 || !strings.Contains(out, "Usage") || strings.Contains(out, "用法") {
		t.Fatalf("切换后 help 应为英文，实际 %s", out)
	}

	// lang 无参数：报告当前语言
	if code, out, _ := call("lang"); code != 0 || !strings.Contains(out, "English (en)") {
		t.Fatalf("lang 应报告当前语言，实际 %q", out)
	}

	// 环境变量单次覆盖优先于状态文件
	t.Setenv(i18n.EnvKey, "zh")
	if code, out, _ := call("help"); code != 0 || !strings.Contains(out, "用法") {
		t.Fatalf("环境变量应覆盖状态文件，实际 %s", out)
	}
	t.Setenv(i18n.EnvKey, "")

	// auto 清除设置，回到默认中文
	if code, out, _ := call("lang", "auto"); code != 0 || !strings.Contains(out, "中文") {
		t.Fatalf("lang auto 应回到中文，实际 %q", out)
	}
	if code, out, _ := call("help"); code != 0 || !strings.Contains(out, "用法") {
		t.Fatalf("auto 之后 help 应为中文，实际 %s", out)
	}

	// 不支持的语言
	if code, _, errOut := call("lang", "klingon"); code != 2 || !strings.Contains(errOut, "不支持的语言") {
		t.Fatalf("未知语言应报错，实际 code=%d err=%q", code, errOut)
	}
}
