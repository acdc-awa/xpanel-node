// Package i18n 提供 CLI 的中英文文案切换。
//
// 语言状态**不落库、不写配置文件**：写在 /run/xray-agent/lang（systemd 系统上 /run 是 tmpfs，
// 只在内存里，重启即回到默认中文）。之所以需要一个文件而不是纯进程内变量：CLI 每次执行都是
// 新进程，纯内存状态无法跨命令生效。
//
// 优先级：环境变量 XRAY_AGENT_LANG（单次覆盖，适合脚本）> 状态文件 > 默认中文。
package i18n

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const (
	// ZH 中文（默认）。
	ZH = "zh"
	// EN 英文。
	EN = "en"
	// EnvKey 单次覆盖语言的环境变量。
	EnvKey = "XRAY_AGENT_LANG"
)

// StatePath 语言状态文件（/run 为内存盘，重启即恢复默认）。
var StatePath = "/run/xray-agent/lang"

var (
	mu       sync.Mutex
	cached   string // 状态文件解析结果（进程内缓存；env 每次都现读）
	loaded   bool
	override string // Set 之后本进程立即生效
)

// Normalize 归一化语言标记（接受 en / en-US / zh / zh-CN，大小写不敏感）。
func Normalize(raw string) (string, bool) {
	s := strings.ToLower(strings.TrimSpace(raw))
	switch {
	case s == "":
		return "", false
	case strings.HasPrefix(s, "en"):
		return EN, true
	case strings.HasPrefix(s, "zh"):
		return ZH, true
	}
	return "", false
}

// Lang 当前语言：环境变量 > 状态文件 > 默认中文。
func Lang() string {
	mu.Lock()
	over, ok := override, override != ""
	mu.Unlock()
	// 环境变量每次现读：测试与脚本里的单次覆盖要立刻生效（os.Getenv 只是 map 查找）。
	if v, ok2 := Normalize(os.Getenv(EnvKey)); ok2 {
		return v
	}
	if ok {
		return over
	}
	mu.Lock()
	defer mu.Unlock()
	if loaded {
		return cached
	}
	loaded, cached = true, ZH
	if data, err := os.ReadFile(StatePath); err == nil {
		if v, ok3 := Normalize(string(data)); ok3 {
			cached = v
		}
	}
	return cached
}

// EnvOverride 返回当前生效的环境变量语言（未设置返回空）。
func EnvOverride() string {
	if v, ok := Normalize(os.Getenv(EnvKey)); ok {
		return v
	}
	return ""
}

// Set 切换语言并写入状态文件（内存盘，不落库）。
func Set(raw string) (string, error) {
	lang, ok := Normalize(raw)
	if !ok {
		return "", fmt.Errorf("unsupported language %q", raw)
	}
	if err := os.MkdirAll(filepath.Dir(StatePath), 0o755); err != nil {
		return lang, err
	}
	if err := os.WriteFile(StatePath, []byte(lang+"\n"), 0o644); err != nil {
		return lang, err
	}
	mu.Lock()
	override, cached, loaded = lang, lang, true
	mu.Unlock()
	return lang, nil
}

// Clear 清除状态文件（回到默认中文 / 环境变量）。
func Clear() error {
	err := os.Remove(StatePath)
	mu.Lock()
	override, cached, loaded = "", ZH, true
	mu.Unlock()
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// T 按当前语言二选一。
func T(zh, en string) string {
	if Lang() == EN {
		return en
	}
	return zh
}

// Tf 带格式化的二选一。
func Tf(zh, en string, args ...any) string {
	return fmt.Sprintf(T(zh, en), args...)
}
