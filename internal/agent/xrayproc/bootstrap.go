package xrayproc

import (
	_ "embed"
	"os"
	"path/filepath"
)

// bootstrap.json 自举最小配置（2026-08-31）：agent 首次启动、主控尚未推送任何配置时，
// xray 也必须能起来（学习 3x-ui 的"任何时刻都有一份配置在"语义）。内容与主控生成器的
// 空业务入站版输出同构：log + api(HandlerService/StatsService/RoutingService) +
// dokodemo-door gRPC inbound(127.0.0.1:10085) + policy/stats + 基础出站 + api 保护路由。
// 端口与 agent config 的 stats.api_addr 默认值一致；修改端口必须同步本文件、
// config 默认值与主控 internal/master/xray/config.go 的 api inbound 三处。
//
//go:embed bootstrap.json
var bootstrapConfig []byte

// EnsureBootstrapConfig 配置缺失时落盘自举最小模板，返回是否实际写入。
// 只在文件不存在时写入，绝不覆盖已有配置；首个 push_config 到达后 RestartWithConfig
// 原子替换，本模板天然充当"上一份好配置"的回滚兜底（-test 失败时回滚到的就是它，
// xray 至少保持最小配置运行、gRPC 可连，而不是裸死）。
func EnsureBootstrapConfig(configPath string) (bool, error) {
	if _, err := os.Stat(configPath); err == nil {
		return false, nil
	}
	if err := os.MkdirAll(filepath.Dir(configPath), 0o755); err != nil {
		return false, err
	}
	if err := os.WriteFile(configPath, bootstrapConfig, 0o644); err != nil {
		return false, err
	}
	return true, nil
}
