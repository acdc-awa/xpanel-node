package upgrade

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"time"
)

// XrayBinVersion 运行 `<bin> version` 并解析版本号。
// bin 为待探测的可执行文件路径；超时或执行失败返回错误（调用方据此判定该二进制不可用）。
func XrayBinVersion(bin string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, bin, "version").CombinedOutput()
	if err != nil {
		if ctx.Err() != nil {
			return "", &probeError{msg: "xray version 探测超时"}
		}
		return "", &probeError{msg: "xray version 执行失败: " + err.Error() + " " + strings.TrimSpace(string(out))}
	}
	v := XrayVersionOf(string(out))
	if v == "" {
		return "", &probeError{msg: "无法解析 xray version 输出: " + strings.TrimSpace(string(out))}
	}
	return v, nil
}

// VerifyXrayBinary 校验待替换的二进制可用：写临时文件 → 执行 version → 解析出版本号。
// 失败时清理临时文件。这是「下载成功」之外的独立一道门：压缩包解出来的可能不是可执行
// xray（架构不符 / 内容被替换），只有能跑起来并报出目标版本才算通过。
func VerifyXrayBinary(data []byte, wanted string) error {
	tmp, err := os.CreateTemp("", "xray-verify-*")
	if err != nil {
		return err
	}
	path := tmp.Name()
	defer os.Remove(path)
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Chmod(path, 0o755); err != nil {
		return err
	}
	got, err := XrayBinVersion(path)
	if err != nil {
		return err
	}
	if wanted != "" && got != strings.TrimPrefix(strings.TrimSpace(wanted), "v") {
		return &probeError{msg: "下载的 xray 版本与目标不符: 目标 " + wanted + " 实际 " + got}
	}
	return nil
}

// probeError 探测类错误的轻量包装（便于调用方区分"二进制不可用"与"网络失败"）。
type probeError struct{ msg string }

func (e *probeError) Error() string { return e.msg }

// IsProbeError 判断是否为二进制探测失败。
func IsProbeError(err error) bool {
	_, ok := err.(*probeError)
	return ok
}
