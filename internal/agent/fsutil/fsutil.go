// Package fsutil 提供落盘一致性工具：同目录临时文件 → 写入 → fsync → rename，
// 保证目标文件不会出现「写了一半」的中间态（被进程/掉电打断时要么是旧内容，要么是新内容）。
package fsutil

import (
	"io"
	"os"
)

// CopyFile 把 src 复制到 dst，原子替换并显式指定权限。
// 临时文件与目标同目录（保证 rename 不跨文件系统）；失败时清理临时文件、不动 dst。
func CopyFile(src, dst string, perm os.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	tmp := dst + ".tmp"
	out, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		return err
	}
	// 上一次中断可能留下同名的旧临时文件：O_TRUNC 不会改权限，这里显式兜一次。
	if err := out.Chmod(perm); err != nil {
		out.Close()
		_ = os.Remove(tmp)
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := out.Sync(); err != nil {
		out.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := out.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, dst)
}
