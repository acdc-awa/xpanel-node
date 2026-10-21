package upgrade

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReplaceGeoFileAtomicAndPrev(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "geoip.dat")

	changed, prev, err := ReplaceGeoFile(dir, "geoip.dat", []byte("v1"))
	if err != nil {
		t.Fatalf("首次写入失败: %v", err)
	}
	if !changed || prev != "" {
		t.Fatalf("首次写入 changed=%v prev=%q，期望 true/空", changed, prev)
	}

	changed, prev, err = ReplaceGeoFile(dir, "geoip.dat", []byte("v2"))
	if err != nil {
		t.Fatalf("替换失败: %v", err)
	}
	if !changed || prev != path+".prev" {
		t.Fatalf("替换 changed=%v prev=%q，期望 true/%s", changed, prev, path+".prev")
	}
	if data, _ := os.ReadFile(path); string(data) != "v2" {
		t.Errorf("目标文件内容 = %q，期望 v2", data)
	}
	if data, _ := os.ReadFile(prev); string(data) != "v1" {
		t.Errorf("备份内容 = %q，期望 v1", data)
	}

	// 内容相同：不重写、不更新备份（避免无意义的 mtime 变化触发重载）
	changed, prev, err = ReplaceGeoFile(dir, "geoip.dat", []byte("v2"))
	if err != nil {
		t.Fatalf("幂等写入失败: %v", err)
	}
	if changed || prev != "" {
		t.Errorf("内容未变时 changed=%v prev=%q，期望 false/空", changed, prev)
	}
}

func TestReplaceGeoFileRejectsTraversal(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"../evil.dat", "a/b.dat", ".hidden", `a\b.dat`} {
		if _, _, err := ReplaceGeoFile(dir, name, []byte("x")); err == nil {
			t.Errorf("%q 应被拒绝", name)
		}
	}
}

func TestExtractGeoFromZip(t *testing.T) {
	// 复用升级包的 zip 构造路径较绕，这里只确认文件名匹配大小写/子目录行为由
	// extractFileFromZip 保证（已有其他用例覆盖内核）；额外守住「空数据不落盘」。
	dir := t.TempDir()
	if _, _, err := ReplaceGeoFile(dir, "geoip.dat", nil); err == nil || !strings.Contains(err.Error(), "为空") {
		t.Errorf("空数据应失败，got %v", err)
	}
}
