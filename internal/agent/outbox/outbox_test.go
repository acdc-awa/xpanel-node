package outbox

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func newTestOutbox(t *testing.T) *Outbox {
	t.Helper()
	o := New(filepath.Join(t.TempDir(), "traffic_outbox.json"))
	if err := o.Load(); err != nil {
		t.Fatalf("Load: %v", err)
	}
	return o
}

// TestAppendPersistAndAck 落盘 → 重载（模拟进程重启）→ 批次仍在 → 确认后删除。
// 这是「主控写库失败/主控崩溃/回执丢失」三种情况都不丢数据的根基。
func TestAppendPersistAndAck(t *testing.T) {
	path := filepath.Join(t.TempDir(), "traffic_outbox.json")
	o := New(path)
	if err := o.Load(); err != nil {
		t.Fatalf("Load: %v", err)
	}

	b, ok, err := o.Append([]Entry{{Email: "u1.i2@panel.local", Up: 100, Down: 200, CycleID: 3}})
	if err != nil || !ok {
		t.Fatalf("Append: ok=%v err=%v", ok, err)
	}
	if b.ID == "" || b.Seq != 1 {
		t.Fatalf("批次应有 ID 与递增 Seq，实际 id=%q seq=%d", b.ID, b.Seq)
	}

	// 模拟进程重启：新实例从同一路径加载
	o2 := New(path)
	if err := o2.Load(); err != nil {
		t.Fatalf("重载失败: %v", err)
	}
	batches, entries := o2.Pending()
	if batches != 1 || entries != 1 {
		t.Fatalf("重启后应恢复 1 批 1 条，实际 %d 批 %d 条", batches, entries)
	}
	got, ok := o2.Oldest()
	if !ok || got.ID != b.ID {
		t.Fatalf("重启后批次 ID 应保持不变（重发复用同一 ID 才能被主控去重），实际 %q want %q", got.ID, b.ID)
	}
	if got.Entries[0].CycleID != 3 {
		t.Fatalf("账期 ID 应随批次持久化，实际 %d", got.Entries[0].CycleID)
	}

	// 确认后删除并再次重载
	hit, err := o2.Ack(b.ID)
	if err != nil || !hit {
		t.Fatalf("Ack: hit=%v err=%v", hit, err)
	}
	o3 := New(path)
	if err := o3.Load(); err != nil {
		t.Fatalf("重载失败: %v", err)
	}
	if batches, _ := o3.Pending(); batches != 0 {
		t.Fatalf("确认后重启不应恢复任何批次，实际 %d", batches)
	}
}

// TestSeqMonotonicAcrossRestart Seq 跨重启单调递增（运维判缺口的前提）。
func TestSeqMonotonicAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "traffic_outbox.json")
	o := New(path)
	_ = o.Load()
	if _, _, err := o.Append([]Entry{{Email: "a", Up: 1}}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := o.Append([]Entry{{Email: "b", Up: 1}}); err != nil {
		t.Fatal(err)
	}

	o2 := New(path)
	if err := o2.Load(); err != nil {
		t.Fatal(err)
	}
	b, _, err := o2.Append([]Entry{{Email: "c", Up: 1}})
	if err != nil {
		t.Fatal(err)
	}
	if b.Seq != 3 {
		t.Fatalf("重启后 Seq 应接着 3（不重置），实际 %d", b.Seq)
	}
}

// TestBatchIsImmutable 新数据进新批次，不并入已建批次。
// 若并入，同一 BatchID 两次投递内容不同，主控按 ID 去重会丢掉后加的部分。
func TestBatchIsImmutable(t *testing.T) {
	o := newTestOutbox(t)
	b1, _, _ := o.Append([]Entry{{Email: "a", Up: 10}})
	_, _, _ = o.Append([]Entry{{Email: "a", Up: 20}})

	first, _ := o.Oldest()
	if len(first.Entries) != 1 || first.Entries[0].Up != 10 {
		t.Fatalf("最旧批次内容必须保持创建时原样，实际 %+v", first.Entries)
	}
	if first.ID != b1.ID {
		t.Fatalf("最旧批次应为先建的那个")
	}
}

// TestOldestExceptSkipsSentBatches 跳过「已发出待回执」的批次取下一个未发的：
// 这是上报侧同连接不重发、又能继续排空积压的基础。
func TestOldestExceptSkipsSentBatches(t *testing.T) {
	o := newTestOutbox(t)
	b1, _, _ := o.Append([]Entry{{Email: "a", Up: 10}})
	b2, _, _ := o.Append([]Entry{{Email: "a", Up: 20}})
	b3, _, _ := o.Append([]Entry{{Email: "a", Up: 30}})

	sent := map[string]struct{}{b1.ID: {}}
	got, ok := o.OldestExcept(func(id string) bool { _, skip := sent[id]; return skip })
	if !ok || got.ID != b2.ID {
		t.Fatalf("应跳过已发出的 b1 取 b2，实际 ok=%v id=%s", ok, got.ID)
	}

	// 全部已发出 → 无可发送批次（而不是回落到队首重发）
	for _, b := range []Batch{b1, b2, b3} {
		sent[b.ID] = struct{}{}
	}
	if _, ok := o.OldestExcept(func(id string) bool { _, skip := sent[id]; return skip }); ok {
		t.Fatal("全部已发出时不应返回任何批次")
	}

	// 空发件箱
	empty := newTestOutbox(t)
	if _, ok := empty.OldestExcept(func(string) bool { return false }); ok {
		t.Fatal("空发件箱不应返回批次")
	}
}

// TestAckUnknownBatchNoop 确认未知批次 ID 不报错也不改动状态（重复回执）。
func TestAckUnknownBatchNoop(t *testing.T) {

	o := newTestOutbox(t)
	_, _, _ = o.Append([]Entry{{Email: "a", Up: 10}})
	hit, err := o.Ack("nonexistent")
	if err != nil || hit {
		t.Fatalf("未知批次确认应 no-op，实际 hit=%v err=%v", hit, err)
	}
	if batches, _ := o.Pending(); batches != 1 {
		t.Fatalf("状态不应被改动，实际 %d 批", batches)
	}
}

// TestDropExpired 超龄批次被丢弃（与主控去重记录保留期对齐），未超龄的保留。
func TestDropExpired(t *testing.T) {
	o := newTestOutbox(t)
	o.SetLimits(7*24*time.Hour, 100)
	old, _, _ := o.Append([]Entry{{Email: "old", Up: 10}})
	fresh, _, _ := o.Append([]Entry{{Email: "fresh", Up: 20}})

	// 把最旧批次的时间拨到 8 天前（直接改内存后落盘由 DropExpired 触发）
	o.mu.Lock()
	o.batches[0].CreatedAt = time.Now().Add(-8 * 24 * time.Hour)
	o.mu.Unlock()

	if n := o.DropExpired(time.Now()); n != 1 {
		t.Fatalf("应丢弃 1 个超龄批次，实际 %d", n)
	}
	if o.Dropped() != 1 {
		t.Fatalf("累计丢弃数应为 1，实际 %d", o.Dropped())
	}
	rest, _ := o.Oldest()
	if rest.ID != fresh.ID {
		t.Fatalf("未超龄批次应保留，实际队首 %q want %q", rest.ID, fresh.ID)
	}
	_ = old
}

// TestDropOverLimit 批次数量上限生效（长期离线的安全阀）。
func TestDropOverLimit(t *testing.T) {
	o := newTestOutbox(t)
	o.SetLimits(24*time.Hour, 3)
	for i := 0; i < 5; i++ {
		if _, _, err := o.Append([]Entry{{Email: "a", Up: int64(i + 1)}}); err != nil {
			t.Fatal(err)
		}
	}
	if n := o.DropExpired(time.Now()); n != 2 {
		t.Fatalf("超量应丢弃 2 个最旧批次，实际 %d", n)
	}
	first, _ := o.Oldest()
	if len(first.Entries) != 1 || first.Entries[0].Up != 3 {
		t.Fatalf("应保留最后 3 批（队首 Up=3），实际 %+v", first.Entries)
	}
}

// TestCorruptFileQuarantined 损坏文件被隔离而非静默覆盖（保留现场），并返回错误供告警。
func TestCorruptFileQuarantined(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "traffic_outbox.json")
	if err := os.WriteFile(path, []byte("{ this is not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	o := New(path)
	err := o.Load()
	if err == nil {
		t.Fatal("损坏文件应返回错误（供调用方告警）")
	}
	if _, serr := os.Stat(path); !os.IsNotExist(serr) {
		t.Fatal("损坏文件应被改名隔离，原路径不应残留")
	}
	matches, _ := filepath.Glob(path + ".corrupt-*")
	if len(matches) != 1 {
		t.Fatalf("应留下 1 个隔离文件，实际 %d", len(matches))
	}
	if batches, _ := o.Pending(); batches != 0 {
		t.Fatalf("隔离后应以空发件箱继续，实际 %d 批", batches)
	}
	// 隔离后仍可正常写入
	if _, _, err := o.Append([]Entry{{Email: "a", Up: 1}}); err != nil {
		t.Fatalf("隔离后应可继续写入: %v", err)
	}
}

// TestMemoryOnlyMode 未配置路径时纯内存可用（不落盘、不报错）。
func TestMemoryOnlyMode(t *testing.T) {
	o := New("")
	if err := o.Load(); err != nil {
		t.Fatalf("纯内存模式 Load 不应报错: %v", err)
	}
	if o.Persistent() {
		t.Fatal("空路径应为非持久化模式")
	}
	if _, ok, err := o.Append([]Entry{{Email: "a", Up: 1}}); err != nil || !ok {
		t.Fatalf("纯内存模式 Append 应成功: ok=%v err=%v", ok, err)
	}
	if batches, _ := o.Pending(); batches != 1 {
		t.Fatalf("纯内存模式应有 1 批，实际 %d", batches)
	}
}

// TestFilePermissions 落盘文件权限 600（含流量明细，不应全局可读）。
func TestFilePermissions(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows 不适用 POSIX 权限")
	}
	path := filepath.Join(t.TempDir(), "traffic_outbox.json")
	o := New(path)
	if _, _, err := o.Append([]Entry{{Email: "a", Up: 1}}); err != nil {
		t.Fatal(err)
	}
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if st.Mode().Perm() != 0o600 {
		t.Fatalf("权限应为 0600，实际 %v", st.Mode().Perm())
	}
}

// TestFileShapeStable 落盘结构是 {seq, batches}（跨版本可读，不随协议字段演进漂移）。
func TestFileShapeStable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "traffic_outbox.json")
	o := New(path)
	if _, _, err := o.Append([]Entry{{Email: "a", Up: 1, CycleID: 2}}); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var f struct {
		Seq     uint64 `json:"seq"`
		Batches []struct {
			ID      string `json:"id"`
			Seq     uint64 `json:"seq"`
			BootID  string `json:"boot_id"`
			Entries []struct {
				Email   string `json:"email"`
				Up      int64  `json:"up"`
				CycleID uint64 `json:"cycle_id"`
			} `json:"entries"`
		} `json:"batches"`
	}
	if err := json.Unmarshal(raw, &f); err != nil {
		t.Fatalf("落盘 JSON 结构不符: %v", err)
	}
	if f.Seq != 1 || len(f.Batches) != 1 || f.Batches[0].Seq != 1 || f.Batches[0].BootID == "" {
		t.Fatalf("落盘内容不符: %+v", f)
	}
	if f.Batches[0].Entries[0].CycleID != 2 {
		t.Fatalf("账期 ID 应落盘，实际 %+v", f.Batches[0].Entries[0])
	}
}
