// Package outbox 实现节点流量上报的本地持久化发件箱（审计 F1）。
//
// 解决什么问题：原实现把待上报流量放在内存 map 里，reportPending 发送前先清空、仅当
// WebSocket 写失败才放回，且主控收到报文后不回复执。于是「主控写库失败 / 主控在提交前退出 /
// 回执丢失」这三种情况都会让这批消费永久消失（xray 采集基线已推进，下一轮增量不会重报）。
//
// 本包提供的是**至少一次投递**的持久化侧：
//   - 采集到的增量先落盘成不可变批次（每个批次一个 UUID，重发复用同一 ID）；
//   - 只有收到主控 traffic_ack(ok=true) 才删批；
//   - 进程重启后从磁盘恢复未确认批次，重连继续发。
//
// 幂等由主控侧承担：同一 BatchID 重复投递会被去重（见主控 traffic_batches 表）。
// 因此本包只保证「不丢」，不保证「不重」——重复是正常恢复路径，不是错误。
//
// 文件格式与 accounts.Store 同风格：JSON + 临时文件 rename 原子替换 + 权限 600。
// 损坏文件不静默覆盖：改名隔离为 .corrupt-<ts> 后以空发件箱继续（保留现场供排查），
// 因为「拒绝启动」会让节点彻底停止上报，比丢一批更糟。
package outbox

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Entry 批次内的一条流量增量（与 protocol.TrafficEntry 同构，刻意不依赖 protocol 包：
// 落盘格式独立于协议演进，协议加字段不会让旧文件读不出来）。
type Entry struct {
	Email   string `json:"email,omitempty"`
	Inbound string `json:"inbound,omitempty"`
	Up      int64  `json:"up"`
	Down    int64  `json:"down"`
	// CycleID 采集时刻的账期 ID（0 = 未知）。随批次一起持久化：重发时仍是原账期，
	// 不会因为重发时刻用户的账期已切换而被算到新账期头上。
	CycleID uint64 `json:"cycle_id,omitempty"`
}

// Batch 一个不可变批次。创建后 Entries 不再变动（新数据进新批次）——这是去重成立的前提：
// 同一 BatchID 在两次投递之间内容必须一致，否则主控按 ID 去重会丢掉后加的部分。
type Batch struct {
	ID        string    `json:"id"`
	Seq       uint64    `json:"seq"`
	BootID    string    `json:"boot_id"`
	CreatedAt time.Time `json:"created_at"`
	Entries   []Entry   `json:"entries"`
}

// Up/Down 批次字节合计（用于日志与主控侧去重记录）。
func (b Batch) Up() int64 {
	var n int64
	for _, e := range b.Entries {
		n += e.Up
	}
	return n
}

func (b Batch) Down() int64 {
	var n int64
	for _, e := range b.Entries {
		n += e.Down
	}
	return n
}

// file 落盘结构（含跨重启单调递增的 Seq）。
type file struct {
	Seq     uint64  `json:"seq"`
	Batches []Batch `json:"batches"`
}

// Outbox 持久化发件箱（并发安全）。
type Outbox struct {
	path string
	// maxAge 超龄批次丢弃阈值：必须 ≤ 主控 traffic_batches 去重记录保留期（默认 7 天），
	// 否则「节点长期离线后补报」会在主控去重记录已清理的情况下重复计量。
	maxAge time.Duration
	// maxBatches 批次数量上限（安全阀，防长期离线把磁盘写满）：超出时丢最旧的并告警。
	maxBatches int

	bootID string

	mu      sync.Mutex
	seq     uint64
	batches []Batch
	// dropped 累计被丢弃的批次数（超龄/超量/损坏隔离），供日志与状态展示。
	dropped int
}

// 默认阈值（构造时可覆盖，见 SetLimits）。
const (
	DefaultMaxAge     = 7 * 24 * time.Hour
	DefaultMaxBatches = 20000
)

// New 创建发件箱（path 由 agent 配置注入，默认 /etc/xray-agent/traffic_outbox.json）。
// path 为空 = 纯内存模式（不落盘）：仅供测试与「未配置路径」的降级运行，生产必须给路径。
func New(path string) *Outbox {
	return &Outbox{
		path:       path,
		maxAge:     DefaultMaxAge,
		maxBatches: DefaultMaxBatches,
		bootID:     newID(),
	}
}

// Persistent 是否落盘（path 非空）。
func (o *Outbox) Persistent() bool { return o.path != "" }

// SetLimits 覆盖超龄/超量阈值（测试用；生产走默认值）。
func (o *Outbox) SetLimits(maxAge time.Duration, maxBatches int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if maxAge > 0 {
		o.maxAge = maxAge
	}
	if maxBatches > 0 {
		o.maxBatches = maxBatches
	}
}

// BootID 本次进程启动标识（批次随附，供主控判 Seq 缺口）。
func (o *Outbox) BootID() string { return o.bootID }

// Load 启动时加载磁盘上的未确认批次。文件不存在视为空；损坏时隔离现场并继续（返回 error
// 供调用方告警，但内部状态已可用）。幂等：可重复调用。
func (o *Outbox) Load() error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.path == "" {
		o.batches = nil
		return nil
	}
	data, err := os.ReadFile(o.path)
	if os.IsNotExist(err) {
		o.batches = nil
		return nil
	}
	if err != nil {
		return fmt.Errorf("读取流量发件箱失败: %w", err)
	}
	var f file
	if err := json.Unmarshal(data, &f); err != nil {
		// 隔离现场：改名保留，绝不静默覆盖
		quarantine := fmt.Sprintf("%s.corrupt-%d", o.path, time.Now().Unix())
		if rerr := os.Rename(o.path, quarantine); rerr != nil {
			return fmt.Errorf("流量发件箱损坏且隔离失败: %w（解析错误: %v）", rerr, err)
		}
		o.batches = nil
		return fmt.Errorf("流量发件箱损坏，已隔离为 %s 并以空发件箱继续（解析错误: %v）", quarantine, err)
	}
	o.seq = f.Seq
	o.batches = f.Batches
	return nil
}

// Append 把一批新采集到的增量落盘为不可变批次，返回该批次。
// entries 为空返回零值批次与 false（不建空批次）。
// 落盘失败时**不改变内存状态**（调用方据此保留原始 pending 下轮重试），返回错误。
func (o *Outbox) Append(entries []Entry) (Batch, bool, error) {
	if len(entries) == 0 {
		return Batch{}, false, nil
	}
	o.mu.Lock()
	defer o.mu.Unlock()

	b := Batch{
		ID:        newID(),
		Seq:       o.seq + 1,
		BootID:    o.bootID,
		CreatedAt: time.Now().UTC(),
		Entries:   entries,
	}
	// 先在副本上改，落盘成功才提交到内存（避免落盘失败后内存与磁盘不一致）
	next := make([]Batch, 0, len(o.batches)+1)
	next = append(next, o.batches...)
	next = append(next, b)
	if err := o.saveLocked(o.seq+1, next); err != nil {
		return Batch{}, false, err
	}
	o.seq++
	o.batches = next
	return b, true, nil
}

// Oldest 返回最旧的未确认批次（无则 false）。调用方拿到的是副本（Entries 已复制），
// 可安全在锁外发送。
func (o *Outbox) Oldest() (Batch, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.batches) == 0 {
		return Batch{}, false
	}
	return cloneBatch(o.batches[0]), true
}

// OldestExcept 按 FIFO 返回第一个 skip 判定为「不跳过」的批次（全被跳过或无批次则 false）。
// 供上报侧跳过本连接已发出、正在等主控回执的批次，避免无谓重发。
// 谓词在持有内部锁时调用：实现方只能读自己的状态，不得回调 Outbox 方法（会死锁）。
func (o *Outbox) OldestExcept(skip func(batchID string) bool) (Batch, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	for i := range o.batches {
		if skip(o.batches[i].ID) {
			continue
		}
		return cloneBatch(o.batches[i]), true
	}
	return Batch{}, false
}

// Pending 返回未确认批次数与总条目数。
func (o *Outbox) Pending() (batches, entries int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, b := range o.batches {
		entries += len(b.Entries)
	}
	return len(o.batches), entries
}

// Ack 确认并删除某批次（主控回执 ok=true 时调用）。返回是否命中。
// 落盘失败时不从内存删除（宁可下次重发被主控去重，也不丢数据）。
func (o *Outbox) Ack(batchID string) (bool, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	idx := -1
	for i := range o.batches {
		if o.batches[i].ID == batchID {
			idx = i
			break
		}
	}
	if idx < 0 {
		return false, nil
	}
	next := make([]Batch, 0, len(o.batches)-1)
	next = append(next, o.batches[:idx]...)
	next = append(next, o.batches[idx+1:]...)
	if err := o.saveLocked(o.seq, next); err != nil {
		return false, err
	}
	o.batches = next
	return true, nil
}

// DropExpired 丢弃超龄与超量批次，返回本次丢弃数（累计值见 Dropped）。
// 由上报循环周期调用。丢弃即数据丢失，故返回的数目必须被调用方以告警级别记录。
func (o *Outbox) DropExpired(now time.Time) int {
	o.mu.Lock()
	defer o.mu.Unlock()

	cut := now.Add(-o.maxAge)
	kept := make([]Batch, 0, len(o.batches))
	dropped := 0
	for _, b := range o.batches {
		if b.CreatedAt.Before(cut) {
			dropped++
			continue
		}
		kept = append(kept, b)
	}
	if len(kept) > o.maxBatches {
		over := len(kept) - o.maxBatches
		kept = kept[over:]
		dropped += over
	}
	if dropped == 0 {
		return 0
	}
	if err := o.saveLocked(o.seq, kept); err != nil {
		// 落盘失败：不提交内存变更，下轮再试（避免磁盘与内存不一致）
		return 0
	}
	o.batches = kept
	o.dropped += dropped
	return dropped
}

// Dropped 累计丢弃批次数（进程内）。
func (o *Outbox) Dropped() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.dropped
}

// saveLocked 原子落盘（同目录临时文件 + rename，权限 600）。纯内存模式直接返回。
func (o *Outbox) saveLocked(seq uint64, batches []Batch) error {
	if o.path == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(o.path), 0o755); err != nil {
		return fmt.Errorf("创建发件箱目录失败: %w", err)
	}
	if batches == nil {
		batches = []Batch{}
	}
	data, err := json.Marshal(file{Seq: seq, Batches: batches})
	if err != nil {
		return fmt.Errorf("序列化发件箱失败: %w", err)
	}
	tmp := o.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return fmt.Errorf("写发件箱临时文件失败: %w", err)
	}
	if err := os.Rename(tmp, o.path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("替换发件箱失败: %w", err)
	}
	return nil
}

// cloneBatch 深拷贝（Entries 切片必须复制，否则锁外读取与后续修改竞态）。
func cloneBatch(b Batch) Batch {
	out := b
	out.Entries = append([]Entry(nil), b.Entries...)
	return out
}

// newID 生成 UUID v4 作为批次标识（crypto/rand，不依赖外部库）。
func newID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand 失败属系统级异常：退化为时间戳+纳秒，仍保证唯一性量级
		return fmt.Sprintf("t-%d", time.Now().UnixNano())
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
