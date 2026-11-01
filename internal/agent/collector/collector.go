// Package collector 采集节点系统信息（Linux /proc，零外部依赖）。
// 生产 Agent 部署在 Linux systemd 环境，无需跨平台。
package collector

import (
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Snapshot 一次采集结果。
type Snapshot struct {
	CPU       float64 // 百分比（自上次采集以来的均值）
	Mem       float64 // 已用内存（字节）
	MemTotal  float64 // 总内存（字节）
	Disk      float64 // 已用磁盘（字节）
	DiskTotal float64 // 总磁盘（字节）
	RxBytes   uint64  // 物理网卡接收总字节
	TxBytes   uint64  // 物理网卡发送总字节
	RxRate    float64 // 入口带宽速率（字节/秒）
	TxRate    float64 // 出口带宽速率（字节/秒）

	// 连接数（整机口径，读内核连接表；非 Linux 平台 ConnOK=false 表示不支持）
	ConnTCPEstablished int  // TCP 已建立连接数
	ConnTCPTotal       int  // TCP 表项总数（不含 LISTEN，含 TIME_WAIT 等残留）
	ConnUDP            int  // UDP 绑定 socket 数（不是会话数）
	ConnOK             bool // 本平台是否支持采集连接数
}

// Collector 通过 /proc 采集。
type Collector struct {
	mu          sync.Mutex
	prevIdle    float64
	prevTotal   float64
	havePrev    bool
	prevCPUTime time.Time
	lastCPU     float64 // 最近一次有效 CPU 值：短窗口调用直接复用它，不重算差分
	prevRx      uint64
	prevTx      uint64
	prevNetTime time.Time
	haveNet     bool
	lastRxRate  float64 // 最近一次有效网速（同上）
	lastTxRate  float64

	// 1秒级网速高频采样（防止长心跳窗口平均除法稀释瞬时尖峰）
	stopCh     chan struct{}
	instRxRate float64 // 最近 1 秒真实瞬时下行速率（字节/秒）
	instTxRate float64 // 最近 1 秒真实瞬时上行速率（字节/秒）
	peakRxRate float64 // 两次 Snapshot 之间的 1 秒最高下行峰值
	peakTxRate float64 // 两次 Snapshot 之间的 1 秒最高上行峰值
	lastSecRx  uint64
	lastSecTx  uint64
	lastSecAt  time.Time
	haveSec    bool

	// 连接数缓存（独立锁：采样在 mu 之外进行，避免拖住 1 秒级网速采样）。
	// 心跳最低可配到 3s，而繁忙节点的 /proc/net/tcp 可达数 MB，故按 connSampleInterval 抽样。
	connMu    sync.Mutex
	connAt    time.Time
	connEst   int
	connTotal int
	connUDP   int
	connOK    bool
}

// connSampleInterval 连接数最小采样间隔。连接规模是慢变量，10s 足够，
// 但能把「3s 心跳 × 几 MB 的 /proc 表」的读取压力降到 1/3。
const connSampleInterval = 10 * time.Second

// New 构造采集器。
func New() *Collector {
	c := &Collector{
		stopCh: make(chan struct{}),
	}
	go c.sampleLoop()
	return c
}

// Close 停止后台高频采样。
func (c *Collector) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopCh != nil {
		select {
		case <-c.stopCh:
		default:
			close(c.stopCh)
		}
	}
}

// sampleLoop 1秒级物理网卡采样循环（只读内存伪文件 /proc/net/dev，开销约 5 微秒）。
func (c *Collector) sampleLoop() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-c.stopCh:
			return
		case <-ticker.C:
			rx, tx, ok := netDevInfo()
			if !ok {
				continue
			}
			c.mu.Lock()
			now := time.Now()
			if c.haveSec && !c.lastSecAt.IsZero() {
				dt := now.Sub(c.lastSecAt).Seconds()
				if dt > 0 {
					if rx >= c.lastSecRx {
						r := float64(rx-c.lastSecRx) / dt
						c.instRxRate = r
						if r > c.peakRxRate {
							c.peakRxRate = r
						}
					} else {
						c.instRxRate = 0
					}
					if tx >= c.lastSecTx {
						r := float64(tx-c.lastSecTx) / dt
						c.instTxRate = r
						if r > c.peakTxRate {
							c.peakTxRate = r
						}
					} else {
						c.instTxRate = 0
					}
				}
			}
			c.lastSecRx = rx
			c.lastSecTx = tx
			c.lastSecAt = now
			c.haveSec = true
			c.mu.Unlock()
		}
	}
}

// minDeltaWindow 差分类指标（CPU / 网速）的最短有效采样窗口。
//
// 低于它时 /proc/stat 的 jiffy 量化会主导结果：2 vCPU / 100Hz 下 1ms 只推进 0.2 个 jiffy，
// dTotal 只能是 0 或 1，而推进的那 1 个 jiffy 往往正是采样器自己刚烧掉的忙 tick ——
// (1 - 0/1)*100 直接把 CPU 报成 100%（dTotal=0 时则报成 0%）。
// 窗口不足时沿用上一帧的值，并且**不更新基线**，让下一个正常窗口照常对齐。
const minDeltaWindow = 1 * time.Second

// /proc 读取与时钟的接缝，仅用于测试注入（生产恒为真实实现）。
var (
	cpuTicksFn   = cpuTicks
	netDevInfoFn = netDevInfo
	nowFn        = time.Now
)

// SystemSnapshot 只采集绝对值类指标（内存 / 磁盘 / 连接数），不碰任何差分基线。
//
// 快车道（metrics_split）模式下心跳走这里：CPU 与网速的差分基线只允许
// RealtimeSnapshot 一个写入方。两个循环共用一个基线时，后到的一方窗口被压缩到
// 亚毫秒，量化误差会产出 100% / 0% 的伪值（见 minDeltaWindow）。
func (c *Collector) SystemSnapshot() Snapshot {
	snap := Snapshot{}
	snap.Mem, snap.MemTotal = memInfo()
	snap.Disk, snap.DiskTotal = diskInfo("/")
	c.fillConns(&snap)
	return snap
}

// RealtimeSnapshot 采集实时差分指标（CPU 为自上次调用以来的均值，网速结合瞬时/峰值防稀释），
// 并附带绝对值类指标。它是 CPU 与网速差分基线**唯一**的写入方。
func (c *Collector) RealtimeSnapshot() Snapshot {
	snap := Snapshot{}
	snap.Mem, snap.MemTotal = memInfo()
	snap.Disk, snap.DiskTotal = diskInfo("/")

	now := nowFn()
	c.mu.Lock()
	c.cpuDelta(now, &snap)
	c.netDelta(now, &snap)
	c.mu.Unlock()

	c.fillConns(&snap)
	return snap
}

// Snapshot 兼容入口：一次取全体指标。旧主控（未协商快车道）下心跳仍用它，
// 此时它是唯一的差分写入方，行为与拆分前一致。
func (c *Collector) Snapshot() Snapshot {
	return c.RealtimeSnapshot()
}

// fillConns 填入连接数（内部按 connSampleInterval 缓存，两个入口都调用也不会重复读表）。
func (c *Collector) fillConns(snap *Snapshot) {
	est, total, udp, ok := c.connSnapshot(time.Now())
	snap.ConnTCPEstablished = est
	snap.ConnTCPTotal = total
	snap.ConnUDP = udp
	snap.ConnOK = ok
}

// cpuDelta 计算 CPU 百分比（调用方需持有 c.mu）。
func (c *Collector) cpuDelta(now time.Time, snap *Snapshot) {
	idle, total, ok := cpuTicksFn()
	if !ok {
		snap.CPU = c.lastCPU
		return
	}
	if !c.havePrev {
		c.prevIdle, c.prevTotal, c.havePrev, c.prevCPUTime = idle, total, true, now
		snap.CPU = c.lastCPU
		return
	}
	if now.Sub(c.prevCPUTime) < minDeltaWindow {
		snap.CPU = c.lastCPU
		return
	}
	if dIdle, dTotal := idle-c.prevIdle, total-c.prevTotal; dTotal > 0 {
		cpu := (1 - dIdle/dTotal) * 100
		switch {
		case cpu < 0:
			cpu = 0
		case cpu > 100:
			cpu = 100
		}
		c.lastCPU = cpu
	}
	c.prevIdle, c.prevTotal, c.prevCPUTime = idle, total, now
	snap.CPU = c.lastCPU
}

// netDelta 计算网速（调用方需持有 c.mu）。
func (c *Collector) netDelta(now time.Time, snap *Snapshot) {
	rx, tx, ok := netDevInfoFn()
	if !ok {
		snap.RxRate, snap.TxRate = c.lastRxRate, c.lastTxRate
		return
	}
	// 累计字节是绝对值，任何窗口都可以照发
	snap.RxBytes, snap.TxBytes = rx, tx

	if !c.haveNet {
		c.prevRx, c.prevTx, c.haveNet, c.prevNetTime = rx, tx, true, now
		snap.RxRate, snap.TxRate = c.lastRxRate, c.lastTxRate
		return
	}
	if now.Sub(c.prevNetTime) < minDeltaWindow {
		// 窗口过短：不重算，也不复位峰值（否则会截断本轮的峰值跟踪）
		snap.RxRate, snap.TxRate = c.lastRxRate, c.lastTxRate
		return
	}

	dt := now.Sub(c.prevNetTime).Seconds()
	var avgRx, avgTx float64
	if rx >= c.prevRx {
		avgRx = float64(rx-c.prevRx) / dt
	}
	if tx >= c.prevTx {
		avgTx = float64(tx-c.prevTx) / dt
	}
	// 实时速率优先取瞬时高频值或峰值，避免被大窗口平均除法稀释
	snap.RxRate = math.Max(avgRx, c.instRxRate)
	snap.TxRate = math.Max(avgTx, c.instTxRate)
	if c.peakRxRate > snap.RxRate {
		snap.RxRate = c.peakRxRate
	}
	if c.peakTxRate > snap.TxRate {
		snap.TxRate = c.peakTxRate
	}
	c.lastRxRate, c.lastTxRate = snap.RxRate, snap.TxRate
	c.prevRx, c.prevTx, c.prevNetTime = rx, tx, now

	// 重置窗口峰值，开始下一个采样周期的峰值跟踪
	c.peakRxRate = 0
	c.peakTxRate = 0
}

// connSnapshot 返回缓存的连接数；距上次采样超过 connSampleInterval 才真正读 /proc。
func (c *Collector) connSnapshot(now time.Time) (est, total, udp int, ok bool) {
	c.connMu.Lock()
	defer c.connMu.Unlock()
	if !c.connAt.IsZero() && now.Sub(c.connAt) < connSampleInterval {
		return c.connEst, c.connTotal, c.connUDP, c.connOK
	}
	est, total, udp, ok = connStats()
	c.connAt = now
	c.connEst, c.connTotal, c.connUDP, c.connOK = est, total, udp, ok
	return est, total, udp, ok
}

// cpuTicks 读取 /proc/stat 的 cpu 行 ticks。
func cpuTicks() (idle, total float64, ok bool) {
	data, err := os.ReadFile("/proc/stat")
	if err != nil {
		return 0, 0, false
	}
	line := ""
	for _, l := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(l, "cpu ") {
			line = l
			break
		}
	}
	if line == "" {
		return 0, 0, false
	}
	fields := strings.Fields(line)[1:] // user nice system idle iowait irq softirq steal
	if len(fields) < 4 {
		return 0, 0, false
	}
	vals := make([]float64, len(fields))
	for i, f := range fields {
		v, err := strconv.ParseFloat(f, 64)
		if err != nil {
			return 0, 0, false
		}
		vals[i] = v
	}
	idle = vals[3] // idle
	total = 0
	for _, v := range vals {
		total += v
	}
	return idle, total, true
}

// memInfo 读取 /proc/meminfo。
func memInfo() (used, total float64) {
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0, 0
	}
	var memTotal, memAvail float64
	for _, l := range strings.Split(string(data), "\n") {
		f := strings.Fields(l)
		if len(f) < 2 {
			continue
		}
		v, _ := strconv.ParseFloat(f[1], 64)
		switch f[0] {
		case "MemTotal:":
			memTotal = v * 1024
		case "MemAvailable:":
			memAvail = v * 1024
		}
	}
	return memTotal - memAvail, memTotal
}

// netDevInfo 读取 /proc/net/dev 汇总物理网卡流量。
func netDevInfo() (rxBytes, txBytes uint64, ok bool) {
	data, err := os.ReadFile("/proc/net/dev")
	if err != nil {
		return 0, 0, false
	}
	rx, tx := parseNetDevData(string(data))
	return rx, tx, true
}

// parseNetDevData 解析 /proc/net/dev 文本，过滤虚拟网卡。
func parseNetDevData(content string) (rxBytes, txBytes uint64) {
	for _, line := range strings.Split(content, "\n") {
		idx := strings.Index(line, ":")
		if idx == -1 {
			continue
		}
		iface := strings.TrimSpace(line[:idx])
		// 过滤回环与虚拟容器网卡
		if iface == "lo" || strings.HasPrefix(iface, "docker") || strings.HasPrefix(iface, "br-") ||
			strings.HasPrefix(iface, "veth") || strings.HasPrefix(iface, "tun") || strings.HasPrefix(iface, "tap") {
			continue
		}
		fields := strings.Fields(line[idx+1:])
		if len(fields) < 9 {
			continue
		}
		rx, err1 := strconv.ParseUint(fields[0], 10, 64)
		tx, err2 := strconv.ParseUint(fields[8], 10, 64)
		if err1 == nil && err2 == nil {
			rxBytes += rx
			txBytes += tx
		}
	}
	return rxBytes, txBytes
}
