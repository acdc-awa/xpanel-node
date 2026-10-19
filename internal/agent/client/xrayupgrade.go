package client

import (
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/acdc-awa/xpanel-node/internal/agent/upgrade"
	"github.com/acdc-awa/xpanel-node/pkg/protocol"
)

// handleUpgradeXray 面板触发的 xray 内核替换：与 agent 自升级一样放到后台 goroutine
// （下载可能持续数分钟，不能阻塞 WebSocket 读循环，否则心跳/pong 停摆会被主控回收）。
func (c *Client) handleUpgradeXray(m *protocol.Message) *protocol.ResultPayload {
	if c.XrayUpgrade == nil || c.Xray == nil {
		return &protocol.ResultPayload{OK: false, Error: "服务器未配置 xray 内核升级源"}
	}
	var p protocol.UpgradeXrayPayload
	if err := m.PayloadTo(&p); err != nil {
		return &protocol.ResultPayload{OK: false, Error: "解析 upgrade_xray 失败: " + err.Error()}
	}
	if !c.upgrading.CompareAndSwap(false, true) {
		return &protocol.ResultPayload{OK: false, Error: "升级正在进行中，请稍候"}
	}
	if !c.upgradingXray.CompareAndSwap(false, true) {
		c.upgrading.Store(false)
		return &protocol.ResultPayload{OK: false, Error: "xray 内核升级正在进行中，请稍候"}
	}
	go c.runUpgradeXray(m.ID, p.Target, p.Force)
	return nil // 回执由 runUpgradeXray 完成
}

// runUpgradeXray 完整流程：
// 探测当前版本 → 下载（sha256 校验 + zip 解包）→ 独立探测新二进制 → 停止 xray →
// 备份并原子替换 → 拉起 → 失败则用 .prev 回滚并尽力恢复服务。
//
// 顺序上的两个关键点：
//  1. 新二进制必须在**停掉 xray 之前**完成探测（跑 `version`），否则下载到坏包会先造成停服；
//  2. 替换后拉起失败要回滚——xray 的 "-test 通过 ≠ 能跑"（端口占用等），只有真的起来才算成功。
func (c *Client) runUpgradeXray(reqID, target string, force bool) {
	defer c.upgrading.Store(false)
	defer c.upgradingXray.Store(false)

	report := func(phase, msg, errStr string) {
		_ = c.send(protocol.MsgUpgradeProgress, "", protocol.UpgradeProgressPayload{
			Phase:   phase,
			Target:  target,
			Message: msg,
			Error:   errStr,
			Kind:    "xray",
			TS:      time.Now().Unix(),
		})
	}
	reply := func(res protocol.ResultPayload) {
		if err := c.send(protocol.MsgResult, reqID, res); err != nil {
			log.Printf("agent: upgrade_xray 回执发送失败: %v", err)
		}
	}
	fail := func(msg, errText string) {
		report("failed", msg, errText)
		reply(protocol.ResultPayload{OK: false, Error: errText})
	}

	binPath := c.Xray.Bin
	if binPath == "" {
		fail("未配置 xray 二进制路径", "配置缺失 xray.bin")
		return
	}

	report("checking", "正在探测当前 xray 内核版本…", "")
	from, err := upgrade.XrayBinVersion(binPath)
	if err != nil {
		// 探测失败不阻断升级：节点上 xray 可能已损坏，正是需要替换的场景。
		log.Printf("agent: 当前 xray 版本探测失败（继续升级）: %v", err)
		from = ""
	}
	fromLabel := from
	if fromLabel == "" {
		fromLabel = "未知"
	}

	target = strings.TrimSpace(target)
	if target != "" && !upgrade.TagPattern.MatchString(target) {
		fail("目标版本号格式不正确", "目标版本号格式不正确: "+target)
		return
	}
	if target == "" {
		report("checking", "正在查询 XTLS/Xray-core 最新版本…", "")
		latest, lerr := c.XrayUpgrade.Latest()
		if lerr != nil {
			fail("查询最新 xray 版本失败", "查询最新 xray 版本失败: "+lerr.Error())
			return
		}
		target = latest
	}

	if from != "" {
		cmp := upgrade.Compare(from, target)
		if !force && cmp >= 0 {
			msg := fmt.Sprintf("xray 已是 %s（目标 %s），无需升级", from, target)
			report("success", msg, "")
			reply(protocol.ResultPayload{OK: true, Data: msg})
			return
		}
		if force && cmp == 0 {
			// force 同版本仍然继续：用于"内核文件损坏/被替换"的强制修复
			report("checking", fmt.Sprintf("强制重装 xray %s", target), "")
		}
	}

	report("downloading", fmt.Sprintf("正在下载 xray %s（当前 %s）…", target, fromLabel), "")
	bin, sha, err := c.XrayUpgrade.XrayDownload(target)
	if err != nil {
		fail("下载 xray 内核失败", "下载 xray 内核失败: "+err.Error())
		return
	}
	log.Printf("agent: xray %s 下载完成（sha256=%s，%d 字节）", target, sha, len(bin))

	report("verifying", "正在校验下载的 xray 内核可执行性…", "")
	if err := upgrade.VerifyXrayBinary(bin, target); err != nil {
		fail("下载的 xray 内核不可用", "下载的 xray 内核不可用: "+err.Error())
		return
	}

	report("replacing", "正在停止 xray 并替换内核…", "")
	wasRunning := c.Xray.IsRunning()
	if err := c.Xray.Stop(); err != nil {
		log.Printf("agent: 停止 xray 失败（继续替换，可能已有残留进程）: %v", err)
	}
	prev, err := upgrade.ReplaceXrayBinary(binPath, bin)
	if err != nil {
		// 替换失败：尽力把服务拉回来
		if wasRunning {
			if serr := c.Xray.Start(); serr != nil {
				log.Printf("agent: 替换失败后重启 xray 也失败: %v", serr)
			}
		}
		fail("替换 xray 内核失败", "替换 xray 内核失败: "+err.Error())
		return
	}
	log.Printf("agent: xray 内核已替换 %s → %s（%s，备份 %s）", fromLabel, target, binPath, prev)

	// 替换后重新探测：确认落盘的就是目标版本（防止解包内容与声明不符）
	nowVersion, verr := upgrade.XrayBinVersion(binPath)
	if verr != nil {
		_ = upgrade.RollbackXrayBinary(binPath, prev)
		if wasRunning {
			_ = c.Xray.Start()
		}
		fail("替换后的 xray 无法执行，已回滚", "替换后的 xray 无法执行: "+verr.Error())
		return
	}
	c.xrayVersion.Store(nowVersion)

	report("restarting", fmt.Sprintf("内核已替换为 %s，正在拉起 xray…", nowVersion), "")
	c.Xray.ResetFailures()
	if err := c.Xray.Start(); err != nil {
		// 拉起失败：回滚内核并再试一次，避免节点直接宕机
		rbErr := upgrade.RollbackXrayBinary(binPath, prev)
		log.Printf("agent: 新内核拉起失败，回滚: %v（回滚错误: %v）", err, rbErr)
		restoreOk := false
		if rbErr == nil {
			if prevVer, e := upgrade.XrayBinVersion(binPath); e == nil {
				c.xrayVersion.Store(prevVer)
			}
			c.Xray.ResetFailures()
			if serr := c.Xray.Start(); serr == nil {
				restoreOk = true
			}
		}
		if restoreOk {
			fail("新内核无法启动，已回滚到原版本",
				fmt.Sprintf("xray %s 无法启动（%v），已回滚到 %s 并恢复服务", target, err, fromLabel))
			return
		}
		fail("新内核无法启动且回滚未恢复服务",
			fmt.Sprintf("xray %s 无法启动（%v），回滚后仍无法启动（%v），请人工介入", target, err, rbErr))
		return
	}

	msg := fmt.Sprintf("xray 内核已升级 %s → %s", fromLabel, nowVersion)
	report("success", msg, "")
	reply(protocol.ResultPayload{OK: true, Data: msg})
	log.Printf("agent: %s", msg)
}

// initXrayVersion 在启动时探测一次内核版本并缓存；失败只记日志（不阻断 agent 启动）。
func (c *Client) initXrayVersion() {
	if c.Xray == nil || c.Xray.Bin == "" {
		return
	}
	v, err := upgrade.XrayBinVersion(c.Xray.Bin)
	if err != nil {
		log.Printf("agent: 探测 xray 内核版本失败（心跳将不带版本）: %v", err)
		return
	}
	c.xrayVersion.Store(v)
	log.Printf("agent: 当前 xray 内核版本 %s", v)
}
