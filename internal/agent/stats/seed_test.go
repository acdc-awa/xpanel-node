package stats

// 回归用例（2026-09-21）：缓存必须按「xray 正在跑的配置」重建，而不是清空。
// 背景（docs/architecture/配置下发-热更冷更与状态机.md §1.2 P1，实测复现）：
// 缓存是移除用户的唯一依据，旧实现每次冷更 / 自愈都把它清空，于是紧随其后的第一次同步
// 一个都摘不掉；添加分支再把 payload 回填进缓存，分歧从此对后续每一次同步都不可见。
//
// 假 gRPC HandlerService 服务端按实测的 xray 错误矩阵实现（重复邮箱 → already exists；
// 不存在的 tag → handler not found；删除不存在用户 → not found），跑的是真实
// Collector.SyncUsers 代码路径。

import (
	"context"
	"fmt"
	"net"
	"strings"
	"testing"

	handlerService "github.com/xtls/xray-core/app/proxyman/command"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/proxy/vless"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/acdc-awa/xpanel-node/pkg/protocol"
)

type fakeXray struct {
	handlerService.UnimplementedHandlerServiceServer
	users map[string]map[string]string // tag -> email -> uuid（= xray 进程内的真实用户集）
	ops   []string
}

func newFakeXray() *fakeXray {
	return &fakeXray{users: map[string]map[string]string{}}
}

// seed 预置 xray 里的用户集（模拟「xray 按这份配置启动」）。
func (f *fakeXray) seed(tag string, emails ...string) {
	m := map[string]string{}
	for i, e := range emails {
		m[e] = fmt.Sprintf("uuid-%s-%d", tag, i)
	}
	f.users[tag] = m
}

func (f *fakeXray) AlterInbound(_ context.Context, req *handlerService.AlterInboundRequest) (*handlerService.AlterInboundResponse, error) {
	tag := req.Tag
	if _, ok := f.users[tag]; !ok {
		f.ops = append(f.ops, "tag="+tag+" -> ERROR handler not found")
		return nil, fmt.Errorf("app/proxyman/command: failed to get handler: %s > app/proxyman/inbound: handler not found: %s", tag, tag)
	}
	url := req.Operation.GetType()
	switch {
	case strings.HasSuffix(url, "AddUserOperation"):
		op := &handlerService.AddUserOperation{}
		if err := proto.Unmarshal(req.Operation.GetValue(), op); err != nil {
			return nil, err
		}
		email := op.User.GetEmail()
		id := ""
		acc := &vless.Account{}
		if op.User.GetAccount() != nil {
			if err := proto.Unmarshal(op.User.GetAccount().GetValue(), acc); err == nil {
				id = acc.GetId()
			}
		}
		if _, ok := f.users[tag][email]; ok {
			f.ops = append(f.ops, fmt.Sprintf("AddUser(tag=%s,%s) -> ERROR already exists", tag, email))
			return nil, fmt.Errorf("proxy/vless: User %s already exists.", email)
		}
		f.users[tag][email] = id
		f.ops = append(f.ops, fmt.Sprintf("AddUser(tag=%s,%s) -> OK", tag, email))
		return &handlerService.AlterInboundResponse{}, nil
	case strings.HasSuffix(url, "RemoveUserOperation"):
		op := &handlerService.RemoveUserOperation{}
		if err := proto.Unmarshal(req.Operation.GetValue(), op); err != nil {
			return nil, err
		}
		if _, ok := f.users[tag][op.GetEmail()]; !ok {
			f.ops = append(f.ops, fmt.Sprintf("RemoveUser(tag=%s,%s) -> ERROR not found", tag, op.GetEmail()))
			return nil, fmt.Errorf("proxy/vless: User %s not found.", op.GetEmail())
		}
		delete(f.users[tag], op.GetEmail())
		f.ops = append(f.ops, fmt.Sprintf("RemoveUser(tag=%s,%s) -> OK", tag, op.GetEmail()))
		return &handlerService.AlterInboundResponse{}, nil
	}
	return nil, fmt.Errorf("unexpected operation %s", url)
}

func startFakeXray(t *testing.T, f *fakeXray) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	handlerService.RegisterHandlerServiceServer(srv, f)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

func testUser(email string) protocol.User {
	return protocol.User{Email: email, UUID: "11111111-1111-1111-1111-111111111111"}
}

// configJSON 拼一份最小 xray 配置：user 入站（面板用户）+ relay 入站（内部账户）。
const testConfigJSON = `{
  "inbounds": [
    {
      "tag": "in-A",
      "port": 10001,
      "protocol": "vless",
      "settings": {"clients": [
        {"id": "uuid-a1", "email": "u1.i10@panel.local", "flow": "xtls-rprx-vision"},
        {"id": "uuid-a2", "email": "u2.i10@panel.local"}
      ]}
    },
    {
      "tag": "relay-in",
      "port": 10002,
      "protocol": "vless",
      "settings": {"clients": [{"id": "uuid-r1", "email": "relay-relay-in@panel.local"}]}
    },
    {"tag": "api", "port": 10085, "protocol": "dokodemo-door", "settings": {}}
  ]
}`

// TestSeedUsersParsesPanelUsersOnly 只收面板用户邮箱：relay 内部账户不得进缓存
// （进了缓存，SyncUsers 末尾的 tag 清理会把它从运行中的 xray 摘掉，转发鉴权当场失效）。
func TestSeedUsersParsesPanelUsersOnly(t *testing.T) {
	c := New("127.0.0.1:0")
	if err := c.SeedUsers([]byte(testConfigJSON)); err != nil {
		t.Fatalf("SeedUsers: %v", err)
	}
	inA := c.currentUsers["in-A"]
	if len(inA) != 2 {
		t.Fatalf("in-A 应有 2 个用户，实际 %d: %v", len(inA), inA)
	}
	if got := inA["u1.i10@panel.local"]; got.UUID != "uuid-a1" || got.Flow != "xtls-rprx-vision" {
		t.Fatalf("u1 解析错误: %+v", got)
	}
	if _, ok := c.currentUsers["relay-in"]; ok {
		t.Fatalf("relay 内部账户不得进缓存: %v", c.currentUsers["relay-in"])
	}
	if _, ok := c.currentUsers["api"]; ok {
		t.Fatalf("无 clients 的入站不应建键: %v", c.currentUsers["api"])
	}
}

// TestSeedUsersKeepsCacheOnParseFailure 解析失败必须保留旧缓存并报错（清空正是要修的 bug）。
func TestSeedUsersKeepsCacheOnParseFailure(t *testing.T) {
	c := New("127.0.0.1:0")
	if err := c.SeedUsers([]byte(testConfigJSON)); err != nil {
		t.Fatalf("SeedUsers: %v", err)
	}
	if err := c.SeedUsers([]byte("{ 这不是 JSON")); err == nil {
		t.Fatal("坏配置必须返回错误")
	}
	if len(c.currentUsers["in-A"]) != 2 {
		t.Fatalf("解析失败后旧缓存应原样保留，实际: %v", c.currentUsers)
	}
}

// TestSeedThenFirstSyncRemovesUser 修好的核心场景：冷更（按配置重建缓存）之后的第一次同步
// 必须能摘掉「xray 有、payload 没有」的用户。旧实现（ResetUsers 清空）在这里摘不掉，
// 且再同步一次也不会自愈——操作序列为空，分歧对后续同步永久不可见。
func TestSeedThenFirstSyncRemovesUser(t *testing.T) {
	ctx := context.Background()
	f := newFakeXray()
	f.seed("in-A", "u1.i10@panel.local", "u2.i10@panel.local") // 冷推把两个用户灌进 xray
	c := New(startFakeXray(t, f))
	if err := c.SeedUsers([]byte(testConfigJSON)); err != nil {
		t.Fatalf("SeedUsers: %v", err)
	}
	// 下一次热更：u2 已被封禁 / 超量 / 过期 → payload 里没有它
	if err := c.SyncUsers(ctx, map[string][]protocol.User{
		"in-A": {testUser("u1.i10@panel.local")},
	}); err != nil {
		t.Fatalf("同步失败: %v", err)
	}
	if _, ok := f.users["in-A"]["u2.i10@panel.local"]; ok {
		t.Fatalf("u2 应被摘掉，操作序列: %v", f.ops)
	}
	if _, ok := f.users["in-A"]["u1.i10@panel.local"]; !ok {
		t.Fatalf("u1 不应被摘掉，操作序列: %v", f.ops)
	}
}

// TestSyncUsersKeepsRelayInternalAccount relay 内部账户不在 payload 里（GetValidUsers 只发
// user 入站），重建缓存之后也不能被 tag 清理摘掉——否则转发链路当场断。
func TestSyncUsersKeepsRelayInternalAccount(t *testing.T) {
	ctx := context.Background()
	f := newFakeXray()
	f.seed("in-A", "u1.i10@panel.local", "u2.i10@panel.local")
	f.seed("relay-in", "relay-relay-in@panel.local")
	c := New(startFakeXray(t, f))
	if err := c.SeedUsers([]byte(testConfigJSON)); err != nil {
		t.Fatalf("SeedUsers: %v", err)
	}
	if err := c.SyncUsers(ctx, map[string][]protocol.User{
		"in-A": {testUser("u1.i10@panel.local")},
	}); err != nil {
		t.Fatalf("同步失败: %v", err)
	}
	if _, ok := f.users["relay-in"]["relay-relay-in@panel.local"]; !ok {
		t.Fatalf("relay 内部账户被误摘: %v（操作序列 %v）", f.users["relay-in"], f.ops)
	}
}

// TestSeedUsersFromFileMissing 文件不存在时返回错误且不动缓存（开机兜底路径的防御）。
func TestSeedUsersFromFileMissing(t *testing.T) {
	c := New("127.0.0.1:0")
	if err := c.SeedUsers([]byte(testConfigJSON)); err != nil {
		t.Fatalf("SeedUsers: %v", err)
	}
	if err := c.SeedUsersFromFile(t.TempDir() + "/nope.json"); err == nil {
		t.Fatal("文件不存在必须返回错误")
	}
	if len(c.currentUsers["in-A"]) != 2 {
		t.Fatalf("读文件失败后旧缓存应原样保留，实际: %v", c.currentUsers)
	}
}

var _ = serial.ToTypedMessage // 保持与探针一致的导入面（proto 序列化路径）
