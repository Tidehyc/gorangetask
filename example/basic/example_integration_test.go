package basic_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	gomysql "github.com/go-sql-driver/mysql"

	gorangetask "github.com/Tidehyc/gorangetask"
	"github.com/Tidehyc/gorangetask/example/basic"
	mysqlstore "github.com/Tidehyc/gorangetask/store/mysql"
)

// 这些用例需要一个可丢弃的 MySQL：
//
//	export GOTASK_TEST_MYSQL_DSN='root:rootpw@tcp(127.0.0.1:3306)/gotask?parseTime=true&loc=UTC&timeout=5s&readTimeout=30s&writeTimeout=30s'
//
// 未设置时全部跳过，因此普通的 go test ./... 不受影响。
//
// 用例只经过公开 API（basic.Open / Store.CreateOrder / Client.Get …）断言行为，
// 因为它们要证明的正是"使用方按文档接进来就能拿到同事务语义"这件事，而不是
// 示例内部某个函数长什么样。唯一的例外是造"订单处于意外状态"时直接 UPDATE 一行，
// 那是模拟"别的东西动过这行"，公开 API 不该有这种能力。
//
// 数据库隔离：本包不直接用 DSN 里的库，而是用派生库 <库名>_example，并在需要时
// 自动创建。集成测试的 setup 是破坏性的（清表），而 go test ./... 会并发运行不同
// 的包：与 store/mysql（<库名>_store）或 migrations/mysql（直接用 <库名>）共用
// 一个库，两边会互相删表，报出与产品无关的假失败。
const (
	testDSNEnv = "GOTASK_TEST_MYSQL_DSN"

	// testDBPrefix 是派生库名后缀。每个集成测试包应使用不同的后缀。
	testDBPrefix = "_example"

	// workerStopDeadline 是等待 Run 返回的上限。它只是兜底，防止用例挂死：
	// 正常停机在毫秒级完成，调大它不会让慢的用例变快。
	workerStopDeadline = 30 * time.Second
)

// exampleTables 是示例会碰到的三张表：库自己的任务表与迁移表，加上示例的业务表。
// 名字写死字面量——它们是"示例跑起来会创建哪些表"这个事实，不是实现的内部细节。
var exampleTables = []string{"gotask_tasks", "gotask_schema_migrations", "example_orders"}

// fastConfig 把所有节拍调快，让"领取 → 执行 → 租约过期 → 回收"这条链在秒级内走完。
//
// CleanupInterval 显式关掉（负值＝关闭清理器）：清理器会删掉终态任务，而下面要读回
// succeeded 的任务做断言；它还参与 Run 的停机等待，开着只会给停机用例添噪声。
func fastConfig() gorangetask.Config {
	return gorangetask.Config{
		Queues:            []string{basic.Queue},
		Concurrency:       2,
		PollInterval:      50 * time.Millisecond,
		MaxPollInterval:   200 * time.Millisecond,
		BatchSize:         2,
		LeaseDuration:     2 * time.Second,
		HeartbeatInterval: 500 * time.Millisecond,
		ReapInterval:      200 * time.Millisecond,
		ShutdownTimeout:   2 * time.Second,
		HandlerTimeout:    10 * time.Second,
		CleanupInterval:   -1,
	}
}

// startTestApp 清库并装配 App，同时返回它用的 DSN。
//
// 返回 DSN 是为了让"停机后换个 Worker 接手"这类用例能在同一个库上重新装配一个
// 配置不同的 App（那一段需要把 Handler 的延迟改回 0）。
func startTestApp(t *testing.T, opts basic.Options) (*basic.App, string) {
	t.Helper()
	dsn := resetTestDatabase(t)
	return openTestApp(t, dsn, opts), dsn
}

// openTestApp 在已有库上装配 App，并在用例结束时关掉它。
func openTestApp(t *testing.T, dsn string, opts basic.Options) *basic.App {
	t.Helper()

	app, err := basic.Open(context.Background(), dsn, opts)
	if err != nil {
		t.Fatalf("open example app: %v", err)
	}
	t.Cleanup(func() { _ = app.Close() })
	return app
}

// resetTestDatabase 派生本包专属的库、清空示例用到的表，返回指向它的 DSN。
func resetTestDatabase(t *testing.T) string {
	t.Helper()

	raw := os.Getenv(testDSNEnv)
	if raw == "" {
		t.Skipf("%s not set; skipping MySQL integration test", testDSNEnv)
	}
	parsed, err := gomysql.ParseDSN(raw)
	if err != nil {
		t.Fatalf("parse %s: %v", testDSNEnv, err)
	}
	if parsed.DBName == "" {
		t.Fatalf("%s must include a database name, e.g. .../gotask", testDSNEnv)
	}
	// 断言里会把 DATETIME 扫成 time.Time，因此强制开启 parseTime，
	// 不要求调用方在 DSN 里记得写。
	parsed.ParseTime = true

	target := parsed.DBName + testDBPrefix
	if err := ensureDatabase(parsed, target); err != nil {
		t.Fatalf("ensure database %s: %v", target, err)
	}

	cfg := *parsed
	cfg.DBName = target
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatalf("open %s: %v", target, err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping %s: %v", target, err)
	}
	// basic.Open 只建表、不清表：上一轮跑剩的行会让"数行数"的断言莫名其妙地失败。
	for _, table := range exampleTables {
		if _, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS "+table); err != nil {
			t.Fatalf("drop table %s: %v", table, err)
		}
	}
	return cfg.FormatDSN()
}

// ensureDatabase 创建派生测试库。它会连到"不指定库"的连接上：
// CREATE DATABASE 需要服务器级权限，不能在目标库自己的连接里执行。
func ensureDatabase(parsed *gomysql.Config, name string) error {
	server := *parsed
	server.DBName = ""

	db, err := sql.Open("mysql", server.FormatDSN())
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	_, err = db.ExecContext(context.Background(),
		"CREATE DATABASE IF NOT EXISTS `"+name+"` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci")
	return err
}

// stopWorker 取消 Worker 并等它在上限内返回。
func stopWorker(t *testing.T, stop context.CancelFunc, done <-chan error) error {
	t.Helper()
	stop()
	select {
	case err := <-done:
		return err
	case <-time.After(workerStopDeadline):
		t.Fatalf("worker did not stop within %s", workerStopDeadline)
		return nil
	}
}

// eventually 轮询 cond 直到成立或超时。用它而不是固定 time.Sleep：Worker 的推进
// 速度取决于调度与数据库，写死的等待要么不够（flake）要么过长（慢）。
func eventually(t *testing.T, deadline time.Duration, what string, cond func() (bool, error)) {
	t.Helper()

	stop := time.Now().Add(deadline)
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

	var lastErr error
	for {
		ok, err := cond()
		if err != nil {
			lastErr = err
		}
		if ok {
			return
		}
		if time.Now().After(stop) {
			if lastErr != nil {
				t.Fatalf("timed out after %s waiting for %s; last error: %v", deadline, what, lastErr)
			}
			t.Fatalf("timed out after %s waiting for %s", deadline, what)
		}
		<-ticker.C
	}
}

// TestExampleCreateOrderCommitsOrderAndTaskTogether 覆盖示例的第一半承诺：
// 提交则业务行与任务行一起可见。
func TestExampleCreateOrderCommitsOrderAndTaskTogether(t *testing.T) {
	app, _ := startTestApp(t, basic.Options{})
	ctx := context.Background()

	orderID, taskID, err := app.Store.CreateOrder(ctx, "alice", 1999)
	if err != nil {
		t.Fatalf("CreateOrder: %v", err)
	}

	order, err := app.Store.GetOrder(ctx, orderID)
	if err != nil {
		t.Fatalf("GetOrder(%d): %v", orderID, err)
	}
	// 期望值写死字面量，不引用 basic.OrderPending：引用常量的话，常量一改断言
	// 就跟着一起改，等于什么都没测。
	if order.Status != "pending" {
		t.Errorf("order %d status = %q, want %q", orderID, order.Status, "pending")
	}
	if order.ExportedAt.Valid {
		t.Errorf("order %d already has exported_at = %v, want NULL", orderID, order.ExportedAt.Time)
	}
	if order.Customer != "alice" || order.AmountCents != 1999 {
		t.Errorf("order %d = {%s, %d}, want {alice, 1999}", orderID, order.Customer, order.AmountCents)
	}

	info, err := app.Client.Get(ctx, taskID)
	if err != nil {
		t.Fatalf("Get(%d): %v", taskID, err)
	}
	if info.Queue != "example.orders" {
		t.Errorf("task queue = %q, want %q", info.Queue, "example.orders")
	}
	if info.Type != "order.export" {
		t.Errorf("task type = %q, want %q", info.Type, "order.export")
	}
	if info.Status != gorangetask.StatusPending {
		t.Errorf("task status = %q, want %q", info.Status, gorangetask.StatusPending)
	}

	// 示例走的是"任务只携带业务主键"这条常见做法：payload 里必须有订单 ID，
	// 否则 Handler 拿到任务也不知道该导出哪一笔。
	var payload struct {
		OrderID int64 `json:"order_id"`
	}
	if err := json.Unmarshal(info.Payload, &payload); err != nil {
		t.Fatalf("decode task payload %s: %v", info.Payload, err)
	}
	if payload.OrderID != orderID {
		t.Errorf("task payload order_id = %d, want %d", payload.OrderID, orderID)
	}
}

// TestExampleFailedEnqueueRollsBackTheOrder 覆盖示例的另一半承诺：
// 任务行写不进去时，业务行也不能留下。
//
// 触发方式刻意选在公开配置上：把 ClientConfig.MaxPayloadSize 调小到装不下任何
// 导出 payload，EnqueueTx 会在插入任务行之前就拒绝，CreateOrder 随后的
// defer Rollback 必须把已经 INSERT 的订单行一起撤掉。
func TestExampleFailedEnqueueRollsBackTheOrder(t *testing.T) {
	// 4 字节装不下 {"order_id":N} 这种 payload（最短也有 14 字节）。
	app, _ := startTestApp(t, basic.Options{
		ClientConfig: gorangetask.ClientConfig{MaxPayloadSize: 4},
	})
	ctx := context.Background()

	if _, _, err := app.Store.CreateOrder(ctx, "bob", 1999); !errors.Is(err, gorangetask.ErrInvalidPayload) {
		t.Fatalf("CreateOrder error = %v, want ErrInvalidPayload", err)
	}

	orders, err := app.Store.ListOrders(ctx, 10)
	if err != nil {
		t.Fatalf("ListOrders: %v", err)
	}
	// 这一条才是重点：失败的入队不能留下孤儿订单。若把 CreateOrder 里的
	// EnqueueTx 挪到事务外执行，这里就会出现一行没人管的 pending 订单。
	if len(orders) != 0 {
		t.Fatalf("order survived a failed enqueue: %+v", orders)
	}
}

// TestExampleWorkerExportsOrders 把示例端到端跑一遍：建订单 → Worker 领取 →
// Handler 写入终态。它同时验证任务侧落到 succeeded、业务侧落到 exported。
func TestExampleWorkerExportsOrders(t *testing.T) {
	app, _ := startTestApp(t, basic.Options{Config: fastConfig()})
	ctx := context.Background()

	const orders = 3
	type pair struct {
		orderID int64
		taskID  gorangetask.TaskID
	}
	pairs := make([]pair, 0, orders)
	for i := 0; i < orders; i++ {
		orderID, taskID, err := app.Store.CreateOrder(ctx, fmt.Sprintf("customer-%d", i), int64(1000*(i+1)))
		if err != nil {
			t.Fatalf("CreateOrder #%d: %v", i, err)
		}
		pairs = append(pairs, pair{orderID: orderID, taskID: taskID})
	}

	runCtx, stopRun := context.WithCancel(ctx)
	defer stopRun()
	workerDone := make(chan error, 1)
	go func() { workerDone <- app.Run(runCtx) }()

	eventually(t, 30*time.Second, "every order to reach exported", func() (bool, error) {
		for _, p := range pairs {
			order, err := app.Store.GetOrder(ctx, p.orderID)
			if err != nil {
				return false, err
			}
			if order.Status != "exported" {
				return false, nil
			}
		}
		return true, nil
	})

	// 正常停机必须返回 nil：没有 Handler 被超时放弃，也没有回收器/清理器拖尾。
	if err := stopWorker(t, stopRun, workerDone); err != nil {
		t.Fatalf("worker returned %v on normal shutdown, want nil", err)
	}

	for _, p := range pairs {
		info, err := app.Client.Get(ctx, p.taskID)
		if err != nil {
			t.Fatalf("Get(%d): %v", p.taskID, err)
		}
		if info.Status != gorangetask.StatusSucceeded {
			t.Errorf("task %d status = %q, want %q", p.taskID, info.Status, gorangetask.StatusSucceeded)
		}
		if info.FinishedAt.IsZero() {
			t.Errorf("task %d is %s but has no finished_at", p.taskID, info.Status)
		}

		order, err := app.Store.GetOrder(ctx, p.orderID)
		if err != nil {
			t.Fatalf("GetOrder(%d): %v", p.orderID, err)
		}
		// 状态是 exported 却没有时间戳，说明 Handler 走了别的写回路径。
		if !order.ExportedAt.Valid {
			t.Errorf("order %d is exported but exported_at is NULL", p.orderID)
		}
	}
}

// TestExampleExportIsIdempotentOnRedelivery 覆盖 Handler 最容易被写错的那条性质：
// 库的语义是"至少一次"，同一条任务可能被跑第二遍。
func TestExampleExportIsIdempotentOnRedelivery(t *testing.T) {
	app, _ := startTestApp(t, basic.Options{})
	ctx := context.Background()

	orderID, taskID, err := app.Store.CreateOrder(ctx, "carol", 5000)
	if err != nil {
		t.Fatalf("CreateOrder: %v", err)
	}
	info, err := app.Client.Get(ctx, taskID)
	if err != nil {
		t.Fatalf("Get(%d): %v", taskID, err)
	}

	// 直接调 Handler，把"重复投递"从 Worker 的调度里拆出来单独验证。
	// 用库里存着的那份 payload，而不是在测试里另拼一个 JSON：两者若不一致，
	// 断言就变成在测测试自己写的字面量了。
	handler := basic.ExportOrderHandler(app.DB, 0)

	if err := handler(ctx, info.Payload); err != nil {
		t.Fatalf("first run: %v", err)
	}
	first, err := app.Store.GetOrder(ctx, orderID)
	if err != nil {
		t.Fatalf("GetOrder: %v", err)
	}
	if first.Status != "exported" || !first.ExportedAt.Valid {
		t.Fatalf("after the first run status = %q, exported_at = %v; want exported with a timestamp",
			first.Status, first.ExportedAt)
	}

	// 第二次：同一笔订单、同一份 payload。重复投递不是错误，必须成功返回——
	// 把它当错误会让任务无谓地重试到次数耗尽，最后进死信。
	if err := handler(ctx, info.Payload); err != nil {
		t.Fatalf("second run returned %v; want nil (an already-exported order is not a failure)", err)
	}

	second, err := app.Store.GetOrder(ctx, orderID)
	if err != nil {
		t.Fatalf("GetOrder: %v", err)
	}
	// 幂等的判据是"副作用只发生一次"：exported_at 不能被第二次执行改写。
	// 少了 WHERE status = 'pending' 的无条件 UPDATE 会在这里露馅。
	if !second.ExportedAt.Time.Equal(first.ExportedAt.Time) {
		t.Fatalf("exported_at moved on redelivery: %v → %v; the second run must be a no-op",
			first.ExportedAt.Time, second.ExportedAt.Time)
	}
	if second.Status != "exported" {
		t.Errorf("order status = %q after redelivery, want %q", second.Status, "exported")
	}
}

// TestExampleExportHandlerReportsMissingOrderAsPermanent 断言"订单不存在"被判成
// 永久失败：重试不会让它出现，白试到次数耗尽只是拖时间。
func TestExampleExportHandlerReportsMissingOrderAsPermanent(t *testing.T) {
	app, _ := startTestApp(t, basic.Options{})

	// 库刚被清过表，1 号订单必然不存在。
	err := basic.ExportOrderHandler(app.DB, 0)(context.Background(), json.RawMessage(`{"order_id":1}`))
	if !gorangetask.IsPermanent(err) {
		t.Fatalf("handler error = %v, want a permanent error (a retry cannot make the order appear)", err)
	}
}

// TestExampleExportHandlerReportsUnparsablePayloadAsPermanent 断言坏 payload 被判成
// 永久失败：payload 是入队时写死的，重试不会让它变得可解析。
func TestExampleExportHandlerReportsUnparsablePayloadAsPermanent(t *testing.T) {
	app, _ := startTestApp(t, basic.Options{})

	err := basic.ExportOrderHandler(app.DB, 0)(context.Background(), json.RawMessage(`{"order_id":`))
	if !gorangetask.IsPermanent(err) {
		t.Fatalf("handler error = %v, want a permanent error", err)
	}
}

// TestExampleExportHandlerTreatsUnexpectedOrderStatusAsRetryable 覆盖最不显眼的那条
// 分支：条件更新影响 0 行，回读又不是 exported，说明有别的东西动过这行。
// 这时既不能当成功（业务没被推进），也不能武断判死（它可能只是暂时不一致）。
func TestExampleExportHandlerTreatsUnexpectedOrderStatusAsRetryable(t *testing.T) {
	app, _ := startTestApp(t, basic.Options{})
	ctx := context.Background()

	orderID, taskID, err := app.Store.CreateOrder(ctx, "erin", 2500)
	if err != nil {
		t.Fatalf("CreateOrder: %v", err)
	}
	// 把订单挪到一个示例没定义的状态，制造"条件更新影响 0 行"的那种情况。
	if _, err := app.DB.ExecContext(ctx,
		"UPDATE example_orders SET status = 'void' WHERE id = ?", orderID); err != nil {
		t.Fatalf("force an unexpected status: %v", err)
	}

	info, err := app.Client.Get(ctx, taskID)
	if err != nil {
		t.Fatalf("Get(%d): %v", taskID, err)
	}

	err = basic.ExportOrderHandler(app.DB, 0)(ctx, info.Payload)
	if err == nil {
		t.Fatal("handler returned nil for an order in an unexpected status; want a retryable error")
	}
	if gorangetask.IsPermanent(err) {
		t.Errorf("handler error %v is permanent, want retryable", err)
	}
}

// TestExampleInterruptedTaskIsRequeuedAfterItsLeaseExpires 走的是"进程走了任务也
// 不会丢"那条路：停机时正在执行的任务保持 running，等租约过期后由回收器交还队列，
// 再由新的 Worker 做完。README 的故障演示与 demo 的收尾提示都指着这条路径。
func TestExampleInterruptedTaskIsRequeuedAfterItsLeaseExpires(t *testing.T) {
	ctx := context.Background()

	// —— 第一段：Handler 故意很慢，Worker 在它执行中途被停掉。——
	app1, dsn := startTestApp(t, basic.Options{Config: fastConfig()})

	orderID, taskID, err := app1.Store.CreateOrder(ctx, "dave", 7000)
	if err != nil {
		t.Fatalf("CreateOrder: %v", err)
	}

	// 停机**时机**是这段用例的关键，不能靠"库里 status 变成 running"来判断：claim
	// 提交与 Handler 启动之间有一个窗口，在窗口里取消，runOnce 会按设计跳过整批、
	// Handler 根本不启动；而任务仍然是 running、仍然会被回收器接手——终态一模一样，
	// 于是用例照样通过，却什么都没测到。（覆盖率会出卖它：那样跑的时候，
	// ExportOrderHandler 里 `delay > 0` 那几行一次都没执行。）
	//
	// 所以让 Handler 自己发信号：包一层示例的慢 Handler，一进来就敲一下 entered，
	// 测试等到这个信号才停机。延迟取远大于 ShutdownTimeout 的值，这样它只可能被
	// 取消打断、不可能自己走完。
	const slowDelay = 30 * time.Second
	entered := make(chan struct{}, 1)
	inner := basic.ExportOrderHandler(app1.DB, slowDelay)
	wrapped := func(hctx context.Context, raw json.RawMessage) error {
		select {
		case entered <- struct{}{}:
		default: // 万一被重复投递：不阻塞，也不挡住这一次执行
		}
		return inner(hctx, raw)
	}

	registry := gorangetask.NewRegistry()
	if err := registry.Register(basic.TaskTypeExportOrder, wrapped); err != nil {
		t.Fatalf("register slow handler: %v", err)
	}
	// 同上一条用例：NewWorker 要的是 WorkerStore，而 App 只暴露了 Client 接口。
	store, err := mysqlstore.New(app1.DB, gorangetask.ClientConfig{})
	if err != nil {
		t.Fatalf("build worker store: %v", err)
	}
	slowWorker, err := gorangetask.NewWorker(store, registry, fastConfig())
	if err != nil {
		t.Fatalf("NewWorker: %v", err)
	}

	runCtx, stopRun := context.WithCancel(ctx)
	defer stopRun()
	workerDone := make(chan error, 1)
	go func() { workerDone <- slowWorker.Run(runCtx) }()

	select {
	case <-entered:
	case <-time.After(30 * time.Second):
		t.Fatal("the handler never started; stopping now would interrupt nothing")
	}

	if err := stopWorker(t, stopRun, workerDone); err != nil {
		t.Fatalf("worker returned %v on normal shutdown, want nil", err)
	}

	// 停机期间不写回失败：任务保持 running、订单保持 pending，等回收器接手。
	// 写回 pending 反而更危险——旧 Handler 的业务副作用可能仍在途。
	info, err := app1.Client.Get(ctx, taskID)
	if err != nil {
		t.Fatalf("Get(%d) after shutdown: %v", taskID, err)
	}
	if info.Status != gorangetask.StatusRunning {
		t.Fatalf("task status after shutdown = %q, want %q (a shutdown-interrupted task is left to the reaper)",
			info.Status, gorangetask.StatusRunning)
	}
	order, err := app1.Store.GetOrder(ctx, orderID)
	if err != nil {
		t.Fatalf("GetOrder: %v", err)
	}
	if order.Status != "pending" {
		t.Fatalf("order status after shutdown = %q, want %q", order.Status, "pending")
	}

	// —— 第二段：同一个库、新的 Worker、Handler 不再延迟。——
	//
	// 它的回收器会发现租约已过期，把任务交还队列，然后自己把它做完。这一段不
	// 依赖第一段的任何内存状态，正是"换个进程接手"的真实形态。
	app2 := openTestApp(t, dsn, basic.Options{Config: fastConfig()})
	runCtx2, stopRun2 := context.WithCancel(ctx)
	defer stopRun2()
	workerDone2 := make(chan error, 1)
	go func() { workerDone2 <- app2.Run(runCtx2) }()

	// 等的主体是**任务**而不是订单：Handler 先改订单、Worker 随后才把任务写成
	// succeeded，中间那一段两份状态本来就可以不一致。盯着订单会在那个窗口里
	// 提前返回，读到一个还写着 running 的任务。
	eventually(t, 60*time.Second, "the second worker to finish the requeued task", func() (bool, error) {
		info, err := app2.Client.Get(ctx, taskID)
		if err != nil {
			return false, err
		}
		return info.Status == gorangetask.StatusSucceeded, nil
	})

	if err := stopWorker(t, stopRun2, workerDone2); err != nil {
		t.Fatalf("second worker returned %v on normal shutdown, want nil", err)
	}

	order, err = app2.Store.GetOrder(ctx, orderID)
	if err != nil {
		t.Fatalf("GetOrder: %v", err)
	}
	if order.Status != "exported" {
		t.Errorf("order status = %q, want %q", order.Status, "exported")
	}

	info, err = app2.Client.Get(ctx, taskID)
	if err != nil {
		t.Fatalf("Get(%d): %v", taskID, err)
	}
	if info.Status != gorangetask.StatusSucceeded {
		t.Errorf("task status = %q, want %q", info.Status, gorangetask.StatusSucceeded)
	}
	// 尝试次数至少 2，证明确实发生了"重新领取"：任务是被回收器交还后又跑了一遍，
	// 而不是第一次执行其实没被打断。
	if info.Attempt < 2 {
		t.Errorf("task attempt = %d, want >= 2 (the task must have been re-delivered after the lease expired)", info.Attempt)
	}
}

// TestExampleExportSideEffectHappensOnceAcrossAFailedAck 注入"副作用已发生、写回
// 之前失败"这一种故障（进程在 ack 前退出、panic、或写回本身失败都长这样）：
// Handler 已经把订单改成 exported，任务却没写成 succeeded，于是被重试。第二次执行
// 必须幂等命中，业务效果只发生一次。
//
// 它与 TestExampleExportIsIdempotentOnRedelivery 的分工：那条直接调两次 Handler，
// 只测幂等本身；这条把"重复执行"交给库的重试机制产生，测的是两者**组合**起来对不对
// ——库单独正确、示例单独正确，合在一起仍然可能错。这正是示例存在的意义。
func TestExampleExportSideEffectHappensOnceAcrossAFailedAck(t *testing.T) {
	app, _ := startTestApp(t, basic.Options{})
	ctx := context.Background()

	orderID, taskID, err := app.Store.CreateOrder(ctx, "frank", 3300)
	if err != nil {
		t.Fatalf("CreateOrder: %v", err)
	}

	// 包一层示例自己的 Handler：第一次把副作用做完之后失败，第二次放行。
	// count 用 mutex 保护：两次执行由不同的 goroutine 完成，靠库里的状态传递
	// 时序关系，Go 自己是看不见这条 happens-before 的。
	inner := basic.ExportOrderHandler(app.DB, 0)
	var (
		mu       sync.Mutex
		calls    int
		exported []time.Time
	)
	wrapped := func(hctx context.Context, raw json.RawMessage) error {
		if err := inner(hctx, raw); err != nil {
			return err
		}

		// 每次都把当时的 exported_at 记下来，用它断言第二次没有改写业务时间戳。
		order, err := app.Store.GetOrder(hctx, orderID)
		if err != nil {
			return err
		}

		mu.Lock()
		calls++
		exported = append(exported, order.ExportedAt.Time)
		n := calls
		mu.Unlock()

		if n == 1 {
			// 副作用已经落库，但"写回之前"失败了。不带永久标记，让库去重试。
			return errors.New("simulated failure between the side effect and the ack")
		}
		return nil
	}

	registry := gorangetask.NewRegistry()
	if err := registry.Register(basic.TaskTypeExportOrder, wrapped); err != nil {
		t.Fatalf("register wrapped handler: %v", err)
	}
	// 自己造一个 WorkerStore：示例的 App 只暴露了 Client 接口，而 NewWorker 要的是
	// 带 Claim/写回的 WorkerStore。两者在 mysql 方言里是同一个类型，这里按同样的
	// 配置再造一个，和 App 里那个共用同一个 *sql.DB。
	store, err := mysqlstore.New(app.DB, gorangetask.ClientConfig{})
	if err != nil {
		t.Fatalf("build worker store: %v", err)
	}
	worker, err := gorangetask.NewWorker(store, registry, fastConfig())
	if err != nil {
		t.Fatalf("NewWorker: %v", err)
	}

	runCtx, stopRun := context.WithCancel(ctx)
	defer stopRun()
	workerDone := make(chan error, 1)
	go func() { workerDone <- worker.Run(runCtx) }()

	eventually(t, 30*time.Second, "the task to succeed on its second attempt", func() (bool, error) {
		info, err := app.Client.Get(ctx, taskID)
		if err != nil {
			return false, err
		}
		return info.Status == gorangetask.StatusSucceeded, nil
	})

	if err := stopWorker(t, stopRun, workerDone); err != nil {
		t.Fatalf("worker returned %v on normal shutdown, want nil", err)
	}

	info, err := app.Client.Get(ctx, taskID)
	if err != nil {
		t.Fatalf("Get(%d): %v", taskID, err)
	}
	order, err := app.Store.GetOrder(ctx, orderID)
	if err != nil {
		t.Fatalf("GetOrder: %v", err)
	}
	if order.Status != "exported" {
		t.Errorf("order status = %q, want %q", order.Status, "exported")
	}

	mu.Lock()
	defer mu.Unlock()
	// 恰好两次：一次把副作用做完（然后失败），一次幂等命中。多一次说明任务被反复
	// 重试到不该有的次数，少一次说明注入的失败根本没发生、这条用例什么都没测。
	if calls != 2 {
		t.Fatalf("handler ran %d time(s), want 2", calls)
	}
	if !exported[0].Equal(exported[1]) {
		t.Fatalf("exported_at moved across the retry: %v → %v; the side effect must happen only once",
			exported[0], exported[1])
	}
	// 第二次执行既然什么都没改，任务的成功写回也不该留下上一次的失败摘要。
	if info.LastError != "" {
		t.Errorf("task last_error = %q after a successful retry, want empty", info.LastError)
	}
	if info.Attempt != 2 {
		t.Errorf("task attempt = %d, want 2", info.Attempt)
	}
}

// TestExampleGetOrderReportsMissingOrder 断言"读一笔不存在的订单"返回可判断的错误，
// 而不是拿一个零值订单当成功——调用方要靠它区分"没有这行"和"读失败"。
func TestExampleGetOrderReportsMissingOrder(t *testing.T) {
	app, _ := startTestApp(t, basic.Options{})

	// 库刚被清过表，1 号订单必然不存在。
	if _, err := app.Store.GetOrder(context.Background(), 1); !errors.Is(err, basic.ErrOrderNotFound) {
		t.Fatalf("GetOrder(1) error = %v, want ErrOrderNotFound", err)
	}
}

// TestExampleListOrdersReadsBackWhatWasCreated 走一遍 ListOrders 的读取循环：
// 行数、每行的字段（含 NULL 的 exported_at）都要对上。
//
// 这里**不声称验证了排序**：三笔订单是依次插入的，id 自然递增，"按 id 排"与
// "按 created_at 排"会给出同一个结果。要真正钉住 ORDER BY，插入顺序与期望顺序
// 必须不同（本表没有可手写的业务列能制造这个差异，故留空不写）。
func TestExampleListOrdersReadsBackWhatWasCreated(t *testing.T) {
	app, _ := startTestApp(t, basic.Options{})
	ctx := context.Background()

	const count = 3
	want := make([]int64, 0, count)
	for i := 0; i < count; i++ {
		orderID, _, err := app.Store.CreateOrder(ctx, fmt.Sprintf("customer-%d", i), int64(100*(i+1)))
		if err != nil {
			t.Fatalf("CreateOrder #%d: %v", i, err)
		}
		want = append(want, orderID)
	}

	// limit 取 10，大于实际行数：这里要测的是"读回来对不对"，不是"截断对不对"。
	orders, err := app.Store.ListOrders(ctx, 10)
	if err != nil {
		t.Fatalf("ListOrders: %v", err)
	}
	if len(orders) != count {
		t.Fatalf("ListOrders returned %d order(s), want %d", len(orders), count)
	}

	for i, order := range orders {
		wantCustomer := fmt.Sprintf("customer-%d", i)
		wantAmount := int64(100 * (i + 1))
		if order.ID != want[i] {
			t.Errorf("orders[%d].ID = %d, want %d", i, order.ID, want[i])
		}
		if order.Customer != wantCustomer || order.AmountCents != wantAmount {
			t.Errorf("orders[%d] = {%s, %d}, want {%s, %d}",
				i, order.Customer, order.AmountCents, wantCustomer, wantAmount)
		}
		if order.Status != "pending" {
			t.Errorf("orders[%d].Status = %q, want %q", i, order.Status, "pending")
		}
		// exported_at 是 NULL，扫进 sql.NullTime 必须保持 Valid == false。
		if order.ExportedAt.Valid {
			t.Errorf("orders[%d].ExportedAt = %v, want NULL", i, order.ExportedAt.Time)
		}
		if order.CreatedAt.IsZero() {
			t.Errorf("orders[%d].CreatedAt is the zero time, want the row's timestamp", i)
		}
	}
}

// TestExampleHandlerDelayReachesTheHandler 覆盖 `Options.HandlerDelay` 的接线。
// 它是 demo 的 `-handler-delay` 与 README 故障演示依赖的那个开关；把它接丢了不会
// 有任何编译错误，只会让"停机打断在途任务"变得几乎不可能手工复现。
//
// 断言刻意只往一个方向用：Handler 不可能比 delay 更快完成，所以
// **耗时 >= delay** 是安全断言（机器越慢越成立），不会因为负载高而假失败。
func TestExampleHandlerDelayReachesTheHandler(t *testing.T) {
	const delay = 400 * time.Millisecond
	app, _ := startTestApp(t, basic.Options{Config: fastConfig(), HandlerDelay: delay})
	ctx := context.Background()

	_, taskID, err := app.Store.CreateOrder(ctx, "grace", 900)
	if err != nil {
		t.Fatalf("CreateOrder: %v", err)
	}

	start := time.Now()
	runCtx, stopRun := context.WithCancel(ctx)
	defer stopRun()
	workerDone := make(chan error, 1)
	go func() { workerDone <- app.Run(runCtx) }()

	eventually(t, 30*time.Second, "the order to be exported", func() (bool, error) {
		info, err := app.Client.Get(ctx, taskID)
		if err != nil {
			return false, err
		}
		return info.Status == gorangetask.StatusSucceeded, nil
	})
	elapsed := time.Since(start)

	if err := stopWorker(t, stopRun, workerDone); err != nil {
		t.Fatalf("worker returned %v on normal shutdown, want nil", err)
	}

	if elapsed < delay {
		t.Errorf("the task finished in %s, want at least the configured %s delay", elapsed, delay)
	}
}
