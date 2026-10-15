package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	gomysql "github.com/go-sql-driver/mysql"

	gorangetask "github.com/Tidehyc/gorangetask"
	mysqlmigrations "github.com/Tidehyc/gorangetask/migrations/mysql"
)

// 集成测试需要一个可丢弃的 MySQL 数据库：
//
//	export GOTASK_TEST_MYSQL_DSN='root:rootpw@tcp(127.0.0.1:3306)/gotask?parseTime=true&loc=UTC&timeout=5s&readTimeout=30s&writeTimeout=30s'
//
// 未设置时全部跳过，因此普通的 go test ./... 不受影响。
//
// DSN 里的 `timeout`/`readTimeout`/`writeTimeout` 不是可有可无的：go-sql-driver
// 默认**不设读写期限**，MySQL 侧短暂不响应时，一条 `BeginTx` 会无限期阻塞在
// `syscall.write` 上（2026-10-15 宿主负载高时实测卡了 8 分钟）。后果不是"这条
// 用例失败"，而是整个包被 `go test` 的包级超时打掉——日志停在半途，只能拿到一份
// goroutine dump，且其余用例的结果一并丢失。有了期限，同样的情况会变成一条普通的
// 连接错误，被 Worker 的上报-重试路径吸收。
//
// 用例只通过公开 API（New / Enqueue / EnqueueTx）与只读 SELECT 断言行为，
// 目的是让"入队写进去的到底长什么样"这件事有据可查。
//
// 数据库隔离：本包不直接使用 DSN 里的库，而是用 <库名>_store 这个派生库
// （DSN 写 gotask 时即 gotask_store），并在需要时自动创建。原因是集成测试的
// setup 是破坏性的（DROP TABLE + 重新迁移），而 go test ./... 会并发编译和运行
// 不同的包：若本包与 migrations/mysql 共用同一个库，两边会互相删表，报出
// "dirty state"或"Table already exists"这类与产品无关的假失败。
const (
	testDSNEnv = "GOTASK_TEST_MYSQL_DSN"

	// testDBPrefix 是派生库名后缀。每个集成测试包应使用不同的后缀。
	testDBPrefix = "_store"

	schemaMigrationsTable = "gotask_schema_migrations"
	businessTable         = "gotask_test_business"
)

// newTestClientAndDB 准备一个已迁移到最新版本、且不含旧数据的数据库。
func newTestClientAndDB(t *testing.T, cfg gorangetask.ClientConfig) (*Client, *sql.DB) {
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

	cfgForTarget := *parsed
	cfgForTarget.DBName = target
	db, err := sql.Open("mysql", cfgForTarget.FormatDSN())
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	ctx := context.Background()
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping database: %v", err)
	}

	for _, table := range []string{tasksTable, schemaMigrationsTable, businessTable} {
		if _, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS "+table); err != nil {
			t.Fatalf("drop table %s: %v", table, err)
		}
	}
	if err := mysqlmigrations.Migrate(ctx, db); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	c, err := New(db, cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return c, db
}

// ensureDatabase 创建一个派生测试库。这会连到"不指定库"的连接上：
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

// TestMySQLServerVersionIsSupported 把"实测在哪个 MySQL 上跑"写进测试日志，
// 并挡住用错镜像的情况（例如误配 MariaDB：它的版本号是 10.x）。
// 项目只验证 MySQL 8.0+，所以这里断言主版本是 8 或 9。
func TestMySQLServerVersionIsSupported(t *testing.T) {
	_, db := newTestClientAndDB(t, gorangetask.ClientConfig{})

	var version string
	if err := db.QueryRowContext(context.Background(), "SELECT VERSION()").Scan(&version); err != nil {
		t.Fatalf("read server version: %v", err)
	}
	t.Logf("MySQL server version: %s", version)

	if !strings.HasPrefix(version, "8.") && !strings.HasPrefix(version, "9.") {
		t.Errorf("server version %q is not MySQL 8.0+; this project only verifies MySQL 8.0+", version)
	}
}

// dbNow 取数据库当前时间。所有时间断言都以数据库时钟为准，避免把测试机器
// 的时钟差异当成产品缺陷（技术设计 §3.2 的同一思路）。
func dbNow(t *testing.T, db *sql.DB) time.Time {
	t.Helper()

	var now time.Time
	if err := db.QueryRowContext(context.Background(), "SELECT UTC_TIMESTAMP(6)").Scan(&now); err != nil {
		t.Fatalf("read database time: %v", err)
	}
	return now.UTC()
}

// taskRow 是一次 SELECT 读回的任务行。可空列用 NullXxx 承载，
// 以便区分"写了 NULL"和"写成了空串/零值"。
type taskRow struct {
	id             int64
	queueName      string
	taskType       string
	payload        string
	status         string
	runAt          time.Time
	attempt        int64
	maxAttempts    int64
	leaseOwner     sql.NullString
	leaseUntil     sql.NullTime
	leaseEpoch     int64
	idempotencyKey sql.NullString
	lastError      sql.NullString
	createdAt      time.Time
	updatedAt      time.Time
	finishedAt     sql.NullTime
}

func readTask(t *testing.T, db *sql.DB, id gorangetask.TaskID) taskRow {
	t.Helper()

	var r taskRow
	err := db.QueryRowContext(context.Background(), `SELECT
        id, queue_name, task_type, payload, status, run_at,
        attempt, max_attempts, lease_owner, lease_until, lease_epoch,
        idempotency_key, last_error, created_at, updated_at, finished_at
    FROM `+tasksTable+` WHERE id = ?`, int64(id)).Scan(
		&r.id, &r.queueName, &r.taskType, &r.payload, &r.status, &r.runAt,
		&r.attempt, &r.maxAttempts, &r.leaseOwner, &r.leaseUntil, &r.leaseEpoch,
		&r.idempotencyKey, &r.lastError, &r.createdAt, &r.updatedAt, &r.finishedAt,
	)
	if err != nil {
		t.Fatalf("read task %d: %v", id, err)
	}
	return r
}

func countTasks(t *testing.T, db *sql.DB) int {
	t.Helper()

	var n int
	if err := db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM "+tasksTable).Scan(&n); err != nil {
		t.Fatalf("count tasks: %v", err)
	}
	return n
}

// TestEnqueueWritesPendingTask 固化新任务的完整初始形态：状态、计数、
// 租约字段、终态字段都必须是"干净的"。
func TestEnqueueWritesPendingTask(t *testing.T) {
	c, db := newTestClientAndDB(t, gorangetask.ClientConfig{})

	before := dbNow(t, db)
	id, err := c.Enqueue(context.Background(), gorangetask.Task{
		Type:    "send_email",
		Payload: []byte(`{"to":"a@example.com"}`),
	})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	after := dbNow(t, db)

	if id == 0 {
		t.Fatal("Enqueue returned zero TaskID on success")
	}

	r := readTask(t, db, id)
	if r.queueName != gorangetask.DefaultQueue {
		t.Errorf("queue_name = %q, want %q", r.queueName, gorangetask.DefaultQueue)
	}
	if r.taskType != "send_email" {
		t.Errorf("task_type = %q, want %q", r.taskType, "send_email")
	}
	if r.status != string(gorangetask.StatusPending) {
		t.Errorf("status = %q, want %q", r.status, gorangetask.StatusPending)
	}
	if r.attempt != 0 {
		t.Errorf("attempt = %d, want 0", r.attempt)
	}
	if r.maxAttempts != int64(gorangetask.DefaultMaxAttempts) {
		t.Errorf("max_attempts = %d, want %d", r.maxAttempts, gorangetask.DefaultMaxAttempts)
	}
	if r.leaseEpoch != 0 {
		t.Errorf("lease_epoch = %d, want 0", r.leaseEpoch)
	}
	for _, col := range []struct {
		name  string
		valid bool
	}{
		{"lease_owner", r.leaseOwner.Valid},
		{"lease_until", r.leaseUntil.Valid},
		{"idempotency_key", r.idempotencyKey.Valid},
		{"last_error", r.lastError.Valid},
		{"finished_at", r.finishedAt.Valid},
	} {
		if col.valid {
			t.Errorf("%s should be NULL for a fresh task", col.name)
		}
	}

	// 立即执行任务的 run_at 由数据库时间决定，必须落在取时间的前后之间。
	if r.runAt.Before(before) || r.runAt.After(after) {
		t.Errorf("run_at = %s, want within [%s, %s]", r.runAt, before, after)
	}
	if !r.createdAt.UTC().Equal(r.runAt) {
		t.Errorf("created_at = %s, want to equal run_at %s", r.createdAt, r.runAt)
	}
	if !r.updatedAt.UTC().Equal(r.runAt) {
		t.Errorf("updated_at = %s, want to equal run_at %s", r.updatedAt, r.runAt)
	}
}

// TestEnqueueDelayedTaskStoresRunAtInUTC 验证延迟任务：run_at 被原样存入且是 UTC。
// 用非 UTC 时区构造、并带纳秒，确认既做了时区换算又截断到 DATETIME(6) 的微秒精度。
func TestEnqueueDelayedTaskStoresRunAtInUTC(t *testing.T) {
	c, db := newTestClientAndDB(t, gorangetask.ClientConfig{})

	// run_at 必须相对**数据库时钟**构造。写死一个绝对时刻的用例会随时间推移
	// 从"未来"变成"过去"，于是它从"延迟任务"悄悄退化成"已到期任务"——
	// 这个断言确实因为日期流逝失败过一次。
	shanghai := time.FixedZone("CST", 8*3600)
	// 带时区、且带非零纳秒：前者验证"传入 +08:00 的时刻按等价 UTC instant 存储"，
	// 后者验证 DATETIME(6) 把纳秒截断到微秒。
	runAt := dbNow(t, db).Add(2 * time.Hour).In(shanghai).Add(123456789 * time.Nanosecond)
	wantUTC := runAt.UTC().Truncate(time.Microsecond)

	id, err := c.Enqueue(context.Background(), gorangetask.Task{
		Type:    "send_email",
		Payload: []byte(`{}`),
		RunAt:   runAt,
	})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	r := readTask(t, db, id)
	if !r.runAt.UTC().Equal(wantUTC) {
		t.Errorf("run_at = %s, want %s", r.runAt.UTC(), wantUTC)
	}
	if r.runAt.UTC().Before(dbNow(t, db)) {
		t.Error("run_at should be in the future for a delayed task")
	}
	// 延迟任务的 created_at 仍是"现在"，而不是 run_at。
	if !r.createdAt.UTC().Before(r.runAt.UTC()) {
		t.Errorf("created_at = %s should precede run_at = %s", r.createdAt, r.runAt)
	}
}

func TestEnqueuePayloadRoundTrip(t *testing.T) {
	c, db := newTestClientAndDB(t, gorangetask.ClientConfig{})

	// MySQL 的 JSON 列会规范化键顺序与空白，所以比较反序列化后的结构而不是原字符串。
	payload := []byte(`{"b":2,"a":{"nested":true},"list":[1,2,3]}`)

	id, err := c.Enqueue(context.Background(), gorangetask.Task{Type: "noop", Payload: payload})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	r := readTask(t, db, id)

	var got, want any
	if err := json.Unmarshal([]byte(r.payload), &got); err != nil {
		t.Fatalf("stored payload is not valid JSON: %v (raw=%s)", err, r.payload)
	}
	if err := json.Unmarshal(payload, &want); err != nil {
		t.Fatalf("test payload is not valid JSON: %v", err)
	}
	if !jsonEqual(got, want) {
		t.Errorf("stored payload = %s, want semantically equal to %s", r.payload, payload)
	}
}

func jsonEqual(a, b any) bool {
	ab, err1 := json.Marshal(a)
	bb, err2 := json.Marshal(b)
	return err1 == nil && err2 == nil && string(ab) == string(bb)
}

// TestEnqueueRejectsDuplicateIdempotencyKey 覆盖幂等键的核心承诺：
// 同队列内相同非空键只允许一条任务，冲突返回可判断的错误。
func TestEnqueueRejectsDuplicateIdempotencyKey(t *testing.T) {
	c, db := newTestClientAndDB(t, gorangetask.ClientConfig{})
	ctx := context.Background()

	task := gorangetask.Task{
		Queue:          "orders",
		Type:           "charge_card",
		Payload:        []byte(`{"order_id":42}`),
		IdempotencyKey: "order-42",
	}

	first, err := c.Enqueue(ctx, task)
	if err != nil {
		t.Fatalf("first Enqueue: %v", err)
	}

	_, err = c.Enqueue(ctx, task)
	if !errors.Is(err, gorangetask.ErrDuplicateTask) {
		t.Fatalf("second Enqueue: want ErrDuplicateTask, got %v", err)
	}
	if n := countTasks(t, db); n != 1 {
		t.Fatalf("task count = %d, want 1 (duplicate must not be inserted)", n)
	}

	// 冲突时不得返回既有任务 ID。
	if _, err := c.Enqueue(ctx, task); err == nil {
		t.Fatal("third Enqueue: want an error")
	}

	r := readTask(t, db, first)
	if !r.idempotencyKey.Valid || r.idempotencyKey.String != "order-42" {
		t.Errorf("idempotency_key = %v, want \"order-42\"", r.idempotencyKey)
	}
}

// TestEnqueueIdempotencyKeyIsScopedPerQueue 说明幂等键的作用域：同队列内唯一。
func TestEnqueueIdempotencyKeyIsScopedPerQueue(t *testing.T) {
	c, db := newTestClientAndDB(t, gorangetask.ClientConfig{})
	ctx := context.Background()

	for _, queue := range []string{"orders", "reports"} {
		if _, err := c.Enqueue(ctx, gorangetask.Task{
			Queue:          queue,
			Type:           "noop",
			Payload:        []byte(`{}`),
			IdempotencyKey: "shared-key",
		}); err != nil {
			t.Fatalf("Enqueue(queue=%s): %v", queue, err)
		}
	}

	if n := countTasks(t, db); n != 2 {
		t.Fatalf("task count = %d, want 2 (same key in different queues must not conflict)", n)
	}
}

// TestEnqueueEmptyIdempotencyKeyDoesNotConflict 固化"空幂等键落成 NULL"的行为：
// 若实现写成空串，第二条任务会撞唯一索引并被误判为重复。
func TestEnqueueEmptyIdempotencyKeyDoesNotConflict(t *testing.T) {
	c, db := newTestClientAndDB(t, gorangetask.ClientConfig{})
	ctx := context.Background()

	first, err := c.Enqueue(ctx, gorangetask.Task{Queue: "orders", Type: "noop", Payload: []byte(`{}`)})
	if err != nil {
		t.Fatalf("first Enqueue: %v", err)
	}
	second, err := c.Enqueue(ctx, gorangetask.Task{Queue: "orders", Type: "noop", Payload: []byte(`{}`)})
	if err != nil {
		t.Fatalf("second Enqueue: %v", err)
	}

	if n := countTasks(t, db); n != 2 {
		t.Fatalf("task count = %d, want 2", n)
	}
	for _, id := range []gorangetask.TaskID{first, second} {
		if r := readTask(t, db, id); r.idempotencyKey.Valid {
			t.Errorf("task %d idempotency_key = %v, want NULL", id, r.idempotencyKey)
		}
	}
}

func TestEnqueueRejectsInvalidTaskBeforeTouchingDatabase(t *testing.T) {
	c, db := newTestClientAndDB(t, gorangetask.ClientConfig{})
	ctx := context.Background()

	tests := []struct {
		name string
		task gorangetask.Task
		want error
	}{
		{"empty payload", gorangetask.Task{Type: "noop"}, gorangetask.ErrInvalidPayload},
		{"invalid json", gorangetask.Task{Type: "noop", Payload: []byte(`{`)}, gorangetask.ErrInvalidPayload},
		{"empty type", gorangetask.Task{Payload: []byte(`{}`)}, gorangetask.ErrInvalidTask},
		{"negative timeout", gorangetask.Task{Type: "noop", Payload: []byte(`{}`), Timeout: -time.Second}, gorangetask.ErrInvalidTask},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := c.Enqueue(ctx, tc.task); !errors.Is(err, tc.want) {
				t.Fatalf("want %v, got %v", tc.want, err)
			}
		})
	}

	if n := countTasks(t, db); n != 0 {
		t.Fatalf("task count = %d, want 0 (校验必须在落库前完成)", n)
	}
}

// TestEnqueueSurfacesDatabaseErrors 固化"只把唯一键冲突翻译成 ErrDuplicateTask"：
// 其它数据库错误（例如表不存在）必须原样带上下文冒泡，不能被悄悄吞掉或误判成重复。
func TestEnqueueSurfacesDatabaseErrors(t *testing.T) {
	c, db := newTestClientAndDB(t, gorangetask.ClientConfig{})
	ctx := context.Background()

	if _, err := db.ExecContext(ctx, "DROP TABLE "+tasksTable); err != nil {
		t.Fatalf("drop tasks table: %v", err)
	}

	_, err := c.Enqueue(ctx, gorangetask.Task{Type: "noop", Payload: []byte(`{}`)})
	if err == nil {
		t.Fatal("want an error when the tasks table is missing")
	}
	for _, sentinel := range []error{
		gorangetask.ErrDuplicateTask,
		gorangetask.ErrInvalidTask,
		gorangetask.ErrInvalidPayload,
	} {
		if errors.Is(err, sentinel) {
			t.Errorf("error %v must not be classified as %v", err, sentinel)
		}
	}
	if !strings.Contains(err.Error(), "insert task") {
		t.Errorf("error %v should carry the operation context", err)
	}
}

// createBusinessTable 建一张最小的"业务表"，用来验证任务与业务数据共享事务。
func createBusinessTable(t *testing.T, db *sql.DB) {
	t.Helper()

	_, err := db.ExecContext(context.Background(),
		"CREATE TABLE "+businessTable+" (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, note VARCHAR(64) NOT NULL) ENGINE=InnoDB")
	if err != nil {
		t.Fatalf("create business table: %v", err)
	}
}

func countBusinessRows(t *testing.T, db *sql.DB) int {
	t.Helper()

	var n int
	if err := db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM "+businessTable).Scan(&n); err != nil {
		t.Fatalf("count business rows: %v", err)
	}
	return n
}

// TestEnqueueIdempotencyKeyIsUniqueUnderConcurrency 是幂等键承诺的并发验收：
// N 个 goroutine 同时用同一个键入队，必须恰好一个成功，其余拿到 ErrDuplicateTask，
// 库里恰好一行。这条依赖唯一索引而不是"先查后插"——后者在并发下必然漏判。
func TestEnqueueIdempotencyKeyIsUniqueUnderConcurrency(t *testing.T) {
	c, db := newTestClientAndDB(t, gorangetask.ClientConfig{})
	ctx := context.Background()

	const workers = 8
	task := gorangetask.Task{
		Queue:          "orders",
		Type:           "charge_card",
		Payload:        []byte(`{"order_id":42}`),
		IdempotencyKey: "order-42",
	}

	var (
		wg      sync.WaitGroup
		start   = make(chan struct{})
		results = make([]error, workers)
	)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start // 尽量让所有 goroutine 同时发出 INSERT
			_, results[i] = c.Enqueue(ctx, task)
		}(i)
	}
	close(start)
	wg.Wait()

	var succeeded, duplicated int
	for i, err := range results {
		switch {
		case err == nil:
			succeeded++
		case errors.Is(err, gorangetask.ErrDuplicateTask):
			duplicated++
		default:
			t.Errorf("worker %d got an unexpected error (must be nil or ErrDuplicateTask): %v", i, err)
		}
	}

	if succeeded != 1 {
		t.Errorf("successful enqueues = %d, want exactly 1", succeeded)
	}
	if duplicated != workers-1 {
		t.Errorf("duplicate errors = %d, want %d", duplicated, workers-1)
	}
	if n := countTasks(t, db); n != 1 {
		t.Errorf("task count = %d, want 1", n)
	}
}

// TestEnqueueTxCommitsWithCallerTransaction 验证核心卖点：
// 业务写入与任务入队在同一个事务里一起提交。
func TestEnqueueTxCommitsWithCallerTransaction(t *testing.T) {
	c, db := newTestClientAndDB(t, gorangetask.ClientConfig{})
	createBusinessTable(t, db)
	ctx := context.Background()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback() }() // 提交后是 no-op

	if _, err := tx.ExecContext(ctx, "INSERT INTO "+businessTable+" (note) VALUES (?)", "order created"); err != nil {
		t.Fatalf("insert business row: %v", err)
	}
	id, err := c.EnqueueTx(ctx, tx, gorangetask.Task{
		Queue:   "orders",
		Type:    "send_receipt",
		Payload: []byte(`{"order_id":42}`),
	})
	if err != nil {
		t.Fatalf("EnqueueTx: %v", err)
	}
	if id == 0 {
		t.Fatal("EnqueueTx returned zero TaskID on success")
	}

	// 提交前：另一个连接不应看到任务（说明它确实在调用方事务里，没有被自动提交）。
	if n := countTasks(t, db); n != 0 {
		t.Fatalf("task count before commit = %d, want 0 (task must stay inside the caller's transaction)", n)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	if n := countBusinessRows(t, db); n != 1 {
		t.Errorf("business rows = %d, want 1", n)
	}
	if n := countTasks(t, db); n != 1 {
		t.Errorf("task count = %d, want 1", n)
	}

	r := readTask(t, db, id)
	if r.queueName != "orders" || r.taskType != "send_receipt" || r.status != string(gorangetask.StatusPending) {
		t.Errorf("unexpected row after commit: queue=%q type=%q status=%q", r.queueName, r.taskType, r.status)
	}
}

// TestEnqueueTxRollsBackWithCallerTransaction 验证反向：事务回滚后任务不存在。
func TestEnqueueTxRollsBackWithCallerTransaction(t *testing.T) {
	c, db := newTestClientAndDB(t, gorangetask.ClientConfig{})
	createBusinessTable(t, db)
	ctx := context.Background()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO "+businessTable+" (note) VALUES (?)", "order created"); err != nil {
		t.Fatalf("insert business row: %v", err)
	}
	if _, err := c.EnqueueTx(ctx, tx, gorangetask.Task{Type: "send_receipt", Payload: []byte(`{}`)}); err != nil {
		t.Fatalf("EnqueueTx: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	if n := countBusinessRows(t, db); n != 0 {
		t.Errorf("business rows = %d, want 0", n)
	}
	if n := countTasks(t, db); n != 0 {
		t.Errorf("task count = %d, want 0 (rollback must discard the task too)", n)
	}
}

// TestEnqueueTxDuplicateIsReportedToCaller 覆盖"事务内命中幂等键"的路径：
// 错误要交给调用方决定，库自己不回滚、不重试。
func TestEnqueueTxDuplicateIsReportedToCaller(t *testing.T) {
	c, db := newTestClientAndDB(t, gorangetask.ClientConfig{})
	ctx := context.Background()

	if _, err := c.Enqueue(ctx, gorangetask.Task{
		Queue: "orders", Type: "charge_card", Payload: []byte(`{}`), IdempotencyKey: "order-42",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	_, err = c.EnqueueTx(ctx, tx, gorangetask.Task{
		Queue: "orders", Type: "charge_card", Payload: []byte(`{}`), IdempotencyKey: "order-42",
	})
	if !errors.Is(err, gorangetask.ErrDuplicateTask) {
		t.Fatalf("want ErrDuplicateTask, got %v", err)
	}

	// 库不替调用方决定事务命运：回滚后仍然只有第一条。
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if n := countTasks(t, db); n != 1 {
		t.Fatalf("task count = %d, want 1", n)
	}
}

// TestEnqueueTxUsesCallerConnection 验证 EnqueueTx 不另取连接：
// 在事务里插入的任务，未提交时只有该事务自己看得见。
func TestEnqueueTxUsesCallerConnection(t *testing.T) {
	c, db := newTestClientAndDB(t, gorangetask.ClientConfig{})
	ctx := context.Background()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	id, err := c.EnqueueTx(ctx, tx, gorangetask.Task{Type: "noop", Payload: []byte(`{}`)})
	if err != nil {
		t.Fatalf("EnqueueTx: %v", err)
	}

	// 同一事务内可见。
	var visibleInside int
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+tasksTable+" WHERE id = ?", int64(id)).Scan(&visibleInside); err != nil {
		t.Fatalf("count inside tx: %v", err)
	}
	if visibleInside != 1 {
		t.Errorf("visible inside tx = %d, want 1", visibleInside)
	}

	// 事务外不可见。
	if n := countTasks(t, db); n != 0 {
		t.Errorf("visible outside tx = %d, want 0", n)
	}
}

// TestEnqueuePersistsAndReturnsTaskTimeout 固定逐任务超时真的落库：
//   - 设置过 → 库里 timeout_ns 是纳秒数，Get 能原样读回；
//   - 没设置 → 库里是 **NULL**（不是 0），Get 读回 0。
//
// 区分 NULL 与 0 是这条用例的重点：读取方把 0 解释为"未设置"，
// 若把"没填"写成 0，之后就无法区分两者的来源了。
func TestEnqueuePersistsAndReturnsTaskTimeout(t *testing.T) {
	c, db := newTestClientAndDB(t, gorangetask.ClientConfig{})
	ctx := context.Background()

	const want = 90 * time.Second
	withTimeout, err := c.Enqueue(ctx, gorangetask.Task{
		Queue:   "q-timeout",
		Type:    "slow.task",
		Payload: json.RawMessage(`{}`),
		Timeout: want,
	})
	if err != nil {
		t.Fatalf("Enqueue(with timeout): %v", err)
	}
	withoutTimeout, err := c.Enqueue(ctx, gorangetask.Task{
		Queue:   "q-timeout",
		Type:    "slow.task",
		Payload: json.RawMessage(`{}`),
	})
	if err != nil {
		t.Fatalf("Enqueue(without timeout): %v", err)
	}

	// 库侧直查，确认写进去的确实是指定的纳秒数 / NULL。
	var (
		storedWith    sql.NullInt64
		storedWithout sql.NullInt64
	)
	if err := db.QueryRowContext(ctx,
		"SELECT timeout_ns FROM "+tasksTable+" WHERE id = ?", int64(withTimeout)).Scan(&storedWith); err != nil {
		t.Fatalf("read stored timeout: %v", err)
	}
	if !storedWith.Valid || storedWith.Int64 != int64(want) {
		t.Errorf("stored timeout_ns = %v, want %d", storedWith, int64(want))
	}
	if err := db.QueryRowContext(ctx,
		"SELECT timeout_ns FROM "+tasksTable+" WHERE id = ?", int64(withoutTimeout)).Scan(&storedWithout); err != nil {
		t.Fatalf("read stored timeout: %v", err)
	}
	if storedWithout.Valid {
		t.Errorf("stored timeout_ns = %d, want NULL when the task sets no timeout", storedWithout.Int64)
	}

	// Get 往返：设置过的原样读回，未设置的读回 0（即"用 Worker 默认值"）。
	info, err := c.Get(ctx, withTimeout)
	if err != nil {
		t.Fatalf("Get(with timeout): %v", err)
	}
	if info.Timeout != want {
		t.Errorf("Get().Timeout = %s, want %s", info.Timeout, want)
	}
	info, err = c.Get(ctx, withoutTimeout)
	if err != nil {
		t.Fatalf("Get(without timeout): %v", err)
	}
	if info.Timeout != 0 {
		t.Errorf("Get().Timeout = %s, want 0 for a task without a timeout", info.Timeout)
	}
}
