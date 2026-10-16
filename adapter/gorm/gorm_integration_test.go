package gormadapter

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	gomysql "github.com/go-sql-driver/mysql"
	gormmysql "gorm.io/driver/mysql"
	gormio "gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"

	gorangetask "github.com/Tidehyc/gorangetask"
	mysqlmigrations "github.com/Tidehyc/gorangetask/migrations/mysql"
	mysqlstore "github.com/Tidehyc/gorangetask/store/mysql"
)

// 集成测试需要一个可丢弃的 MySQL 数据库（与 store/mysql 用同一个 DSN 环境变量）：
//
//	export GOTASK_TEST_MYSQL_DSN='root:rootpw@tcp(127.0.0.1:3306)/gotask?parseTime=true&loc=UTC&timeout=5s&readTimeout=30s&writeTimeout=30s'
//
// 未设置时全部跳过。
//
// 数据库隔离：本包用 <库名>_adapter_gorm 这个派生库并自动创建。集成测试的 setup
// 是破坏性的（DROP TABLE + 重新迁移），而 go test ./... 会并发运行不同的包；
// 共用一个库会让各包互相删表，报出与产品无关的假失败。每个集成测试包取一个
// 独立后缀，这是 store/mysql 已经踩过并写在注释里的约定。
const (
	testDSNEnv            = "GOTASK_TEST_MYSQL_DSN"
	testDBPrefix          = "_adapter_gorm"
	tasksTable            = "gotask_tasks"
	schemaMigrationsTable = "gotask_schema_migrations"
	businessTable         = "gorm_adapter_orders"
)

// Order 是模拟的业务表：示例用它证明"业务行与任务行同事务"。
//
// 它由测试自己拥有，库不管它——与 example/basic 的分工一致。
type Order struct {
	ID        uint64 `gorm:"primaryKey"`
	Ref       string `gorm:"size:64;uniqueIndex"`
	CreatedAt time.Time
}

func (Order) TableName() string { return businessTable }

// fixture 是一次集成测试的完整环境。
type fixture struct {
	adapter *Adapter
	client  *mysqlstore.Client
	gormDB  *gormio.DB
	db      *sql.DB
}

// newFixture 准备一个已迁移到最新版本、不含旧数据、业务表已建好的环境。
//
// gormCfg 为 nil 时用一个静默 logger（否则 GORM 会把每条 SQL 打到测试输出里，
// 把真正有用的失败信息淹掉）。
func newFixture(t *testing.T, cfg gorangetask.ClientConfig, gormCfg *gormio.Config) *fixture {
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
	if _, err := db.ExecContext(ctx, `CREATE TABLE `+businessTable+` (
		id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
		ref VARCHAR(64) NOT NULL,
		created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
		PRIMARY KEY (id),
		UNIQUE KEY uq_gorm_adapter_orders_ref (ref)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`); err != nil {
		t.Fatalf("create business table: %v", err)
	}

	client, err := mysqlstore.New(db, cfg)
	if err != nil {
		t.Fatalf("mysqlstore.New: %v", err)
	}
	adapter, err := New(client)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// GORM 复用同一个 *sql.DB：否则它会自己开一个连接池，测试就变成了在验证
	// "两个连接池之间能不能同事务"——那不可能成立，也不是本 adapter 的用法。
	if gormCfg == nil {
		gormCfg = &gormio.Config{}
	}
	gormCfg.Logger = gormlogger.Default.LogMode(gormlogger.Silent)
	gormDB, err := gormio.Open(gormmysql.New(gormmysql.Config{Conn: db}), gormCfg)
	if err != nil {
		t.Fatalf("gorm.Open: %v", err)
	}

	return &fixture{adapter: adapter, client: client, gormDB: gormDB, db: db}
}

// ensureDatabase 创建一个派生测试库。它会连到"不指定库"的连接上：
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

// countRows 数一张表里的行数。断言"没有写成"时直接数行数，比走查询 API 更硬：
// 查询 API 可能因为别的原因查不到，而行数不会。
func (f *fixture) countRows(t *testing.T, table string) int {
	t.Helper()
	var n int
	if err := f.db.QueryRowContext(context.Background(),
		"SELECT COUNT(*) FROM "+table).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n
}

func testTask(queue string) gorangetask.Task {
	return gorangetask.Task{Queue: queue, Type: "order.export", Payload: json.RawMessage(`{"ref":"o-1"}`)}
}

// TestEnqueueTxCommitsWithBusinessWrite 是 adapter 的正向用例：业务行与任务行
// 在同一个 GORM 事务里写入，一起提交。
func TestEnqueueTxCommitsWithBusinessWrite(t *testing.T) {
	f := newFixture(t, gorangetask.ClientConfig{}, nil)
	ctx := context.Background()

	var taskID gorangetask.TaskID
	err := f.gormDB.WithContext(ctx).Transaction(func(tx *gormio.DB) error {
		if err := tx.Create(&Order{Ref: "o-1"}).Error; err != nil {
			return err
		}
		id, err := f.adapter.EnqueueTx(ctx, tx, testTask("orders"))
		if err != nil {
			return err
		}
		taskID = id
		return nil
	})
	if err != nil {
		t.Fatalf("Transaction: %v", err)
	}

	if n := f.countRows(t, businessTable); n != 1 {
		t.Errorf("business rows = %d, want 1", n)
	}
	if n := f.countRows(t, tasksTable); n != 1 {
		t.Errorf("task rows = %d, want 1", n)
	}
	if taskID == 0 {
		t.Fatal("EnqueueTx returned task ID 0")
	}

	// 返回的 ID 能查到，且任务确实落在请求的队列上。
	info, err := f.client.Get(ctx, taskID)
	if err != nil {
		t.Fatalf("Get(%d): %v", taskID, err)
	}
	if info.Status != gorangetask.StatusPending || info.Queue != "orders" || info.Type != "order.export" {
		t.Errorf("task = %+v, want a pending order.export task in queue \"orders\"", info)
	}
}

// TestEnqueueTxRollsBackWithBusinessWrite 是本文件最有判别力的一条：事务回滚后
// **两行都不该存在**。
//
// 若 adapter 换了自己的连接（或另开事务），业务行会回滚、任务行却留了下来——
// 这正是"业务数据与任务行同一事务提交"这个卖点被悄悄毁掉的方式，而且从调用方
// 代码上看不出任何异常。
func TestEnqueueTxRollsBackWithBusinessWrite(t *testing.T) {
	f := newFixture(t, gorangetask.ClientConfig{}, nil)
	ctx := context.Background()

	sentinel := errors.New("business write rejected")
	err := f.gormDB.WithContext(ctx).Transaction(func(tx *gormio.DB) error {
		if err := tx.Create(&Order{Ref: "o-rollback"}).Error; err != nil {
			return err
		}
		if _, err := f.adapter.EnqueueTx(ctx, tx, testTask("orders")); err != nil {
			return err
		}
		return sentinel // 业务侧决定放弃
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("Transaction error = %v, want the sentinel", err)
	}

	if n := f.countRows(t, businessTable); n != 0 {
		t.Errorf("business rows = %d after rollback, want 0", n)
	}
	if n := f.countRows(t, tasksTable); n != 0 {
		t.Errorf("task rows = %d after rollback, want 0 (任务行与业务行必须同生共死)", n)
	}
}

// TestEnqueueTxRejectsNonTransactionHandle 挡住最危险的误用：传入非事务的
// *gorm.DB。
//
// 它同时断言"被拒绝的调用没有留下任何痕迹"——先写后校验的实现会在库里留下
// 一条任务行，而调用方以为这次入队失败了。
func TestEnqueueTxRejectsNonTransactionHandle(t *testing.T) {
	f := newFixture(t, gorangetask.ClientConfig{}, nil)
	ctx := context.Background()

	// f.gormDB 是 gorm.Open 的返回值，不是事务句柄。
	_, err := f.adapter.EnqueueTx(ctx, f.gormDB, testTask("orders"))
	if !errors.Is(err, gorangetask.ErrInvalidTask) {
		t.Fatalf("EnqueueTx(plain *gorm.DB) error = %v, want ErrInvalidTask", err)
	}
	if n := f.countRows(t, tasksTable); n != 0 {
		t.Errorf("task rows = %d after a rejected call, want 0（拒绝不能有副作用）", n)
	}

	// 同样是"非事务"：db.Session 出来的句柄依然直接持有 *sql.DB。
	_, err = f.adapter.EnqueueTx(ctx, f.gormDB.Session(&gormio.Session{}), testTask("orders"))
	if !errors.Is(err, gorangetask.ErrInvalidTask) {
		t.Fatalf("EnqueueTx(session, non-tx) error = %v, want ErrInvalidTask", err)
	}
	if n := f.countRows(t, tasksTable); n != 0 {
		t.Errorf("task rows = %d after a rejected call, want 0", n)
	}

	// nil 句柄。
	if _, err := f.adapter.EnqueueTx(ctx, nil, testTask("orders")); !errors.Is(err, gorangetask.ErrInvalidTask) {
		t.Fatalf("EnqueueTx(nil) error = %v, want ErrInvalidTask", err)
	}
	if n := f.countRows(t, tasksTable); n != 0 {
		t.Errorf("task rows = %d after a rejected call, want 0", n)
	}
}

// TestEnqueueTxRejectsFailedSession 确保会话自带的错误被原样暴露，而不是被
// 吞掉后写成一条注定不会提交的任务。
func TestEnqueueTxRejectsFailedSession(t *testing.T) {
	f := newFixture(t, gorangetask.ClientConfig{}, nil)
	ctx := context.Background()

	boom := errors.New("begin failed")
	tx := f.gormDB.Begin()
	tx.AddError(boom)

	_, err := f.adapter.EnqueueTx(ctx, tx, testTask("orders"))
	if !errors.Is(err, gorangetask.ErrInvalidTask) {
		t.Fatalf("EnqueueTx(failed session) error = %v, want ErrInvalidTask", err)
	}
	if !errors.Is(err, boom) {
		t.Errorf("error = %v, want it to wrap the session error %v", err, boom)
	}
	if n := f.countRows(t, tasksTable); n != 0 {
		t.Errorf("task rows = %d, want 0", n)
	}
}

// TestEnqueueTxReusesClientValidation 证明 adapter 用的是 store/mysql 的那条
// INSERT 与那套校验，而不是自己重写一份。
//
// 判别方式：把 Client 的 payload 上限设成 4 字节，adapter 必须报出同一个
// ErrInvalidPayload。若 adapter 自己写 INSERT，这条会直接写进库（或被 MySQL 的
// 严格模式以 1406 拒绝），报出来的错误与普通入队路径就不一样了。
func TestEnqueueTxReusesClientValidation(t *testing.T) {
	f := newFixture(t, gorangetask.ClientConfig{MaxPayloadSize: 4}, nil)
	ctx := context.Background()

	err := f.gormDB.WithContext(ctx).Transaction(func(tx *gormio.DB) error {
		_, err := f.adapter.EnqueueTx(ctx, tx, gorangetask.Task{
			Queue: "orders", Type: "order.export", Payload: json.RawMessage(`{"too":"long"}`),
		})
		return err
	})
	if !errors.Is(err, gorangetask.ErrInvalidPayload) {
		t.Fatalf("error = %v, want ErrInvalidPayload", err)
	}
	if n := f.countRows(t, tasksTable); n != 0 {
		t.Errorf("task rows = %d, want 0", n)
	}
}

// TestEnqueueTxDetectsIdempotencyConflictInsideTheTransaction 从另一个角度
// 证明"确实在同一个事务上执行"：同一个事务里插两条相同幂等键，第二条必须**当场**
// 撞上唯一索引（未提交的行在同事务内可见）。
//
// 如果 adapter 走的是另一个连接/另一个事务，第二条就看不到第一条的行，不会冲突
// ——用例会以"没报 ErrDuplicateTask"失败。
func TestEnqueueTxDetectsIdempotencyConflictInsideTheTransaction(t *testing.T) {
	f := newFixture(t, gorangetask.ClientConfig{}, nil)
	ctx := context.Background()

	task := testTask("orders")
	task.IdempotencyKey = "once-only"

	err := f.gormDB.WithContext(ctx).Transaction(func(tx *gormio.DB) error {
		if _, err := f.adapter.EnqueueTx(ctx, tx, task); err != nil {
			return err
		}
		_, err := f.adapter.EnqueueTx(ctx, tx, task)
		return err
	})
	if !errors.Is(err, gorangetask.ErrDuplicateTask) {
		t.Fatalf("second EnqueueTx error = %v, want ErrDuplicateTask", err)
	}
	// 事务因返回错误而回滚 ⇒ 库里一条都不该留下。
	if n := f.countRows(t, tasksTable); n != 0 {
		t.Errorf("task rows = %d after rollback, want 0", n)
	}
}

// TestEnqueueTxWorksWithPreparedStatements 覆盖 PrepareStmt 模式。
//
// 它存在的理由是 txConnPool 的判据：那里用"不是 *sql.DB"来认定"在事务里"，
// 而开启 PrepareStmt 之后事务里的连接池是 gorm 自己的 *PreparedStmtTX 包装，
// 并不是 *sql.Tx。这条用例确认那种正当用法不会被误伤。
func TestEnqueueTxWorksWithPreparedStatements(t *testing.T) {
	f := newFixture(t, gorangetask.ClientConfig{}, &gormio.Config{PrepareStmt: true})
	ctx := context.Background()

	var taskID gorangetask.TaskID
	err := f.gormDB.WithContext(ctx).Transaction(func(tx *gormio.DB) error {
		if err := tx.Create(&Order{Ref: "o-prepared"}).Error; err != nil {
			return err
		}
		id, err := f.adapter.EnqueueTx(ctx, tx, testTask("orders"))
		if err != nil {
			return err
		}
		taskID = id
		return nil
	})
	if err != nil {
		t.Fatalf("Transaction with PrepareStmt: %v", err)
	}

	if _, err := f.client.Get(ctx, taskID); err != nil {
		t.Errorf("Get(%d): %v", taskID, err)
	}
	if n := f.countRows(t, tasksTable); n != 1 {
		t.Errorf("task rows = %d, want 1", n)
	}
}

// TestNewRejectsNilClient 是构造期的输入校验。
func TestNewRejectsNilClient(t *testing.T) {
	if _, err := New(nil); !errors.Is(err, gorangetask.ErrInvalidConfig) {
		t.Fatalf("New(nil) error = %v, want ErrInvalidConfig", err)
	}
}
