package mysqlmigrations

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"sync"
	"testing"

	_ "github.com/go-sql-driver/mysql"
)

// 集成测试需要一个可丢弃的 MySQL 数据库：
//
//	export GOTASK_TEST_MYSQL_DSN='root:rootpw@tcp(127.0.0.1:3306)/gotask?parseTime=true&loc=UTC'
//
// 未设置时全部跳过，因此普通的 go test ./... 不受影响。
// 测试会删除该库中的 gotask_tasks 与 gotask_schema_migrations 表。
//
// 本包**直接使用 DSN 里的库、不做派生**，并**不会创建该库**：迁移是"建表"
// 这件事的完整闭环，建库属于部署方的事。所以该库必须真实存在（本地由
// scripts/dev-mysql.sh up 创建，CI 由 service container 的 MYSQL_DATABASE 预设）。
// 库不存在时 sql.Open 不报错（惰性），要到下面的 PingContext 才以
// `Error 1049 Unknown database` 失败——报错文案看着像"迁移失败"，
// 实际是一条迁移语句都没跑。
//
// 并发安全：go test ./... 会并发运行不同的包，而本包 setup 是破坏性的
// （DROP TABLE + 重新 Migrate）。store/mysql 用派生库 <库名>_store 避开，
// 与本包不共用同一个库。

const testDSNEnv = "GOTASK_TEST_MYSQL_DSN"

func testDB(t *testing.T) *sql.DB {
	t.Helper()

	dsn := os.Getenv(testDSNEnv)
	if dsn == "" {
		t.Skipf("%s not set; skipping MySQL integration test", testDSNEnv)
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	ctx := context.Background()
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping database: %v", err)
	}

	for _, table := range []string{"gotask_tasks", migrationsTable} {
		if _, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS "+table); err != nil {
			t.Fatalf("drop table %s: %v", table, err)
		}
	}
	return db
}

func tableExists(t *testing.T, db *sql.DB, name string) bool {
	t.Helper()
	var n int
	err := db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM information_schema.tables
		  WHERE table_schema = DATABASE() AND table_name = ?`, name).Scan(&n)
	if err != nil {
		t.Fatalf("check table %s: %v", name, err)
	}
	return n > 0
}

// latestMigrationVersion 是 embed 进来的迁移里最高的版本号。
//
// 新增迁移文件时**必须同步上调**这个值——它正是本测试想强制的一件事：
// 迁移文件与"迁移全跑完后应该是什么版本"的期望必须一起改，不能只加文件。
const latestMigrationVersion = 2 // 0001 建表 + 0002 逐任务超时列

func TestMigrateIntegrationAppliesAndIsIdempotent(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()

	if err := Migrate(ctx, db); err != nil {
		t.Fatalf("first Migrate() failed: %v", err)
	}
	if !tableExists(t, db, "gotask_tasks") {
		t.Fatal("gotask_tasks was not created")
	}

	version, err := CurrentVersion(ctx, db)
	if err != nil {
		t.Fatalf("CurrentVersion() failed: %v", err)
	}
	if version != latestMigrationVersion {
		t.Fatalf("CurrentVersion() = %d, want %d", version, latestMigrationVersion)
	}

	// 再次运行必须安全且不重复应用。
	if err := Migrate(ctx, db); err != nil {
		t.Fatalf("second Migrate() failed (not idempotent): %v", err)
	}
	version, err = CurrentVersion(ctx, db)
	if err != nil {
		t.Fatalf("CurrentVersion() after second run failed: %v", err)
	}
	if version != latestMigrationVersion {
		t.Fatalf("CurrentVersion() after second run = %d, want %d", version, latestMigrationVersion)
	}

	// 记录表里每个版本恰好一条记录。
	var rows int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM `+migrationsTable).Scan(&rows); err != nil {
		t.Fatalf("count migration rows: %v", err)
	}
	if int64(rows) != latestMigrationVersion {
		t.Fatalf("migration table has %d rows, want %d", rows, latestMigrationVersion)
	}
}

func TestMigrateIntegrationCreatesExpectedIndexes(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()

	if err := Migrate(ctx, db); err != nil {
		t.Fatalf("Migrate() failed: %v", err)
	}

	rows, err := db.QueryContext(ctx,
		`SELECT DISTINCT index_name FROM information_schema.statistics
		  WHERE table_schema = DATABASE() AND table_name = 'gotask_tasks'`)
	if err != nil {
		t.Fatalf("read indexes: %v", err)
	}
	defer rows.Close()

	got := make(map[string]bool)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan index name: %v", err)
		}
		got[name] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate indexes: %v", err)
	}

	for _, want := range []string{
		"PRIMARY", "uq_gotask_queue_idempotency",
		"ix_gotask_claim", "ix_gotask_lease", "ix_gotask_cleanup",
	} {
		if !got[want] {
			t.Errorf("expected index %q not found; got %v", want, got)
		}
	}
}

// TestMigrateIntegrationAddsTaskTimeoutColumn 固定 0002 的结果：timeout_ns 存在、
// 可为 NULL（NULL 表示"未设置，用 Worker 默认超时"）、类型是 BIGINT UNSIGNED
// （纳秒口径下 time.Duration 的整个取值范围都能装下，不会截断）。
func TestMigrateIntegrationAddsTaskTimeoutColumn(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()

	if err := Migrate(ctx, db); err != nil {
		t.Fatalf("Migrate() failed: %v", err)
	}

	var columnType, nullable string
	err := db.QueryRowContext(ctx,
		`SELECT COLUMN_TYPE, IS_NULLABLE FROM information_schema.columns
		  WHERE table_schema = DATABASE() AND table_name = 'gotask_tasks'
		    AND column_name = 'timeout_ns'`).Scan(&columnType, &nullable)
	if errors.Is(err, sql.ErrNoRows) {
		t.Fatal("column timeout_ns not found; migration 0002 did not apply")
	}
	if err != nil {
		t.Fatalf("read timeout_ns column: %v", err)
	}
	if columnType != "bigint unsigned" {
		t.Errorf("timeout_ns COLUMN_TYPE = %q, want bigint unsigned", columnType)
	}
	if nullable != "YES" {
		t.Errorf("timeout_ns IS_NULLABLE = %q, want YES (NULL 表示未设置)", nullable)
	}
}

func TestMigrateIntegrationRejectsDirtyState(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()

	if err := Migrate(ctx, db); err != nil {
		t.Fatalf("Migrate() failed: %v", err)
	}

	// 模拟上一次迁移失败留下的痕迹。
	if _, err := db.ExecContext(ctx,
		`UPDATE `+migrationsTable+` SET dirty = 1 WHERE version = 1`); err != nil {
		t.Fatalf("set dirty flag: %v", err)
	}

	err := Migrate(ctx, db)
	if !errors.Is(err, ErrDirty) {
		t.Fatalf("Migrate() error = %v, want ErrDirty", err)
	}
}

// TestMigrateIntegrationMarksDirtyOnFailure 验证"先标记、成功后再消除"的脏状态机制：
// 迁移语句真实失败时留下 dirty=1 的记录，后续运行被拒绝，人工清理后可以恢复。
func TestMigrateIntegrationMarksDirtyOnFailure(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()

	// 预先建一张同名表，使 0001 的 CREATE TABLE 必然失败。
	if _, err := db.ExecContext(ctx, `CREATE TABLE gotask_tasks (id INT)`); err != nil {
		t.Fatalf("pre-create conflicting table: %v", err)
	}

	err := Migrate(ctx, db)
	if err == nil {
		t.Fatal("Migrate() = nil, want failure")
	}
	if errors.Is(err, ErrDirty) {
		t.Fatalf("first run should surface the SQL error, not ErrDirty: %v", err)
	}

	var dirty int
	if err := db.QueryRowContext(ctx,
		`SELECT dirty FROM `+migrationsTable+` WHERE version = 1`).Scan(&dirty); err != nil {
		t.Fatalf("read dirty flag after failed migration: %v", err)
	}
	if dirty != 1 {
		t.Fatalf("dirty = %d, want 1 after a failed migration", dirty)
	}

	if err := Migrate(ctx, db); !errors.Is(err, ErrDirty) {
		t.Fatalf("second Migrate() error = %v, want ErrDirty", err)
	}

	// 模拟人工恢复：修正结构、清除失败记录，然后重跑。
	if _, err := db.ExecContext(ctx, `DROP TABLE gotask_tasks`); err != nil {
		t.Fatalf("drop conflicting table: %v", err)
	}
	if _, err := db.ExecContext(ctx, `DELETE FROM `+migrationsTable+` WHERE version = 1`); err != nil {
		t.Fatalf("clear failed version row: %v", err)
	}
	if err := Migrate(ctx, db); err != nil {
		t.Fatalf("Migrate() after manual recovery failed: %v", err)
	}
	if v, err := CurrentVersion(ctx, db); err != nil || v != latestMigrationVersion {
		t.Fatalf("CurrentVersion() after recovery = %d, %v; want %d, nil", v, err, latestMigrationVersion)
	}
}

func TestMigrateIntegrationConcurrentCallsAreSerialized(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()

	const callers = 4
	errs := make([]error, callers)

	var wg sync.WaitGroup
	wg.Add(callers)
	for i := 0; i < callers; i++ {
		go func(i int) {
			defer wg.Done()
			errs[i] = Migrate(ctx, db)
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("concurrent Migrate() caller %d failed: %v", i, err)
		}
	}

	var rows int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM `+migrationsTable).Scan(&rows); err != nil {
		t.Fatalf("count migration rows: %v", err)
	}
	if int64(rows) != latestMigrationVersion {
		t.Fatalf("migration table has %d rows after concurrent runs, want %d", rows, latestMigrationVersion)
	}
}

func TestCurrentVersionWithoutMigrationTable(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()

	version, err := CurrentVersion(ctx, db)
	if err != nil {
		t.Fatalf("CurrentVersion() on empty database failed: %v", err)
	}
	if version != 0 {
		t.Fatalf("CurrentVersion() on empty database = %d, want 0", version)
	}
}
