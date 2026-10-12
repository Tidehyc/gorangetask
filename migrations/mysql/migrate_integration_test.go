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
//	export GOTASK_TEST_MYSQL_DSN='root:rootpw@tcp(127.0.0.1:3306)/gotask_test?parseTime=true&loc=UTC'
//
// 未设置时全部跳过，因此普通的 go test ./... 不受影响。
// 测试会删除该库中的 gotask_tasks 与 gotask_schema_migrations 表。

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
	if version != 1 {
		t.Fatalf("CurrentVersion() = %d, want 1", version)
	}

	// 再次运行必须安全且不重复应用。
	if err := Migrate(ctx, db); err != nil {
		t.Fatalf("second Migrate() failed (not idempotent): %v", err)
	}
	version, err = CurrentVersion(ctx, db)
	if err != nil {
		t.Fatalf("CurrentVersion() after second run failed: %v", err)
	}
	if version != 1 {
		t.Fatalf("CurrentVersion() after second run = %d, want 1", version)
	}

	// 记录表里只应有一条版本记录。
	var rows int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM `+migrationsTable).Scan(&rows); err != nil {
		t.Fatalf("count migration rows: %v", err)
	}
	if rows != 1 {
		t.Fatalf("migration table has %d rows, want 1", rows)
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
	if v, err := CurrentVersion(ctx, db); err != nil || v != 1 {
		t.Fatalf("CurrentVersion() after recovery = %d, %v; want 1, nil", v, err)
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
	if rows != 1 {
		t.Fatalf("migration table has %d rows after concurrent runs, want 1", rows)
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
