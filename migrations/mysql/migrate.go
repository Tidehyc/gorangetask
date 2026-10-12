// Package mysqlmigrations 提供 GoRangeTask 的 MySQL 版本化数据库迁移。
//
// 迁移 SQL 通过 embed 打包进二进制，调用方无需在部署时携带 .sql 文件。
// 迁移必须由调用方显式触发，不会在应用启动时自动执行。
//
// 典型用法：
//
//	db, err := sql.Open("mysql", dsn)
//	if err != nil { ... }
//	if err := mysqlmigrations.Migrate(ctx, db); err != nil {
//		log.Fatalf("migrate: %v", err)
//	}
//
// 机制说明、MySQL 的 DDL 隐式提交与脏状态、advisory lock 的处理，
// 见仓库内 docs/GoRangeTask-迁移机制.md。
//
// 已知限制：
//   - 只支持向前迁移（不做 down 回滚）。MySQL 的 DDL 无法回滚，
//     需要撤销变更时应新增一个反向迁移。
//   - 仅验证 MySQL 8.0+。
package mysqlmigrations

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

//go:embed *.sql
var sqlFS embed.FS

const (
	// migrationsTable 是记录已应用版本的元数据表。
	migrationsTable = "gotask_schema_migrations"

	// advisoryLockName 是迁移期间持有的 MySQL 咨询锁名称。
	// 通过 GET_LOCK 获取，保证多实例同时启动时只有一个在迁移。
	advisoryLockName = "gorangetask_migrate"

	// advisoryLockTimeoutSec 是获取咨询锁的最长等待秒数。
	advisoryLockTimeoutSec = 10

	// releaseLockTimeout 是释放咨询锁的超时。
	releaseLockTimeout = 5 * time.Second
)

// 迁移相关的可判断错误。
var (
	// ErrDirty 表示数据库中有迁移失败留下的脏记录，必须先人工处理。
	ErrDirty = errors.New("gorangetask/migrate: database is in a dirty state")

	// ErrLockTimeout 表示未能在超时内获取迁移咨询锁，通常意味着另一个实例正在迁移。
	ErrLockTimeout = errors.New("gorangetask/migrate: could not acquire migration lock")

	// ErrInvalidMigrationFile 表示迁移文件命名或内容不符合约定。
	ErrInvalidMigrationFile = errors.New("gorangetask/migrate: invalid migration file")
)

// createMigrationsTableSQL 建立版本记录表。dirty 列的语义见 docs/GoRangeTask-迁移机制.md §4。
const createMigrationsTableSQL = `CREATE TABLE IF NOT EXISTS ` + migrationsTable + ` (
    version    BIGINT       NOT NULL,
    name       VARCHAR(255) NOT NULL,
    applied_at DATETIME(6)  NOT NULL,
    dirty      TINYINT(1)   NOT NULL DEFAULT 0,
    PRIMARY KEY (version)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci`

// migration 是一个已解析的迁移文件。
type migration struct {
	version int64
	name    string
	stmts   []string
}

// migrationFilePattern 匹配 "0001_create_tasks_table.sql" 这样的文件名。
// 版本号取前缀数字，名字取其余部分（允许下划线）。
var migrationFilePattern = regexp.MustCompile(`^(\d+)_(.+)\.sql$`)

// Migrate 把数据库结构升级到迁移文件的最新版本。
//
// 行为：
//   - 幂等：已应用的版本会被跳过，重复调用安全。
//   - 串行：迁移期间持有 MySQL 咨询锁，多实例并发调用只有一个真正执行。
//   - 失败即停：任一语句失败会返回带上下文的错误，且不再继续后续版本。
//   - 脏状态保护：发现上一次迁移失败留下的标记时拒绝执行，要求人工处理。
//
// Migrate 不会提交或回滚调用方的事务；它自行管理连接与语句执行。
func Migrate(ctx context.Context, db *sql.DB) error {
	// GET_LOCK 是会话级的，必须在同一条连接上完成取锁、执行与释放。
	// 用 db.Conn 从连接池取出一条连接并独占，避免中途换连接导致锁失效。
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("gorangetask/migrate: acquire connection: %w", err)
	}
	defer conn.Close()

	if err := acquireLock(ctx, conn); err != nil {
		return err
	}
	// 即使 ctx 已被取消也要释放锁。连接归还池中不会自动断开，
	// 不显式释放会让后续迁移一直拿不到锁。
	defer func() {
		rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), releaseLockTimeout)
		defer cancel()
		_, _ = conn.ExecContext(rctx, "DO RELEASE_LOCK(?)", advisoryLockName)
	}()

	if _, err := conn.ExecContext(ctx, createMigrationsTableSQL); err != nil {
		return fmt.Errorf("gorangetask/migrate: create migration table: %w", err)
	}

	if err := checkDirty(ctx, conn); err != nil {
		return err
	}

	applied, err := appliedVersions(ctx, conn)
	if err != nil {
		return err
	}

	all, err := loadMigrations(sqlFS)
	if err != nil {
		return err
	}

	for _, m := range all {
		if _, ok := applied[m.version]; ok {
			continue
		}
		if err := applyMigration(ctx, conn, m); err != nil {
			return err
		}
	}
	return nil
}

// CurrentVersion 返回已成功应用的最高版本号；没有任何迁移或元数据表尚不存在时返回 0。
//
// 它只读取元数据表，不修改数据库；如果表不存在则视为 0，不报错。
func CurrentVersion(ctx context.Context, db *sql.DB) (int64, error) {
	var exists int
	err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM information_schema.tables
		  WHERE table_schema = DATABASE() AND table_name = ?`, migrationsTable).Scan(&exists)
	if err != nil {
		return 0, fmt.Errorf("gorangetask/migrate: probe migration table: %w", err)
	}
	if exists == 0 {
		return 0, nil
	}

	var version sql.NullInt64
	err = db.QueryRowContext(ctx,
		`SELECT MAX(version) FROM `+migrationsTable+` WHERE dirty = 0`).Scan(&version)
	if err != nil {
		return 0, fmt.Errorf("gorangetask/migrate: read current version: %w", err)
	}
	if !version.Valid {
		return 0, nil
	}
	return version.Int64, nil
}

// acquireLock 获取迁移咨询锁。返回 1 表示拿到；0 表示超时；NULL 表示出错。
func acquireLock(ctx context.Context, conn *sql.Conn) error {
	var got sql.NullInt64
	if err := conn.QueryRowContext(ctx,
		"SELECT GET_LOCK(?, ?)", advisoryLockName, advisoryLockTimeoutSec).Scan(&got); err != nil {
		return fmt.Errorf("gorangetask/migrate: acquire lock: %w", err)
	}
	if !got.Valid || got.Int64 != 1 {
		return fmt.Errorf("%w: another instance may be running migrations", ErrLockTimeout)
	}
	return nil
}

// checkDirty 检查是否存在上一次迁移失败留下的脏记录。
func checkDirty(ctx context.Context, conn *sql.Conn) error {
	rows, err := conn.QueryContext(ctx,
		`SELECT version, name FROM `+migrationsTable+` WHERE dirty = 1 ORDER BY version`)
	if err != nil {
		return fmt.Errorf("gorangetask/migrate: check dirty state: %w", err)
	}
	defer rows.Close()

	var dirty []string
	for rows.Next() {
		var version int64
		var name string
		if err := rows.Scan(&version, &name); err != nil {
			return fmt.Errorf("gorangetask/migrate: scan dirty row: %w", err)
		}
		dirty = append(dirty, fmt.Sprintf("%d (%s)", version, name))
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("gorangetask/migrate: iterate dirty rows: %w", err)
	}
	if len(dirty) > 0 {
		return fmt.Errorf("%w: affected version(s): %s; inspect the actual schema, "+
			"align it with expectations, then clear the dirty flag", ErrDirty, strings.Join(dirty, ", "))
	}
	return nil
}

// appliedVersions 返回已成功应用的版本集合。
func appliedVersions(ctx context.Context, conn *sql.Conn) (map[int64]struct{}, error) {
	rows, err := conn.QueryContext(ctx,
		`SELECT version FROM `+migrationsTable+` WHERE dirty = 0`)
	if err != nil {
		return nil, fmt.Errorf("gorangetask/migrate: read applied versions: %w", err)
	}
	defer rows.Close()

	applied := make(map[int64]struct{})
	for rows.Next() {
		var version int64
		if err := rows.Scan(&version); err != nil {
			return nil, fmt.Errorf("gorangetask/migrate: scan applied version: %w", err)
		}
		applied[version] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("gorangetask/migrate: iterate applied versions: %w", err)
	}
	return applied, nil
}

// applyMigration 应用单个迁移：先写脏标记，执行全部语句，成功后再清除标记。
//
// 这样无论进程在哪一步崩溃，元数据表都会留下"该版本未完成"的痕迹，
// 下次运行时 checkDirty 会拒绝继续，避免把失败静默掩盖。
func applyMigration(ctx context.Context, conn *sql.Conn, m migration) error {
	if _, err := conn.ExecContext(ctx,
		`INSERT INTO `+migrationsTable+` (version, name, applied_at, dirty)
		 VALUES (?, ?, UTC_TIMESTAMP(6), 1)`, m.version, m.name); err != nil {
		return fmt.Errorf("gorangetask/migrate: record start of version %d: %w", m.version, err)
	}

	for i, stmt := range m.stmts {
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("gorangetask/migrate: version %d (%s) statement %d/%d failed: %w; "+
				"MySQL DDL cannot be rolled back, the schema may be partially changed — "+
				"manual intervention required", m.version, m.name, i+1, len(m.stmts), err)
		}
	}

	if _, err := conn.ExecContext(ctx,
		`UPDATE `+migrationsTable+` SET dirty = 0 WHERE version = ?`, m.version); err != nil {
		return fmt.Errorf("gorangetask/migrate: finalize version %d: %w", m.version, err)
	}
	return nil
}

// loadMigrations 从 fsys 读取并解析全部迁移文件，按版本号升序返回。
func loadMigrations(fsys fs.FS) ([]migration, error) {
	entries, err := fs.ReadDir(fsys, ".")
	if err != nil {
		return nil, fmt.Errorf("gorangetask/migrate: read migrations: %w", err)
	}

	var migrations []migration
	seen := make(map[int64]string)

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}

		match := migrationFilePattern.FindStringSubmatch(entry.Name())
		if match == nil {
			return nil, fmt.Errorf("%w: %q does not match <version>_<name>.sql",
				ErrInvalidMigrationFile, entry.Name())
		}

		version, err := strconv.ParseInt(match[1], 10, 64)
		if err != nil || version <= 0 {
			return nil, fmt.Errorf("%w: %q has an invalid version number",
				ErrInvalidMigrationFile, entry.Name())
		}
		if prev, dup := seen[version]; dup {
			return nil, fmt.Errorf("%w: duplicate version %d in %q and %q",
				ErrInvalidMigrationFile, version, prev, entry.Name())
		}
		seen[version] = entry.Name()

		content, err := fs.ReadFile(fsys, entry.Name())
		if err != nil {
			return nil, fmt.Errorf("gorangetask/migrate: read %q: %w", entry.Name(), err)
		}
		stmts := splitStatements(string(content))
		if len(stmts) == 0 {
			return nil, fmt.Errorf("%w: %q contains no statements",
				ErrInvalidMigrationFile, entry.Name())
		}

		migrations = append(migrations, migration{
			version: version,
			name:    match[2],
			stmts:   stmts,
		})
	}

	if len(migrations) == 0 {
		return nil, fmt.Errorf("%w: no migration files found", ErrInvalidMigrationFile)
	}

	// 按解析出的版本号排序，而不是文件名字典序。
	sort.Slice(migrations, func(i, j int) bool {
		return migrations[i].version < migrations[j].version
	})
	return migrations, nil
}

// splitStatements 把迁移文件内容切分为单条语句。
//
// 先去除注释，再按分号切分。迁移 SQL 由本项目手写，约定不在字符串字面量中
// 出现分号，因此这里不做完整的 SQL 词法分析。
func splitStatements(content string) []string {
	stripped := stripComments(content)

	var stmts []string
	for _, part := range strings.Split(stripped, ";") {
		if s := strings.TrimSpace(part); s != "" {
			stmts = append(stmts, s)
		}
	}
	return stmts
}

// stripComments 去掉 SQL 中的 -- 行注释、# 行注释和 /* */ 块注释。
func stripComments(content string) string {
	var b strings.Builder
	b.Grow(len(content))

	for i := 0; i < len(content); {
		switch {
		case strings.HasPrefix(content[i:], "--"), content[i] == '#':
			for i < len(content) && content[i] != '\n' {
				i++
			}
		case strings.HasPrefix(content[i:], "/*"):
			end := strings.Index(content[i+2:], "*/")
			if end < 0 {
				return b.String()
			}
			i += 2 + end + len("*/")
		default:
			b.WriteByte(content[i])
			i++
		}
	}
	return b.String()
}
