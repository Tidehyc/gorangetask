package basic

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"time"

	gorangetask "github.com/Tidehyc/gorangetask"
	mysqlmigrations "github.com/Tidehyc/gorangetask/migrations/mysql"
	mysqlstore "github.com/Tidehyc/gorangetask/store/mysql"

	// 示例自己负责打开数据库连接，因此驱动程序必须在这里注册。
	_ "github.com/go-sql-driver/mysql"
)

//go:embed schema.sql
var schemaSQL string

// Options 是装配示例的参数。零值可直接使用。
type Options struct {
	// HandlerDelay 非零时，导出 Handler 在写回之前先等这么久。
	//
	// 它只服务于演示：让"任务正在处理时进程被停掉"这件事可以被手工复现
	// （见 README 的故障演示）。真实的 Handler 不需要这种开关。
	HandlerDelay time.Duration

	// Config 覆盖 Worker 配置。零值字段由库的 WithDefaults 填默认值；
	// Queues 留空时只监听示例队列 Queue。
	//
	// 想让演示跑得快（或想复现"租约过期后任务被接手"）时，用它把
	// LeaseDuration / ReapInterval / PollInterval 调小。
	Config gorangetask.Config

	// ClientConfig 覆盖入队配置。零值字段由库的 WithDefaults 填默认值。
	ClientConfig gorangetask.ClientConfig
}

// App 把示例用得到的部件装在一起：数据库、GoRangeTask Client、业务仓储与 Worker。
type App struct {
	DB     *sql.DB
	Client gorangetask.Client
	Store  *Store
	Worker *gorangetask.Worker
}

// Open 打开数据库、建好两张表，并装配出一个可以直接 Run 的 App。
//
// 建表顺序是刻意的，也是使用方接入时要照做的那一套：
//
//  1. **迁移任务表**：gotask_tasks 属于库，由 migrations/mysql 建与管版本；
//  2. **建业务表**：业务表属于使用方，库一无所知（示例用 schema.sql）；
//  3. **构造 store**：store 只读表、不建表，所以必须排在两步建表之后。
//
// dsn 必须包含数据库名。库不替调用方创建数据库本身。
func Open(ctx context.Context, dsn string, opts Options) (*App, error) {
	if dsn == "" {
		return nil, errors.New("example/basic: DSN must not be empty")
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("example/basic: open database: %w", err)
	}
	// Open 是惰性的，第一次真正连库在这里。连不上就没必要继续建表。
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("example/basic: ping database: %w", err)
	}

	if err := migrateSchema(ctx, db); err != nil {
		_ = db.Close()
		return nil, err
	}

	client, err := mysqlstore.New(db, opts.ClientConfig)
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("example/basic: build task client: %w", err)
	}

	cfg := opts.Config
	if len(cfg.Queues) == 0 {
		cfg.Queues = []string{Queue}
	}
	registry := gorangetask.NewRegistry()
	if err := registry.Register(TaskTypeExportOrder, ExportOrderHandler(db, opts.HandlerDelay)); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("example/basic: register %s handler: %w", TaskTypeExportOrder, err)
	}
	worker, err := gorangetask.NewWorker(client, registry, cfg)
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("example/basic: build worker: %w", err)
	}

	return &App{
		DB:     db,
		Client: client,
		Store:  NewStore(db, client),
		Worker: worker,
	}, nil
}

// migrateSchema 建库自己的任务表，再建示例的业务表。
func migrateSchema(ctx context.Context, db *sql.DB) error {
	if err := mysqlmigrations.Migrate(ctx, db); err != nil {
		return fmt.Errorf("example/basic: migrate task table: %w", err)
	}
	// schema.sql 只有一条语句，因此整段交给驱动执行；多语句 DDL 请用
	// migrations/mysql 的分句实现。
	if _, err := db.ExecContext(ctx, schemaSQL); err != nil {
		return fmt.Errorf("example/basic: create %s: %w", ordersTable, err)
	}
	return nil
}

// Run 启动 Worker 直到 ctx 被取消，然后优雅停机。它等价于直接调用
// App.Worker.Run，包一层只是为了让 demo 的入口读起来更短。
func (a *App) Run(ctx context.Context) error {
	return a.Worker.Run(ctx)
}

// Close 释放数据库连接。
func (a *App) Close() error {
	return a.DB.Close()
}
