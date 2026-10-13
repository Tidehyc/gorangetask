// Package mysql 是 GoRangeTask 的 MySQL 方言实现。
//
// 它实现根包 gorangetask 的 Client 接口（入队与状态查询），是唯一直接书写
// SQL 的地方；根包只保留与数据库无关的公开契约。本包按需引入
// github.com/go-sql-driver/mysql：既用于识别唯一键冲突，也顺带完成驱动注册，
// 因此调用方无需额外 import 该驱动。
//
// 表结构由 migrations/mysql 建立，本包不建表、不迁移。
//
// 典型用法：
//
//	db, err := sql.Open("mysql", dsn)
//	if err != nil { ... }
//	if err := mysqlmigrations.Migrate(ctx, db); err != nil { ... }
//
//	client, err := mysqlstore.New(db, gorangetask.ClientConfig{})
//	if err != nil { ... }
//	id, err := client.Enqueue(ctx, gorangetask.Task{Type: "send_email"})
package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
	"unicode/utf8"

	gomysql "github.com/go-sql-driver/mysql"

	gorangetask "github.com/Tidehyc/gorangetask"
)

const (
	// tasksTable 是任务表名，与 migrations/mysql 的 DDL 保持一致。
	tasksTable = "gotask_tasks"

	// 列宽上限，单位是字符而不是字节：MySQL 的 VARCHAR(n) 在 utf8mb4 下按字符计数。
	// 在 Go 侧先校验，是为了让调用方拿到可判断的 ErrInvalidTask，
	// 而不是 MySQL 严格模式下截断/超长的 1406 错误。
	maxQueueNameLen      = 64  // queue_name  VARCHAR(64)
	maxTaskTypeLen       = 128 // task_type   VARCHAR(128)
	maxIdempotencyKeyLen = 191 // idempotency_key VARCHAR(191)

	// dbTimeLayout 是写入 DATETIME(6) 的固定格式。
	//
	// run_at 以字符串而非 time.Time 传入：go-sql-driver 会按 DSN 里的 loc
	// 格式化 time.Time，调用方若把 loc 设成非 UTC，写入值就会和 SQL 侧的
	// UTC_TIMESTAMP(6) 差一个时区。格式化成 UTC 字符串可以彻底避开这个坑，
	// 也让"库里存的一定是 UTC"这件事不依赖 DSN 配置。
	dbTimeLayout = "2006-01-02 15:04:05.000000"

	// mysqlErrDuplicateEntry 是 MySQL 唯一键冲突的错误号。
	mysqlErrDuplicateEntry = 1062
)

// insertTaskSQL 插入一条 pending 任务，参数依次为：
// queue_name、task_type、payload、run_at、max_attempts、idempotency_key、timeout_ns。
//
// run_at 用 COALESCE(?, UTC_TIMESTAMP(6))：传 NULL 表示"立即执行"，
// 由数据库时间决定，避免依赖应用机器的时钟（技术设计 §3.2）。
// timeout_ns 传 NULL 表示"未设置逐任务超时"，由 Worker 回落到配置的默认值。
// status / attempt / lease_epoch / created_at / updated_at 都是新任务的
// 固定初值，硬编码在 SQL 里，不给调用方留出错空间。
const insertTaskSQL = `INSERT INTO ` + tasksTable + ` (
    queue_name, task_type, payload, status, run_at,
    attempt, max_attempts, lease_epoch, idempotency_key, timeout_ns,
    created_at, updated_at
) VALUES (?, ?, CAST(? AS JSON), 'pending', COALESCE(?, UTC_TIMESTAMP(6)),
    0, ?, 0, ?, ?, UTC_TIMESTAMP(6), UTC_TIMESTAMP(6))`

// execer 是 insert 需要的最小执行能力。*sql.DB 与 *sql.Tx 都满足它，
// 因此 Enqueue（库自己取连接）与 EnqueueTx（复用调用方事务）可以共用同一段代码。
type execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// Client 是 gorangetask.Client 的 MySQL 实现。
//
// 它不持有连接，只持有 *sql.DB；连接池的生命周期由调用方管理，
// 因此 Client 没有 Close。
type Client struct {
	db  *sql.DB
	cfg gorangetask.ClientConfig
}

// 编译期断言：Client 完整实现了根包的公开接口。
var _ gorangetask.Client = (*Client)(nil)

// New 构造一个 MySQL Client。db 不能为 nil。
//
// cfg 会先经 WithDefaults 填充再由 Validate 校验；此外本包会额外校验
// cfg.DefaultQueue 是否超过 MySQL 列宽——这是方言才知道的约束，根包不关心。
func New(db *sql.DB, cfg gorangetask.ClientConfig) (*Client, error) {
	if db == nil {
		return nil, fmt.Errorf("%w: nil *sql.DB", gorangetask.ErrInvalidConfig)
	}
	cfg = cfg.WithDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if n := utf8.RuneCountInString(cfg.DefaultQueue); n > maxQueueNameLen {
		return nil, fmt.Errorf("%w: client default queue is %d characters, MySQL allows %d",
			gorangetask.ErrInvalidConfig, n, maxQueueNameLen)
	}
	return &Client{db: db, cfg: cfg}, nil
}

// Enqueue 在库自己取的连接上插入一条新任务，返回其 ID。
//
// 单条 INSERT 自身即原子，因此不额外开启事务。任务插入后状态为 pending。
// 幂等键冲突时返回 ErrDuplicateTask，不返回既有任务 ID（见根包 ErrDuplicateTask）。
func (c *Client) Enqueue(ctx context.Context, task gorangetask.Task) (gorangetask.TaskID, error) {
	return c.insert(ctx, c.db, task)
}

// EnqueueTx 在调用方传入的事务执行器上插入一条新任务，返回其 ID。
//
// 本方法不开启、不提交、不回滚该事务，也不另取连接：任务行与调用方的业务
// 变更同生共死——事务提交则任务可见，回滚则任务不存在。
// 因此返回的 ID 在事务提交前不具备"任务已存在"的含义。
func (c *Client) EnqueueTx(ctx context.Context, tx gorangetask.Tx, task gorangetask.Task) (gorangetask.TaskID, error) {
	if tx == nil {
		return 0, fmt.Errorf("%w: nil transaction executor", gorangetask.ErrInvalidTask)
	}
	return c.insert(ctx, tx, task)
}

// insert 是 Enqueue 与 EnqueueTx 的共用实现：先校验并归一化，再执行同一条 INSERT。
func (c *Client) insert(ctx context.Context, q execer, task gorangetask.Task) (gorangetask.TaskID, error) {
	p, err := c.prepare(task)
	if err != nil {
		return 0, err
	}

	res, err := q.ExecContext(ctx, insertTaskSQL,
		p.queue, p.taskType, []byte(p.payload), p.runAt, p.maxAttempts,
		p.idempotencyKey, p.timeoutNs)
	if err != nil {
		if isDuplicateEntry(err) {
			return 0, fmt.Errorf("%w: queue %q already has a task with idempotency key %q",
				gorangetask.ErrDuplicateTask, p.queue, p.task.IdempotencyKey)
		}
		return 0, fmt.Errorf("gorangetask/store/mysql: insert task: %w", err)
	}

	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("gorangetask/store/mysql: read inserted task id: %w", err)
	}
	return gorangetask.TaskID(id), nil
}

// preparedTask 是一条入队请求经校验、归一化后、可直接绑定到 INSERT 的结果。
type preparedTask struct {
	task gorangetask.Task

	queue          string
	taskType       string
	payload        json.RawMessage
	runAt          any // nil 表示立即执行，交由 SQL 用 UTC_TIMESTAMP(6) 填
	maxAttempts    int
	idempotencyKey any // nil 表示无幂等键，必须是 NULL 而不是空串（见下）
	timeoutNs      any // nil 表示未设置逐任务超时，由 Worker 回落默认值
}

// prepare 校验任务定义并归一化缺省值。所有失败都返回可用 errors.Is 判断的错误。
func (c *Client) prepare(task gorangetask.Task) (preparedTask, error) {
	p := preparedTask{task: task}

	// —— queue：空值落到配置默认队列 ——
	p.queue = task.Queue
	if p.queue == "" {
		p.queue = c.cfg.DefaultQueue
	}
	if n := utf8.RuneCountInString(p.queue); n > maxQueueNameLen {
		return preparedTask{}, fmt.Errorf("%w: queue name is %d characters, MySQL allows %d",
			gorangetask.ErrInvalidTask, n, maxQueueNameLen)
	}

	// —— type：必填 ——
	if task.Type == "" {
		return preparedTask{}, fmt.Errorf("%w: task type is required", gorangetask.ErrInvalidTask)
	}
	if n := utf8.RuneCountInString(task.Type); n > maxTaskTypeLen {
		return preparedTask{}, fmt.Errorf("%w: task type is %d characters, MySQL allows %d",
			gorangetask.ErrInvalidTask, n, maxTaskTypeLen)
	}
	p.taskType = task.Type

	// —— payload：必填、必须是合法 JSON、不超过上限 ——
	// 空 payload 直接拒绝而不是替他写成 null：payload 列是 NOT NULL 的 JSON，
	// 把"忘了填"和"故意无参数"混为一谈会让问题更难查。
	if len(task.Payload) == 0 {
		return preparedTask{}, fmt.Errorf("%w: payload is required", gorangetask.ErrInvalidPayload)
	}
	if len(task.Payload) > c.cfg.MaxPayloadSize {
		return preparedTask{}, fmt.Errorf("%w: payload is %d bytes, limit is %d bytes",
			gorangetask.ErrInvalidPayload, len(task.Payload), c.cfg.MaxPayloadSize)
	}
	if !json.Valid(task.Payload) {
		return preparedTask{}, fmt.Errorf("%w: payload is not valid JSON", gorangetask.ErrInvalidPayload)
	}
	p.payload = task.Payload

	// —— run_at：零值交给数据库时间 ——
	if !task.RunAt.IsZero() {
		p.runAt = task.RunAt.UTC().Format(dbTimeLayout)
	}

	// —— max_attempts：零值落默认，负数拒绝 ——
	switch {
	case task.MaxAttempts == 0:
		p.maxAttempts = c.cfg.DefaultMaxAttempts
	case task.MaxAttempts < 0:
		return preparedTask{}, fmt.Errorf("%w: max attempts must be > 0, got %d",
			gorangetask.ErrInvalidTask, task.MaxAttempts)
	default:
		p.maxAttempts = task.MaxAttempts
	}

	// —— timeout_ns：零值落 NULL（按文档即"未设置，用 Worker 默认"），负值拒绝 ——
	// 存 NULL 而不是 0：读取方把 0 解释为"未设置"，所以 0 本来就没有独立语义，
	// 两者共用 NULL 这一种表示即可。纳秒口径下 time.Duration 不会被截断。
	if task.Timeout < 0 {
		return preparedTask{}, fmt.Errorf("%w: timeout must not be negative, got %s",
			gorangetask.ErrInvalidTask, task.Timeout)
	}
	if task.Timeout > 0 {
		p.timeoutNs = int64(task.Timeout)
	}

	// —— idempotency_key：空串必须落成 NULL ——
	// 唯一索引是 (queue_name, idempotency_key)。MySQL 的唯一索引不约束 NULL，
	// 但会把空串当成一个真实取值：若把 "" 写进去，同一队列的第二条无幂等键
	// 任务就会撞上唯一键被误判为重复。所以这里必须区分"没有键"和"键是空串"。
	if task.IdempotencyKey != "" {
		if n := utf8.RuneCountInString(task.IdempotencyKey); n > maxIdempotencyKeyLen {
			return preparedTask{}, fmt.Errorf("%w: idempotency key is %d characters, MySQL allows %d",
				gorangetask.ErrInvalidTask, n, maxIdempotencyKeyLen)
		}
		p.idempotencyKey = task.IdempotencyKey
	}

	return p, nil
}

// isDuplicateEntry 报告错误是否为 MySQL 唯一键冲突。
func isDuplicateEntry(err error) bool {
	var me *gomysql.MySQLError
	if errors.As(err, &me) {
		return me.Number == mysqlErrDuplicateEntry
	}
	return false
}

// selectTaskSQL 按主键读取一条任务的当前状态（Get）。
//
// 只查需要的列，不 SELECT *：让"哪些字段会返回给调用方"在代码里一目了然，
// 也避免以后加列时无意中把内部字段暴露出去。
// lease_owner / lease_until / lease_epoch 属于 Worker 内部状态，不进 TaskInfo。
const selectTaskSQL = `SELECT
    queue_name, task_type, payload, status, run_at,
    attempt, max_attempts, timeout_ns, idempotency_key, last_error,
    created_at, updated_at, finished_at
FROM ` + tasksTable + ` WHERE id = ?`

// cancelTaskSQL 只取消仍是 pending 的任务。
//
// 刻意不校验 run_at：已到期但尚未被领取的任务仍是 pending，按设计仍可取消
// （技术设计 §4 状态迁移表）。也不触碰租约字段——pending 任务的租约字段本就是 NULL。
// 用 WHERE status = 'pending' 而不是"先查后改"：条件更新是原子的，
// 不会出现"查到是 pending、更新前被 Worker 领走"的漏判。
const cancelTaskSQL = `UPDATE ` + tasksTable + `
    SET status = 'canceled', finished_at = UTC_TIMESTAMP(6), updated_at = UTC_TIMESTAMP(6)
    WHERE id = ? AND status = 'pending'`

// currentStatusSQL 在取消影响 0 行时用来区分"任务不存在"和"状态不是 pending"。
const currentStatusSQL = `SELECT status FROM ` + tasksTable + ` WHERE id = ?`

// Get 按 ID 返回任务的当前状态，任务不存在时返回 ErrNotFound。
//
// 返回的时间字段一律换算为 UTC，避免调用方因为自己 DSN 里的 loc 设置
// 拿到本地时区的值。
func (c *Client) Get(ctx context.Context, id gorangetask.TaskID) (gorangetask.TaskInfo, error) {
	var (
		info                        gorangetask.TaskInfo
		queue, taskType, status     string
		payload                     []byte
		runAt, createdAt, updatedAt time.Time
		timeoutNs                   sql.NullInt64
		idempotencyKey, lastError   sql.NullString
		finishedAt                  sql.NullTime
	)

	err := c.db.QueryRowContext(ctx, selectTaskSQL, int64(id)).Scan(
		&queue, &taskType, &payload, &status, &runAt,
		&info.Attempt, &info.MaxAttempts, &timeoutNs, &idempotencyKey, &lastError,
		&createdAt, &updatedAt, &finishedAt,
	)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return gorangetask.TaskInfo{}, fmt.Errorf("%w: task %d", gorangetask.ErrNotFound, id)
	case err != nil:
		return gorangetask.TaskInfo{}, fmt.Errorf("gorangetask/store/mysql: get task: %w", err)
	}

	info.ID = id
	info.Queue = queue
	info.Type = taskType
	info.Payload = json.RawMessage(payload)
	info.Status = gorangetask.Status(status)
	info.RunAt = runAt.UTC()
	info.CreatedAt = createdAt.UTC()
	info.UpdatedAt = updatedAt.UTC()
	if idempotencyKey.Valid {
		info.IdempotencyKey = idempotencyKey.String
	}
	if timeoutNs.Valid {
		info.Timeout = time.Duration(timeoutNs.Int64)
	}
	if lastError.Valid {
		info.LastError = lastError.String
	}
	if finishedAt.Valid {
		info.FinishedAt = finishedAt.Time.UTC()
	}
	return info, nil
}

// Cancel 取消一个尚未被领取的 pending 任务。
//
// 任务不存在返回 ErrNotFound；任务已领取（running）或已处于终态
// （succeeded / dead / canceled）返回 ErrNotPending，错误信息里带上实际状态，
// 便于调用方在日志里直接看出原因。重复取消同一个任务因此也返回 ErrNotPending：
// canceled 是终态，不再满足"只允许取消 pending"的条件。
func (c *Client) Cancel(ctx context.Context, id gorangetask.TaskID) error {
	res, err := c.db.ExecContext(ctx, cancelTaskSQL, int64(id))
	if err != nil {
		return fmt.Errorf("gorangetask/store/mysql: cancel task: %w", err)
	}

	affected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("gorangetask/store/mysql: cancel task: %w", err)
	}
	if affected > 0 {
		return nil
	}

	// 影响 0 行只说明"没取消成功"，还得再读一次状态才能给出可判断的错误。
	// 这里不包事务：读到的状态只是刚才那一瞬的快照，够调用方判断原因了，
	// 不承诺更强的一致性；反正任务只要不是 pending，取消本来就该失败。
	var status string
	switch err := c.db.QueryRowContext(ctx, currentStatusSQL, int64(id)).Scan(&status); {
	case errors.Is(err, sql.ErrNoRows):
		return fmt.Errorf("%w: task %d", gorangetask.ErrNotFound, id)
	case err != nil:
		return fmt.Errorf("gorangetask/store/mysql: read task status: %w", err)
	}
	return fmt.Errorf("%w: task %d is %s", gorangetask.ErrNotPending, id, status)
}
