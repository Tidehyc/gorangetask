package basic

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	gorangetask "github.com/Tidehyc/gorangetask"
)

// 示例用到的固定值。队列名与库的 DefaultQueue 刻意分开，免得示例任务和别的用途
// 混在一个队列里。
const (
	// Queue 是示例使用的队列名。
	Queue = "example.orders"
	// TaskTypeExportOrder 是示例的任务类型：把一笔订单导出。
	TaskTypeExportOrder = "order.export"
	// ordersTable 是示例业务表名，与 schema.sql 保持一致。
	ordersTable = "example_orders"
)

// 订单状态。示例只用两个状态，好让"任务把记录推进到终态"一眼可见。
const (
	// OrderPending 表示订单已写入、导出任务已投递但尚未完成。
	OrderPending = "pending"
	// OrderExported 表示导出任务已把订单推进到终态。
	OrderExported = "exported"
)

// ErrOrderNotFound 表示按 ID 查不到订单。
var ErrOrderNotFound = errors.New("example/basic: order not found")

// Order 是一行示例业务记录。
type Order struct {
	ID          int64
	Customer    string
	AmountCents int64
	Status      string
	// ExportedAt 在 Status 变为 exported 时被写入；此前为 NULL。
	ExportedAt sql.NullTime
	CreatedAt  time.Time
}

// exportPayload 是导出任务的 payload。示例用一个极小的结构，只为演示
// "任务携带业务主键"这一条常见做法——Handler 拿到的只是 ID，业务数据现读现用。
type exportPayload struct {
	OrderID int64 `json:"order_id"`
}

const (
	insertOrderSQL = `INSERT INTO ` + ordersTable + ` (customer, amount_cents, status, created_at)
    VALUES (?, ?, 'pending', UTC_TIMESTAMP(6))`

	selectOrderSQL = `SELECT id, customer, amount_cents, status, exported_at, created_at
    FROM ` + ordersTable + ` WHERE id = ?`

	listOrdersSQL = `SELECT id, customer, amount_cents, status, exported_at, created_at
    FROM ` + ordersTable + ` ORDER BY id LIMIT ?`
)

// Store 是示例的业务侧仓储。它同时持有数据库句柄与 GoRangeTask 的 Client——
// 这正是示例要展示的关系：业务写入与任务入队走的是**同一个** *sql.Tx。
//
// 依赖的是 gorangetask.Client 接口而不是具体方言，因此示例代码对 store/mysql
// 没有硬依赖；换成别的方言实现时这里一行都不用改。
type Store struct {
	db     *sql.DB
	client gorangetask.Client
}

// NewStore 装配示例仓储。db 与 client 不能为 nil。
func NewStore(db *sql.DB, client gorangetask.Client) *Store {
	return &Store{db: db, client: client}
}

// CreateOrder 在一个事务里写入订单行并投递导出任务，返回订单 ID 与任务 ID。
//
// 这是整个示例的核心，三步的顺序是关键：
//
//  1. 在**调用方自己开的**事务里 INSERT 订单行；
//  2. 用**同一个** tx 调 EnqueueTx 插入任务行——库不开启、不提交、不回滚这个事务，
//     它只是借用这条连接写一行；
//  3. 由本函数提交一次。
//
// 因此两者的可见性完全绑定：提交则都可见，回滚则都不存在，中途失败也不会留下
// 半条记录。注意返回值里的 taskID 在提交前不具备"任务已存在"的含义。
func (s *Store) CreateOrder(ctx context.Context, customer string, amountCents int64) (int64, gorangetask.TaskID, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, 0, fmt.Errorf("example/basic: begin order transaction: %w", err)
	}
	// 提交成功后 Rollback 返回 sql.ErrTxDone，是可忽略的常规行为。
	defer func() { _ = tx.Rollback() }()

	res, err := tx.ExecContext(ctx, insertOrderSQL, customer, amountCents)
	if err != nil {
		return 0, 0, fmt.Errorf("example/basic: insert order: %w", err)
	}
	orderID, err := res.LastInsertId()
	if err != nil {
		return 0, 0, fmt.Errorf("example/basic: read new order id: %w", err)
	}

	payload, err := json.Marshal(exportPayload{OrderID: orderID})
	if err != nil {
		return 0, 0, fmt.Errorf("example/basic: encode export payload: %w", err)
	}

	// MaxAttempts / Timeout 留零值，表示用 ClientConfig 与 Config 的默认值。
	// 示例不复述库的默认值，免得默认值一改这里就变成过期文档。
	taskID, err := s.client.EnqueueTx(ctx, tx, gorangetask.Task{
		Queue:   Queue,
		Type:    TaskTypeExportOrder,
		Payload: payload,
	})
	if err != nil {
		return 0, 0, fmt.Errorf("example/basic: enqueue export task for order %d: %w", orderID, err)
	}

	if err := tx.Commit(); err != nil {
		return 0, 0, fmt.Errorf("example/basic: commit order transaction: %w", err)
	}
	return orderID, taskID, nil
}

// GetOrder 按 ID 读回一行订单，不存在时返回 ErrOrderNotFound。
func (s *Store) GetOrder(ctx context.Context, id int64) (Order, error) {
	var o Order
	err := s.db.QueryRowContext(ctx, selectOrderSQL, id).
		Scan(&o.ID, &o.Customer, &o.AmountCents, &o.Status, &o.ExportedAt, &o.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return Order{}, fmt.Errorf("%w: %d", ErrOrderNotFound, id)
	}
	if err != nil {
		return Order{}, fmt.Errorf("example/basic: read order %d: %w", id, err)
	}
	return o, nil
}

// ListOrders 按 ID 顺序返回最多 limit 行订单。demo 用它打印进度。
func (s *Store) ListOrders(ctx context.Context, limit int) ([]Order, error) {
	rows, err := s.db.QueryContext(ctx, listOrdersSQL, limit)
	if err != nil {
		return nil, fmt.Errorf("example/basic: list orders: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var orders []Order
	for rows.Next() {
		var o Order
		if err := rows.Scan(&o.ID, &o.Customer, &o.AmountCents, &o.Status, &o.ExportedAt, &o.CreatedAt); err != nil {
			return nil, fmt.Errorf("example/basic: scan order: %w", err)
		}
		orders = append(orders, o)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("example/basic: read orders: %w", err)
	}
	return orders, nil
}
