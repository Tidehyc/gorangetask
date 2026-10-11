package gorangetask

import (
	"context"
	"database/sql"
)

// Tx 表示一个能够在调用方现有事务内执行语句的执行器。
//
// 它的唯一职责是让 GoRangeTask 把任务行 INSERT 到调用方正在使用的事务里。
// 因此接口只包含 ExecContext，刻意不暴露 Commit/Rollback，避免库误操作
// 调用方的事务生命周期。
//
// *sql.Tx 直接满足该接口。官方可选 GORM adapter 提供面向 *gorm.DB 的等价入口。
type Tx interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// Client 是任务入队与状态查询的入口。
type Client interface {
	// Enqueue 非事务入队：由实现自行在短事务中插入任务行。
	// 失败时不返回任务 ID。
	Enqueue(ctx context.Context, task Task) (TaskID, error)

	// EnqueueTx 在调用方传入的事务执行器上插入任务行。
	// 实现不开启、不提交、不回滚该事务；任务与业务数据共用同一事务。
	// 当幂等键冲突时返回 ErrDuplicateTask。
	EnqueueTx(ctx context.Context, tx Tx, task Task) (TaskID, error)

	// Get 按 ID 查询任务当前状态，任务不存在时返回 ErrNotFound。
	Get(ctx context.Context, id TaskID) (TaskInfo, error)

	// Cancel 取消一个尚未被领取的 pending 任务。
	// 任务已被领取、已处于终态或不存在时分别返回可判断的错误。
	Cancel(ctx context.Context, id TaskID) error
}
