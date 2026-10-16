// Package gormadapter 是 GoRangeTask 的官方可选 GORM adapter。
//
// 包名刻意与目录名（adapter/gorm）不同：使用方几乎总要同时 import
// gorm.io/gorm，若本包也叫 gorm，每个调用点都得起别名。
//
// # 它解决什么
//
// *gorm.DB 无法直接满足 gorangetask.Tx：
//
//   - 它没有 ExecContext 方法。它的执行入口是 Exec(sql, values...) *DB，
//     返回的是链式句柄，错误与结果都藏在会话状态里。
//   - 它的 Exec **拿不到新插入行的 ID**。GORM 只在内部的 Create 流程里读
//     LastInsertId（v1.31.2 的 callbacks/create.go），裸 SQL 的 Exec 会把
//     sql.Result 丢掉。
//
// 所以 adapter 不去用 GORM 的执行入口，而是从 *gorm.DB 上取出**它正在使用的
// 连接**（Statement.ConnPool），交给 store/mysql 执行与普通入队完全相同的那条
// INSERT——校验、幂等键冲突识别、时间口径全部复用，不重写一份。
//
// # 核心包不依赖 GORM
//
// gorangetask 根包与 store/mysql 都不 import 本包，也不 import gorm；
// 只有真正使用 GORM 的项目才编译本包。
//
// # 必须在事务里调用
//
// EnqueueTx 要求传入一个**已开启的事务句柄**：db.Transaction(func(tx *gorm.DB)
// error {...}) 的回调参数，或 db.Begin() 的返回值。
//
// 传一个非事务的 *gorm.DB 会让任务行走自己的连接（autocommit），与业务写入不在
// 同一个事务里——那恰恰是本库存在的理由被悄悄毁掉的方式，而且从代码上看不出来
// （编译通过、运行也不报错，只是回滚时任务行留了下来）。因此 adapter 会直接
// 拒绝这种调用：非事务的 *gorm.DB 直接持有 *sql.DB，这一点可以判定。
package gormadapter

import (
	"context"
	"database/sql"
	"fmt"

	gormio "gorm.io/gorm"

	gorangetask "github.com/Tidehyc/gorangetask"
	mysqlstore "github.com/Tidehyc/gorangetask/store/mysql"
)

// 编译期断言：GORM 的连接池抽象满足 gorangetask.Tx。
//
// 这是 adapter 得以成立的基础——ConnPool.ExecContext 与 Tx.ExecContext 的签名
// 必须一致（ctx、查询、变参、返回 (sql.Result, error)）。GORM 哪天改了它，
// 这里会**编译失败**，而不是在运行期把任务写进错误的连接。
var _ gorangetask.Tx = gormio.ConnPool(nil)

// Adapter 把 GoRangeTask 的入队能力接到 GORM 事务上。
//
// 它不自己持有连接，只持有一个已完成配置的 mysql Client，因此两者的默认队列、
// payload 上限等口径天然一致。
type Adapter struct {
	client *mysqlstore.Client
}

// New 构造一个 Adapter。client 不能为 nil。
//
// 传 nil 而不是在这里自己建一个，是有意的：入队侧的配置（默认队列、payload
// 上限）应当只有一处来源，否则"库里写的队列"和"adapter 写的队列"可能悄悄分叉。
func New(client *mysqlstore.Client) (*Adapter, error) {
	if client == nil {
		return nil, fmt.Errorf("%w: nil mysql client", gorangetask.ErrInvalidConfig)
	}
	return &Adapter{client: client}, nil
}

// EnqueueTx 在调用方已开启的 GORM 事务里插入一条任务行，返回其 ID。
//
// 它不开启、不提交、不回滚事务，也不换连接：任务行与调用方的业务写入同生共死
// ——事务提交则任务可见，回滚则任务不存在。因此返回的 ID 在事务提交前不具备
// "任务已存在"的含义。
//
// 幂等键冲突（同事务内重复，或与库中已提交的行重复）返回 ErrDuplicateTask。
//
// 典型用法：
//
//	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
//		if err := tx.Create(&order).Error; err != nil {
//			return err
//		}
//		id, err := adapter.EnqueueTx(ctx, tx, gorangetask.Task{
//			Type:    "order.export",
//			Payload: payload,
//		})
//		if err != nil {
//			return err
//		}
//		log.Printf("queued export task %d", id)
//		return nil
//	})
func (a *Adapter) EnqueueTx(ctx context.Context, tx *gormio.DB, task gorangetask.Task) (gorangetask.TaskID, error) {
	pool, err := txConnPool(tx)
	if err != nil {
		return 0, err
	}
	return a.client.EnqueueTx(ctx, pool, task)
}

// txConnPool 从 *gorm.DB 上取出正在使用的连接，并确认它确实处在一个事务里。
//
// 三种拒绝各有各的来由，见各自的注释：它们都会让任务行落到调用方以为的地方之外。
func txConnPool(tx *gormio.DB) (gorangetask.Tx, error) {
	if tx == nil {
		return nil, fmt.Errorf("%w: nil *gorm.DB", gorangetask.ErrInvalidTask)
	}
	// 会话可能已经带着错误（Begin 失败、事务已被判定回滚、之前的语句出错）。
	// 这时再插一行只会写进一个注定不会提交的上下文，不如把原始错误还给调用方，
	// 让它看见真正的原因。
	if tx.Error != nil {
		return nil, fmt.Errorf("%w: gorm session already failed: %w", gorangetask.ErrInvalidTask, tx.Error)
	}
	if tx.Statement == nil || tx.Statement.ConnPool == nil {
		return nil, fmt.Errorf("%w: gorm session carries no connection", gorangetask.ErrInvalidTask)
	}
	// 非事务的 *gorm.DB 直接持有 *sql.DB：它上面的每条语句各自 autocommit，
	// 与业务写入不在同一个事务里。这是本 adapter 最需要挡住的一种误用，因为它
	// 编译通过、运行也不报错，只在回滚时才暴露（任务行留了下来）。
	//
	// 判据取"是不是 *sql.DB"而不是"是不是 *sql.Tx"：开启 PrepareStmt 之后，
	// 事务里的连接池是 gorm 自己的包装类型（*PreparedStmtTX），并不是 *sql.Tx，
	// 那种用法完全正当，不能被误伤。
	if _, isPlainDB := tx.Statement.ConnPool.(*sql.DB); isPlainDB {
		return nil, fmt.Errorf(
			"%w: EnqueueTx needs a transaction handle (the tx given to db.Transaction, or the result of db.Begin), not a plain *gorm.DB",
			gorangetask.ErrInvalidTask)
	}
	return tx.Statement.ConnPool, nil
}
