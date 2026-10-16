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

const (
	// 幂等的核心：条件更新。已经导出的行不再被改动（影响 0 行），
	// 因此重复投递不会产生第二次副作用。
	exportOrderSQL = `UPDATE ` + ordersTable + `
    SET status = 'exported', exported_at = UTC_TIMESTAMP(6)
    WHERE id = ? AND status = 'pending'`

	selectOrderStatusSQL = `SELECT status FROM ` + ordersTable + ` WHERE id = ?`
)

// ExportOrderHandler 返回示例的 Handler：把订单推进到 exported 终态。
//
// delay 非零时，Handler 在写回之前先等这么久（可被 ctx 取消），用来演示"耗时任务"
// 以及"停机时正在处理中的任务"；正常演示传 0。
//
// # 它为什么必须是幂等的
//
// 库的执行语义是「至少一次」：进程崩溃、租约过期、停机超时都会让同一条任务被重新
// 领取，同一个 Handler 会对同一笔订单跑第二次。因此幂等是使用方的义务，而不是库的
// 可选增强。这里的幂等来自一条**条件更新**（`WHERE status = 'pending'`）：
//
//   - 第一次执行影响 1 行，订单进入 exported；
//   - 第二次执行影响 0 行，说明"已经导出过"，直接返回成功——而不是把它当错误。
//     把 0 行当成错误会让任务无谓地重试到次数耗尽、最后进死信，那才是真的故障。
//
// 幂等的**粒度**更要选对：判据是业务状态（status），不是任务本身。按任务 ID 去重
// 是不够的——重试会产生一条新任务、任务 ID 变了，但业务效果必须仍然只发生一次。
//
// 反过来说，这条 Handler 只对"重复执行"负责；它不负责让外部副作用（发邮件、调
// 第三方接口）恰好一次——那需要业务侧自己的去重键，库不提供这种保证。
func ExportOrderHandler(db *sql.DB, delay time.Duration) gorangetask.Handler {
	return func(ctx context.Context, raw json.RawMessage) error {
		var p exportPayload
		if err := json.Unmarshal(raw, &p); err != nil {
			// 带永久标记：payload 是入队时写死的，重试不会让它变得可解析，
			// 因此库会直接判死，而不是白试到次数耗尽。
			return gorangetask.Permanent(fmt.Errorf("example/basic: decode export payload %s: %w", raw, err))
		}
		if p.OrderID <= 0 {
			return gorangetask.Permanent(fmt.Errorf("example/basic: export payload has no order id: %s", raw))
		}

		if delay > 0 {
			// 用 timer + ctx.Done 而不是 time.Sleep：停机或逐任务超时时必须能立刻
			// 收手，否则 Worker 只能等满 ShutdownTimeout 才放弃这条任务。
			timer := time.NewTimer(delay)
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		res, err := db.ExecContext(ctx, exportOrderSQL, p.OrderID)
		if err != nil {
			return fmt.Errorf("example/basic: export order %d: %w", p.OrderID, err)
		}
		affected, err := res.RowsAffected()
		if err != nil {
			return fmt.Errorf("example/basic: read affected rows for order %d: %w", p.OrderID, err)
		}
		if affected > 0 {
			return nil
		}

		// 0 行受影响有两种含义，必须回读一次才能区分。这与库自己在 Cancel 影响
		// 0 行时的做法是同一个道理：不区分就会把"订单根本不存在"静默当成"已经导出"。
		var status string
		switch err := db.QueryRowContext(ctx, selectOrderStatusSQL, p.OrderID).Scan(&status); {
		case errors.Is(err, sql.ErrNoRows):
			// 订单不在了，重试也不会让它出现：永久失败。
			return gorangetask.Permanent(fmt.Errorf("example/basic: order %d does not exist", p.OrderID))
		case err != nil:
			return fmt.Errorf("example/basic: read status of order %d: %w", p.OrderID, err)
		case status == OrderExported:
			return nil // 幂等命中：上一次执行已经把订单导出完了。
		default:
			// 订单既不是 pending 也不是 exported，说明有别的东西动过它。
			// 按可重试失败处理，让退避后再看一眼，而不是武断判死。
			return fmt.Errorf("example/basic: order %d is %q, want pending or exported", p.OrderID, status)
		}
	}
}
