package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
	"unicode/utf8"

	gorangetask "github.com/Tidehyc/gorangetask"
)

const (
	// maxLeaseOwnerLen 是 lease_owner 列的字符上限（VARCHAR(128)）。
	maxLeaseOwnerLen = 128
	// maxLastErrorLen 是 last_error 列的字符上限（VARCHAR(2048)）。
	maxLastErrorLen = 2048
)

// 编译期断言：本实现同时满足 Worker 侧的存储接口。
var _ gorangetask.WorkerStore = (*Client)(nil)

// claimTaskIDsSQL 选出可领取任务的 id。
//
// 刻意**只取 id**：这条加锁读要停在覆盖索引 ix_gotask_claim
// (queue_name, status, run_at, id) 上；一旦把 payload 这类大列也取出来，
// 覆盖索引就失效，加锁范围与代价都会随行宽放大。完整行在事务提交后按主键回读。
//
// FOR UPDATE SKIP LOCKED 会让并发领取的 Worker 跳过彼此已锁定的行，
// 因此它只用于队列领取，不能当作一般一致性读（技术设计 §5.2、§10）。
// 锁定读可能跳过被锁记录，所以本查询不提供严格 FIFO 或多队列公平性。
const claimTaskIDsSQL = `SELECT id FROM ` + tasksTable + `
    WHERE queue_name = ?
      AND status = 'pending'
      AND run_at <= UTC_TIMESTAMP(6)
    ORDER BY run_at, id
    LIMIT ?
    FOR UPDATE SKIP LOCKED`

// markRunningSQL 把一条已锁定的 pending 任务置为 running，并一次性完成
// 三件事：设置本次租约（owner + deadline）、递增 epoch、递增 attempt。
//
// lease_epoch 每次领取都 +1 且永不由任务重置，它和 owner 一起构成 fencing 凭据：
// 之后任何写回都必须同时匹配这两个值，旧的持有者自然被挡住。
// WHERE status = 'pending' 让"仍是 pending"与"改成 running"在一条语句里完成。
const markRunningSQL = `UPDATE ` + tasksTable + `
    SET status = 'running',
        lease_owner = ?,
        lease_until = TIMESTAMPADD(MICROSECOND, ?, UTC_TIMESTAMP(6)),
        lease_epoch = lease_epoch + 1,
        attempt = attempt + 1,
        updated_at = UTC_TIMESTAMP(6)
    WHERE id = ? AND status = 'pending'`

// selectClaimedTaskSQL 在领取事务提交后按主键回读整行，供 Worker 执行。
const selectClaimedTaskSQL = `SELECT
    queue_name, task_type, payload, attempt, max_attempts, lease_owner, lease_epoch
FROM ` + tasksTable + ` WHERE id = ?`

// 下面三条是状态写回语句，共用同一组 fencing 条件：
//
//	id 匹配、状态仍是 running、owner 与 epoch 都匹配、且租约尚未过期。
//
// 四个条件缺一不可——只比 id 会让失去租约的旧 Worker 覆盖新持有者的结果；
// 只比 owner 会在同一 Worker 重复领取同一任务时误判。影响 0 行即 ErrLeaseLost。
const (
	succeedTaskSQL = `UPDATE ` + tasksTable + `
    SET status = 'succeeded', finished_at = UTC_TIMESTAMP(6),
        lease_owner = NULL, lease_until = NULL,
        last_error = NULL, updated_at = UTC_TIMESTAMP(6)
    WHERE id = ? AND status = 'running'
      AND lease_owner = ? AND lease_epoch = ?
      AND lease_until > UTC_TIMESTAMP(6)`

	retryTaskSQL = `UPDATE ` + tasksTable + `
    SET status = 'pending',
        run_at = TIMESTAMPADD(MICROSECOND, ?, UTC_TIMESTAMP(6)),
        lease_owner = NULL, lease_until = NULL,
        last_error = ?, updated_at = UTC_TIMESTAMP(6)
    WHERE id = ? AND status = 'running'
      AND lease_owner = ? AND lease_epoch = ?
      AND lease_until > UTC_TIMESTAMP(6)`

	markDeadTaskSQL = `UPDATE ` + tasksTable + `
    SET status = 'dead', finished_at = UTC_TIMESTAMP(6),
        lease_owner = NULL, lease_until = NULL,
        last_error = ?, updated_at = UTC_TIMESTAMP(6)
    WHERE id = ? AND status = 'running'
      AND lease_owner = ? AND lease_epoch = ?
      AND lease_until > UTC_TIMESTAMP(6)`
)

// Claim 原子领取一批到期任务，实现 gorangetask.WorkerStore。
//
// 顺序是刻意的，分两段：
//
//  1. 一个短事务里用锁定读选出 id，逐条置为 running（设置租约、epoch+1、
//     attempt+1），然后提交。事务期间只持有行的悲观锁，不执行任何业务逻辑。
//  2. 事务提交后按 id 回读整行返回给调用方。
//
// 这样 Handler 永远在事务之外运行（技术设计 §5.2 第 5 步）。若在提交与回读
// 之间进程崩溃，任务会停在 running 且租约未过期，由回收器（§4）在租约到期后接手。
//
// 没有可领取任务时返回 (nil, nil)。
func (c *Client) Claim(ctx context.Context, spec gorangetask.ClaimSpec) ([]gorangetask.ClaimedTask, error) {
	if err := validateClaimSpec(spec); err != nil {
		return nil, err
	}

	ids, err := c.claimIDs(ctx, spec)
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, nil
	}

	claimed := make([]gorangetask.ClaimedTask, 0, len(ids))
	for _, id := range ids {
		task, err := c.readClaimedTask(ctx, id)
		if err != nil {
			return nil, err
		}
		claimed = append(claimed, task)
	}
	return claimed, nil
}

// claimIDs 在单个事务里选出并锁定一批待领取任务，把它们置为 running 后提交。
func (c *Client) claimIDs(ctx context.Context, spec gorangetask.ClaimSpec) ([]int64, error) {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("gorangetask/store/mysql: begin claim transaction: %w", err)
	}
	// 提交成功后 Rollback 返回 sql.ErrTxDone，是可忽略的常规行为；
	// 任何提前返回的分支都靠它兜底回滚。
	defer func() { _ = tx.Rollback() }()

	ids, err := selectClaimableIDs(ctx, tx, spec)
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		// 空事务直接提交，等价于结束这个事务。
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("gorangetask/store/mysql: commit empty claim transaction: %w", err)
		}
		return nil, nil
	}

	leaseMicros := spec.LeaseDuration.Microseconds()
	for _, id := range ids {
		res, err := tx.ExecContext(ctx, markRunningSQL, spec.Owner, leaseMicros, id)
		if err != nil {
			return nil, fmt.Errorf("gorangetask/store/mysql: mark task %d running: %w", id, err)
		}
		affected, err := res.RowsAffected()
		if err != nil {
			return nil, fmt.Errorf("gorangetask/store/mysql: mark task %d running: %w", id, err)
		}
		if affected != 1 {
			// 理论上不可能：这些 id 刚在同一个事务里被 SELECT ... FOR UPDATE
			// 锁住，其间别人改不动它们。真出现说明有 bug 或写路径漏了锁，
			// 此时必须回滚整个 claim 事务，不能只算这一条没领到。
			// 这里不套公开哨兵：它是内部不变式被破坏，调用方只需看到并退避重试。
			return nil, fmt.Errorf(
				"gorangetask/store/mysql: task %d changed while claiming (rows affected = %d)", id, affected)
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("gorangetask/store/mysql: commit claim transaction: %w", err)
	}
	return ids, nil
}

// selectClaimableIDs 执行加锁读并收集 id。rows 必须在事务内关闭后才能执行后续语句。
func selectClaimableIDs(ctx context.Context, tx *sql.Tx, spec gorangetask.ClaimSpec) ([]int64, error) {
	rows, err := tx.QueryContext(ctx, claimTaskIDsSQL, spec.Queue, spec.Limit)
	if err != nil {
		return nil, fmt.Errorf("gorangetask/store/mysql: select claimable tasks: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("gorangetask/store/mysql: scan claimable task id: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("gorangetask/store/mysql: read claimable tasks: %w", err)
	}
	return ids, nil
}

// readClaimedTask 在领取事务提交后回读整行，组装成 ClaimedTask。
func (c *Client) readClaimedTask(ctx context.Context, id int64) (gorangetask.ClaimedTask, error) {
	var (
		task            gorangetask.ClaimedTask
		queue, taskType string
		payload         []byte
		owner           sql.NullString
		leaseEpoch      uint64
	)

	err := c.db.QueryRowContext(ctx, selectClaimedTaskSQL, id).Scan(
		&queue, &taskType, &payload, &task.Attempt, &task.MaxAttempts, &owner, &leaseEpoch,
	)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return gorangetask.ClaimedTask{}, fmt.Errorf(
			"%w: claimed task %d disappeared after the claim transaction committed",
			gorangetask.ErrNotFound, id)
	case err != nil:
		return gorangetask.ClaimedTask{}, fmt.Errorf("gorangetask/store/mysql: read claimed task %d: %w", id, err)
	}
	if !owner.Valid {
		// 刚被置为 running 的行必然带 owner；为空说明写路径出了问题。
		return gorangetask.ClaimedTask{}, fmt.Errorf(
			"gorangetask/store/mysql: claimed task %d has no lease owner", id)
	}

	task.ID = gorangetask.TaskID(id)
	task.Queue = queue
	task.Type = taskType
	task.Payload = json.RawMessage(payload)
	task.Owner = owner.String
	task.Epoch = leaseEpoch
	return task, nil
}

// Succeed 把任务写为 succeeded 终态，实现 gorangetask.WorkerStore。
//
// 写回带完整 fencing 条件；影响 0 行说明租约已过期或已易主，返回 ErrLeaseLost，
// 且**不做**无条件更新补救——那会让旧持有者覆盖新持有者的结果。
func (c *Client) Succeed(ctx context.Context, task gorangetask.ClaimedTask) error {
	if err := validateLease(task); err != nil {
		return err
	}
	res, err := c.db.ExecContext(ctx, succeedTaskSQL, int64(task.ID), task.Owner, task.Epoch)
	if err != nil {
		return fmt.Errorf("gorangetask/store/mysql: succeed task: %w", err)
	}
	return checkLeaseAck(res, task)
}

// Retry 把任务写回 pending，实现 gorangetask.WorkerStore：delay 之后可以再次
// 被领取（delay 为 0 表示立即），并把错误摘要写进 last_error。
//
// run_at 用数据库时钟加 delay 计算，不接受调用方传入的时刻，避免应用机器与
// 数据库时钟不一致导致的时序判断偏差（技术设计 §3.2）。
func (c *Client) Retry(ctx context.Context, task gorangetask.ClaimedTask, delay time.Duration, lastError string) error {
	if err := validateLease(task); err != nil {
		return err
	}
	if delay < 0 {
		return fmt.Errorf("%w: retry delay must not be negative, got %s", gorangetask.ErrInvalidTask, delay)
	}

	res, err := c.db.ExecContext(ctx, retryTaskSQL,
		delay.Microseconds(), truncateRunes(lastError, maxLastErrorLen),
		int64(task.ID), task.Owner, task.Epoch)
	if err != nil {
		return fmt.Errorf("gorangetask/store/mysql: retry task: %w", err)
	}
	return checkLeaseAck(res, task)
}

// MarkDead 把任务写入死信终态，实现 gorangetask.WorkerStore。
//
// 死信不再自动重试；错误摘要写入 last_error。写回同样带 fencing 条件。
func (c *Client) MarkDead(ctx context.Context, task gorangetask.ClaimedTask, lastError string) error {
	if err := validateLease(task); err != nil {
		return err
	}
	res, err := c.db.ExecContext(ctx, markDeadTaskSQL,
		truncateRunes(lastError, maxLastErrorLen), int64(task.ID), task.Owner, task.Epoch)
	if err != nil {
		return fmt.Errorf("gorangetask/store/mysql: mark task dead: %w", err)
	}
	return checkLeaseAck(res, task)
}

// checkLeaseAck 把"影响 0 行"翻译成 ErrLeaseLost。
func checkLeaseAck(res sql.Result, task gorangetask.ClaimedTask) error {
	affected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("gorangetask/store/mysql: read rows affected: %w", err)
	}
	if affected == 0 {
		return fmt.Errorf("%w: task %d (owner %q, epoch %d)",
			gorangetask.ErrLeaseLost, task.ID, task.Owner, task.Epoch)
	}
	return nil
}

// validateClaimSpec 校验领取请求。这些值来自 Worker 配置，非法即为配置错误。
func validateClaimSpec(spec gorangetask.ClaimSpec) error {
	if spec.Queue == "" {
		return fmt.Errorf("%w: claim queue is empty", gorangetask.ErrInvalidConfig)
	}
	if n := utf8.RuneCountInString(spec.Queue); n > maxQueueNameLen {
		return fmt.Errorf("%w: claim queue is %d characters, MySQL allows %d",
			gorangetask.ErrInvalidConfig, n, maxQueueNameLen)
	}
	if spec.Limit < 1 {
		return fmt.Errorf("%w: claim limit must be > 0, got %d", gorangetask.ErrInvalidConfig, spec.Limit)
	}
	if spec.Owner == "" {
		return fmt.Errorf("%w: claim owner is empty", gorangetask.ErrInvalidConfig)
	}
	if n := utf8.RuneCountInString(spec.Owner); n > maxLeaseOwnerLen {
		return fmt.Errorf("%w: claim owner is %d characters, MySQL allows %d",
			gorangetask.ErrInvalidConfig, n, maxLeaseOwnerLen)
	}
	if spec.LeaseDuration <= 0 {
		return fmt.Errorf("%w: claim lease duration must be > 0, got %s",
			gorangetask.ErrInvalidConfig, spec.LeaseDuration)
	}
	return nil
}

// validateLease 校验写回用的 fencing 凭据。凭据只能来自 Claim 的返回值，
// 因此为空就是调用方用错了。
func validateLease(task gorangetask.ClaimedTask) error {
	if task.ID == 0 {
		return fmt.Errorf("%w: lease has no task id", gorangetask.ErrInvalidTask)
	}
	if task.Owner == "" {
		return fmt.Errorf("%w: lease of task %d has no owner", gorangetask.ErrInvalidTask, task.ID)
	}
	return nil
}

// truncateRunes 按字符截断到最多 n 个字符，用于写入定长 VARCHAR 列。
// n 由列宽常量提供，恒为正数。
func truncateRunes(s string, n int) string {
	if utf8.RuneCountInString(s) <= n {
		return s
	}
	return string([]rune(s)[:n])
}
