package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
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
//
// timeout_ns 一并取回：逐任务超时要靠它还原，否则重启或重新领取后就只剩
// Worker 的默认值了。
//
// 最后一列是排队等待时长，直接由数据库时钟算出（从 GREATEST(created_at, run_at)
// 到此刻的微秒数）。取"最早可被领取"而不是 created_at：延迟任务在 run_at 之前
// 本来就不该被领取，把它刻意安排的等待算进"排队延迟"会污染这个指标。
const selectClaimedTaskSQL = `SELECT
    queue_name, task_type, payload, attempt, max_attempts, timeout_ns,
    lease_owner, lease_epoch,
    TIMESTAMPDIFF(MICROSECOND, GREATEST(created_at, run_at), UTC_TIMESTAMP(6))
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

// renewLeaseSQL 延长一条 running 任务的租约（技术设计 §5.3）。
//
// 新的 lease_until 从**数据库当前时间**起算，而不是在旧的 lease_until 上累加：
// 续租表达的是"从我续租的这一刻起，再保证这么长时间"，累加会让租约随续租次数
// 越推越远，一次失败的续租就能把任务锁住远超预期。
//
// WHERE 条件与三条状态写回完全一致（含 status='running'）：续租同样是一次
// "凭租约说话"的操作，旧持有者必须在续租上也被挡住，否则一个已经失去租约的
// Worker 可以靠续租把自己"续"回有效状态，fencing 就形同虚设。
const renewLeaseSQL = `UPDATE ` + tasksTable + `
    SET lease_until = TIMESTAMPADD(MICROSECOND, ?, UTC_TIMESTAMP(6)),
        updated_at = UTC_TIMESTAMP(6)
    WHERE id = ? AND status = 'running'
      AND lease_owner = ? AND lease_epoch = ?
      AND lease_until > UTC_TIMESTAMP(6)`

// 回收器写回的错误摘要。用固定文案而不是拼接原错误：回收器手上没有别的信息，
// 它唯一能说明的就是"这个持有者没能确认自己的结果，租约已经过期"。
const (
	reapPendingError = "lease expired"
	reapDeadError    = "lease expired at final attempt"
)

// reapExpiredIDsSQL 选出"仍标记为 running、但租约已经过期"的任务，并连带取回
// 分流所需的 attempt / max_attempts。
//
// 走 ix_gotask_lease (status, lease_until, id)：前缀正是 status，因此这条扫描
// 只落在"已过期"的那一段上。加锁读同样用 SKIP LOCKED —— 集群里每个 Worker 都会
// 跑回收器，彼此跳过已锁定的行即可，不需要额外协调。
//
// FORCE INDEX 是**必需的，不是优化**。这里取出的 attempt / max_attempts 不在
// ix_gotask_lease 里，于是这条读不是覆盖索引；实测（MySQL 8.0.46，12 行过期任务）
// 优化器会选 `type=ALL, key=NULL, Using filesort`，即全表扫描 + 排序：
//
//	EXPLAIN -> type=ALL key=NULL Extra=Using where; Using filesort
//	tx1（LIMIT 3）SELECT -> [1 2 3]
//	但 data_locks 显示 tx1 在 PRIMARY 上锁了**全部 12 行**；
//	再开一个 tx2 执行同一条读，它返回 **0 行**，却同样把 ix_gotask_lease 上
//	12 条索引项全锁了一遍（每个回收器都会把整个积压锁住）。
//
// 也就是说 LIMIT 不再限制加锁范围，SKIP LOCKED 也失去意义：并发回收器互相
// 把对方挡住，一轮下来只有一个能推进。加 FORCE INDEX 后计划变为
// `type=range, key=ix_gotask_lease`，只锁选中的那几行（实测 7 条 vs 13 条），
// 并发回收恢复正常。回归用例见 TestReapLockingReadStaysOnLeaseIndex。
//
// lease_until 刻意用 DATE_FORMAT 取成字符串、而不是扫进 time.Time：这个值要原样
// 带回去做下面 UPDATE 的等值守卫，走字符串能完全避开 go-sql-driver 按 DSN 里的
// loc 重新格式化时间的问题（与 client.go 里 dbTimeLayout 是同一条理由）。
// 格式与 DATETIME(6) 的字面量一致。
const reapExpiredIDsSQL = `SELECT id, attempt, max_attempts,
    DATE_FORMAT(lease_until, '%Y-%m-%d %H:%i:%s.%f')
FROM ` + tasksTable + ` FORCE INDEX (ix_gotask_lease)
    WHERE status = 'running'
      AND lease_until <= UTC_TIMESTAMP(6)
    ORDER BY lease_until, id
    LIMIT ?
    FOR UPDATE SKIP LOCKED`

// 回收的两条写回语句。
//
// 守卫不是 owner/epoch：回收器**不是租约持有者**，它没有凭据可比。它能证明的只有
// "这条过期租约还是我刚才读到的那一条"，所以把读到的 lease_until 原值带进 WHERE。
// 期间有人续租（lease_until 变成未来）或任务已被处理（status 变了），影响行数就是
// 0，此时放手即可。
//
// 两条都**不动 attempt**：一次尝试从成功领取开始，这条任务已经用掉了那次机会，
// 回收只是给下一次让路（技术设计 §4）。
const (
	reapToPendingSQL = `UPDATE ` + tasksTable + `
    SET status = 'pending',
        run_at = UTC_TIMESTAMP(6),
        lease_owner = NULL, lease_until = NULL,
        last_error = ?, updated_at = UTC_TIMESTAMP(6)
    WHERE id = ? AND status = 'running' AND lease_until = ?`

	reapToDeadSQL = `UPDATE ` + tasksTable + `
    SET status = 'dead', finished_at = UTC_TIMESTAMP(6),
        lease_owner = NULL, lease_until = NULL,
        last_error = ?, updated_at = UTC_TIMESTAMP(6)
    WHERE id = ? AND status = 'running' AND lease_until = ?`
)

// cleanupIDsSQL 选出"已进入终态、且超过保留期"的行 id。
//
// 走 ix_gotask_cleanup (status, finished_at, id)，**而且不需要 FORCE INDEX**——
// 与回收器（§5.6）的差别正在这里：这条读只取 id，三列全在索引里，是天然的覆盖
// 索引，优化器没有理由离开它。（回收器那条要连带取 attempt / max_attempts，
// 才必须把计划钉死。）
//
// status 用**等值**而不是 `IN (...)`，也是刻意的：等值前缀让索引的后两列
// (finished_at, id) 继续保证有序，ORDER BY 与 LIMIT 都吃索引顺序、没有 filesort；
// 换成 IN，各段之间无法保序，优化器会退化成范围扫描 + filesort，加锁范围就不再受
// LIMIT 约束（同类事故的完整记录见技术设计附录 A）。这也是 CleanupSpec 做成
// "一次一个终态"的原因。
//
// 时间边界用**数据库时钟**减去保留期；负号放在 Go 侧传进来，SQL 保持
// TIMESTAMPADD 的统一写法。
//
// `finished_at <= ...` 顺带把 finished_at IS NULL 的行排除掉了（NULL 与任何值比较
// 都不为真）。终态行不该出现 NULL，但兜底方向是"宁可少删"。
const cleanupIDsSQL = `SELECT id FROM ` + tasksTable + `
    WHERE status = ?
      AND finished_at <= TIMESTAMPADD(MICROSECOND, ?, UTC_TIMESTAMP(6))
    ORDER BY finished_at, id
    LIMIT ?
    FOR UPDATE SKIP LOCKED`

// deleteTasksByIDPrefix 按主键删除，主键列表由调用方拼成等量占位符。
//
// 删除条件只用主键、不重复一遍筛选条件：选中的 id 就是"刚刚确认过期的行"，按主键删
// 既不会误删别的行，也不会因为期间有人改了 finished_at 而漏删。
const deleteTasksByIDPrefix = `DELETE FROM ` + tasksTable + ` WHERE id IN (`

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
		timeoutNs       sql.NullInt64
		owner           sql.NullString
		leaseEpoch      uint64
		waitMicros      int64
	)

	err := c.db.QueryRowContext(ctx, selectClaimedTaskSQL, id).Scan(
		&queue, &taskType, &payload, &task.Attempt, &task.MaxAttempts, &timeoutNs,
		&owner, &leaseEpoch, &waitMicros,
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
	if timeoutNs.Valid {
		// 入库时按纳秒存，读取后还原成 time.Duration；NULL 表示未设置，保持 0。
		task.Timeout = time.Duration(timeoutNs.Int64)
	}
	task.Owner = owner.String
	task.Epoch = leaseEpoch
	// 直接用 int64 扫描而不是 sql.NullInt64：created_at 与 run_at 都是 NOT NULL，
	// GREATEST 不可能返回 NULL。真出现 NULL 时让扫描当场报错，好过静默传一个 0
	// 出去——那会把"数据坏了"伪装成"这条任务没排队"。
	task.Wait = time.Duration(waitMicros) * time.Microsecond
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

// RenewLease 为一条在途任务续租，实现 gorangetask.WorkerStore。
//
// 新的 lease_until 由数据库时钟加上 leaseDuration 得到，不接受调用方传入的时刻
// （技术设计 §3.2）；影响 0 行即 ErrLeaseLost。
//
// 注意区分两种失败：ErrLeaseLost 表示**本次执行已经不再持有租约**（租约过期、
// 已易主、或任务已不在 running），调用方必须立刻停止该次执行且不得写回；
// 而驱动错误（连接断开、超时）只说明"这次续租没问成"，租约可能仍然有效，
// 必须原样冒泡而不是被翻译成 ErrLeaseLost —— 否则一次数据库抖动就会被误判成
// "任务已被别人接手"，把还在正常执行的 Handler 无谓地半路掐掉。
func (c *Client) RenewLease(ctx context.Context, task gorangetask.ClaimedTask, leaseDuration time.Duration) error {
	if err := validateLease(task); err != nil {
		return err
	}
	if leaseDuration <= 0 {
		return fmt.Errorf("%w: lease duration must be > 0, got %s",
			gorangetask.ErrInvalidConfig, leaseDuration)
	}

	res, err := c.db.ExecContext(ctx, renewLeaseSQL,
		leaseDuration.Microseconds(), int64(task.ID), task.Owner, task.Epoch)
	if err != nil {
		return fmt.Errorf("gorangetask/store/mysql: renew lease of task %d: %w", task.ID, err)
	}
	return checkLeaseAck(res, task)
}

// ReapExpiredLeases 回收一批租约已过期的任务，实现 gorangetask.WorkerStore。
//
// 它是"持有者已经消失"这条路径的唯一出口：进程崩溃、停机超时、Handler 不理会
// 取消，都会留下一条永远不会自己结束的 running 任务，而租约是其中唯一会自然
// 过期的凭据。
//
// 分流按 attempt 与 max_attempts 比较（技术设计 §4、§5.6）：
//   - 次数未耗尽 → pending，run_at 设为数据库当前时间、清空租约；
//   - 次数已耗尽 → dead，写 finished_at、清空租约。
//
// 整个动作在一个短事务里完成，**不执行任何 Handler**。并发安全来自两处：加锁读的
// SKIP LOCKED 让多个回收器不抢同一行；每条更新再带 status='running' 与读到的
// lease_until 原值做守卫。守卫不成立（影响 0 行）说明这行已被续租或已被处理，
// 跳过它继续即可，**不返回 ErrLeaseLost** —— 那是租约持有者的错误语义，借用它
// 会让"集群里有别的回收器在干活"看起来像故障。
//
// 返回的两个计数只统计**真正写回成功**的行数，被守卫挡掉的行不计入，因此 limit
// 是"最多尝试处理"的上界，不是"至少处理"的下界。
func (c *Client) ReapExpiredLeases(ctx context.Context, limit int) (gorangetask.ReapResult, error) {
	var result gorangetask.ReapResult
	if limit < 1 {
		return result, fmt.Errorf("%w: reap limit must be > 0, got %d", gorangetask.ErrInvalidConfig, limit)
	}

	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return result, fmt.Errorf("gorangetask/store/mysql: begin reap transaction: %w", err)
	}
	// 提交成功后 Rollback 返回 sql.ErrTxDone，是可忽略的常规行为。
	defer func() { _ = tx.Rollback() }()

	expiries, err := selectExpiredLeases(ctx, tx, limit)
	if err != nil {
		return result, err
	}

	for _, e := range expiries {
		exhausted := e.attempt >= e.maxAttempts
		stmt, marker := reapToPendingSQL, reapPendingError
		if exhausted {
			stmt, marker = reapToDeadSQL, reapDeadError
		}

		res, err := tx.ExecContext(ctx, stmt, marker, e.id, e.leaseUntil)
		if err != nil {
			return result, fmt.Errorf("gorangetask/store/mysql: reap task %d: %w", e.id, err)
		}
		affected, err := res.RowsAffected()
		if err != nil {
			return result, fmt.Errorf("gorangetask/store/mysql: reap task %d: %w", e.id, err)
		}
		if affected == 0 {
			continue
		}
		if exhausted {
			result.ToDead++
		} else {
			result.ToPending++
		}
	}

	if err := tx.Commit(); err != nil {
		return result, fmt.Errorf("gorangetask/store/mysql: commit reap transaction: %w", err)
	}
	return result, nil
}

// expiredLease 是一条待回收的行，连同分流判断与写回守卫所需的全部信息。
type expiredLease struct {
	id          int64
	attempt     int
	maxAttempts int
	// leaseUntil 是 DATE_FORMAT 出来的 DATETIME(6) 字面量，原样带回去做等值守卫。
	leaseUntil string
}

// selectExpiredLeases 执行加锁读并收集结果。返回前必须把 rows 关掉：同一条事务
// 连接上未读完的结果集会挡住后面的 UPDATE。
func selectExpiredLeases(ctx context.Context, tx *sql.Tx, limit int) ([]expiredLease, error) {
	rows, err := tx.QueryContext(ctx, reapExpiredIDsSQL, limit)
	if err != nil {
		return nil, fmt.Errorf("gorangetask/store/mysql: select expired leases: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var expiries []expiredLease
	for rows.Next() {
		var e expiredLease
		if err := rows.Scan(&e.id, &e.attempt, &e.maxAttempts, &e.leaseUntil); err != nil {
			return nil, fmt.Errorf("gorangetask/store/mysql: scan expired lease: %w", err)
		}
		expiries = append(expiries, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("gorangetask/store/mysql: read expired leases: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("gorangetask/store/mysql: close expired lease rows: %w", err)
	}
	return expiries, nil
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

// CleanupFinished 删除一批超过保留期的终态任务，实现 gorangetask.WorkerStore。
//
// 与回收器同构：一个短事务里"加锁读选出 id → 按主键删除 → 提交"，事务期间不做任何
// 别的事。选行用 SKIP LOCKED，集群里多个 Worker 各跑各的清理器时彼此跳过已锁定的行；
// 删除只按主键，因此批内不会出现"筛选中途条件变了"的情况。
//
// 返回的是**真正删除**的行数（RowsAffected，而不是"选出了几行"）：调用方用
// "是否删满 limit"判断还有没有剩余，所以不需要额外的 COUNT。
//
// 只删 spec.Status 指定的那一个终态：pending / running 一行都不碰，它们是队列本身。
func (c *Client) CleanupFinished(ctx context.Context, spec gorangetask.CleanupSpec) (int64, error) {
	if err := validateCleanupSpec(spec); err != nil {
		return 0, err
	}

	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("gorangetask/store/mysql: begin cleanup transaction: %w", err)
	}
	// 提交成功后 Rollback 返回 sql.ErrTxDone，是可忽略的常规行为。
	defer func() { _ = tx.Rollback() }()

	ids, err := selectCleanupIDs(ctx, tx, spec)
	if err != nil {
		return 0, err
	}
	if len(ids) == 0 {
		return 0, nil
	}

	deleted, err := deleteTasksByID(ctx, tx, ids)
	if err != nil {
		return 0, err
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("gorangetask/store/mysql: commit cleanup transaction: %w", err)
	}
	return deleted, nil
}

// selectCleanupIDs 执行加锁读并收集待删除的 id。
//
// 返回前必须把 rows 关掉：同一条事务连接上未读完的结果集会挡住后面的 DELETE。
func selectCleanupIDs(ctx context.Context, tx *sql.Tx, spec gorangetask.CleanupSpec) ([]int64, error) {
	// 保留期取负：cleanupIDsSQL 统一用 TIMESTAMPADD(..., ?, UTC_TIMESTAMP(6))，
	// 要往前推就得传负数。
	retentionMicros := -spec.Retention.Microseconds()

	rows, err := tx.QueryContext(ctx, cleanupIDsSQL, string(spec.Status), retentionMicros, spec.Limit)
	if err != nil {
		return nil, fmt.Errorf("gorangetask/store/mysql: select cleanup candidates: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("gorangetask/store/mysql: scan cleanup candidate: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("gorangetask/store/mysql: read cleanup candidates: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("gorangetask/store/mysql: close cleanup candidate rows: %w", err)
	}
	return ids, nil
}

// deleteTasksByID 按主键删除给定的 id，返回真正删除的行数。
//
// 行数上限由 MySQL 的 max_allowed_packet 与占位符数量共同约束，而调用方的 limit 是
// 可配置的；这里不额外设上限，是因为批大小本来就该由使用方按自己的行宽与包大小去定
// （默认 1000 个占位符远在限额之内）。
func deleteTasksByID(ctx context.Context, tx *sql.Tx, ids []int64) (int64, error) {
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(ids)), ",")
	args := make([]any, len(ids))
	for i, id := range ids {
		args[i] = id
	}

	res, err := tx.ExecContext(ctx, deleteTasksByIDPrefix+placeholders+")", args...)
	if err != nil {
		return 0, fmt.Errorf("gorangetask/store/mysql: delete %d finished task(s): %w", len(ids), err)
	}
	deleted, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("gorangetask/store/mysql: read deleted rows: %w", err)
	}
	return deleted, nil
}

// validateCleanupSpec 校验清理入参。
//
// **状态必须是终态**：这不是"顺手校验"，而是这条语句唯一的防误删屏障——它一旦放行
// pending / running，被删掉的任务既不会被执行，也不会留下任何痕迹。
func validateCleanupSpec(spec gorangetask.CleanupSpec) error {
	if !spec.Status.IsTerminal() {
		return fmt.Errorf("%w: cleanup status must be terminal, got %q",
			gorangetask.ErrInvalidConfig, spec.Status)
	}
	if spec.Retention <= 0 {
		return fmt.Errorf("%w: cleanup retention must be > 0, got %s",
			gorangetask.ErrInvalidConfig, spec.Retention)
	}
	if spec.Limit < 1 {
		return fmt.Errorf("%w: cleanup limit must be > 0, got %d", gorangetask.ErrInvalidConfig, spec.Limit)
	}
	return nil
}
