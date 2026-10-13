package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	gorangetask "github.com/Tidehyc/gorangetask"
)

// 本文件覆盖 Worker 侧的领取与确认：Claim / Succeed / Retry / MarkDead，
// 以及一个 Worker 端到端的执行闭环。
//
// 用例的取向是"把语义固定在真库上"——尤其是 fencing：旧 owner/epoch 或已过期的
// 租约写回必须被拒绝，这是"至少一次"语义下唯一能防止旧执行者覆盖新结果的东西。

// --- 测试辅助 ---------------------------------------------------------------

// lastErrorColumnLen 与 DDL 里 last_error 的 VARCHAR(2048) 对应。
const lastErrorColumnLen = 2048

// claimOnce 领取一条任务，断言恰好领到一条。
func claimOnce(t *testing.T, c *Client, queue, owner string) gorangetask.ClaimedTask {
	t.Helper()

	tasks, err := c.Claim(context.Background(), gorangetask.ClaimSpec{
		Queue:         queue,
		Limit:         1,
		Owner:         owner,
		LeaseDuration: 30 * time.Second,
	})
	if err != nil {
		t.Fatalf("Claim(%s): %v", queue, err)
	}
	if len(tasks) != 1 {
		t.Fatalf("Claim(%s) returned %d tasks, want 1", queue, len(tasks))
	}
	return tasks[0]
}

// expireLease 把租约的到期时间改到过去，用于构造"租约已过期"的前提。
// 这是直接改库构造前提，不是假装走产品路径。
func expireLease(t *testing.T, db *sql.DB, id gorangetask.TaskID) {
	t.Helper()

	if _, err := db.ExecContext(context.Background(),
		"UPDATE "+tasksTable+" SET lease_until = TIMESTAMPADD(SECOND, -1, UTC_TIMESTAMP(6)) WHERE id = ?",
		int64(id)); err != nil {
		t.Fatalf("expire lease of task %d: %v", id, err)
	}
}

// rewindToPending 把一条 running 任务改回 pending，用于模拟"回收器已经回收过"
// （回收器本身属于后续工作项，此处只造前提）。
func rewindToPending(t *testing.T, db *sql.DB, id gorangetask.TaskID) {
	t.Helper()

	if _, err := db.ExecContext(context.Background(),
		"UPDATE "+tasksTable+
			" SET status = 'pending', lease_owner = NULL, lease_until = NULL, run_at = UTC_TIMESTAMP(6)"+
			" WHERE id = ?", int64(id)); err != nil {
		t.Fatalf("rewind task %d to pending: %v", id, err)
	}
}

// waitForStatus 轮询等待任务进入期望状态。
func waitForStatus(t *testing.T, db *sql.DB, id gorangetask.TaskID, want string, timeout time.Duration) taskRow {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for {
		row := readTask(t, db, id)
		if row.status == want {
			return row
		}
		if time.Now().After(deadline) {
			t.Fatalf("task %d status = %q after %s, want %q (last_error: %q)",
				id, row.status, timeout, want, row.lastError.String)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// workerConfig 返回一个轮询很快、租约较短的 Worker 配置。
func workerConfig(queue string) gorangetask.Config {
	return gorangetask.Config{
		Queues:            []string{queue},
		Concurrency:       1,
		PollInterval:      5 * time.Millisecond,
		MaxPollInterval:   20 * time.Millisecond,
		LeaseDuration:     5 * time.Second,
		HeartbeatInterval: time.Second,
	}
}

func mustRegister(t *testing.T, reg *gorangetask.Registry, taskType string, h gorangetask.Handler) {
	t.Helper()

	if err := reg.Register(taskType, h); err != nil {
		t.Fatalf("Register(%q): %v", taskType, err)
	}
}

// --- Claim -----------------------------------------------------------------

func TestClaimMarksTaskRunningWithLease(t *testing.T) {
	c, db := newTestClientAndDB(t, gorangetask.ClientConfig{})
	ctx := context.Background()

	id, err := c.Enqueue(ctx, gorangetask.Task{
		Queue:       "q-claim-basic",
		Type:        "email.send",
		Payload:     json.RawMessage(`{"to":"a@example.com"}`),
		MaxAttempts: 5,
	})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	before := dbNow(t, db)
	tasks, err := c.Claim(ctx, gorangetask.ClaimSpec{
		Queue:         "q-claim-basic",
		Limit:         1,
		Owner:         "worker-1",
		LeaseDuration: 30 * time.Second,
	})
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if len(tasks) != 1 {
		t.Fatalf("Claim returned %d tasks, want 1", len(tasks))
	}

	got := tasks[0]
	if got.ID != id {
		t.Errorf("claimed id = %d, want %d", got.ID, id)
	}
	if got.Queue != "q-claim-basic" {
		t.Errorf("claimed queue = %q, want q-claim-basic", got.Queue)
	}
	if got.Type != "email.send" {
		t.Errorf("claimed type = %q, want email.send", got.Type)
	}
	if !jsonEqual(mustDecode(t, got.Payload), mustDecode(t, json.RawMessage(`{"to":"a@example.com"}`))) {
		t.Errorf("claimed payload = %s, want the enqueued payload", got.Payload)
	}
	// attempt 是"包含本次"的次数：第一次领取后就是 1。
	if got.Attempt != 1 {
		t.Errorf("claimed attempt = %d, want 1", got.Attempt)
	}
	if got.MaxAttempts != 5 {
		t.Errorf("claimed max attempts = %d, want 5", got.MaxAttempts)
	}
	if got.Owner != "worker-1" {
		t.Errorf("claimed owner = %q, want worker-1", got.Owner)
	}
	if got.Epoch != 1 {
		t.Errorf("claimed epoch = %d, want 1 (首次领取)", got.Epoch)
	}

	row := readTask(t, db, id)
	if row.status != string(gorangetask.StatusRunning) {
		t.Errorf("status = %q, want running", row.status)
	}
	if row.attempt != 1 {
		t.Errorf("attempt column = %d, want 1", row.attempt)
	}
	if row.leaseEpoch != 1 {
		t.Errorf("lease_epoch column = %d, want 1", row.leaseEpoch)
	}
	if !row.leaseOwner.Valid || row.leaseOwner.String != "worker-1" {
		t.Errorf("lease_owner = %v, want worker-1", row.leaseOwner)
	}
	if !row.leaseUntil.Valid {
		t.Fatal("lease_until is NULL; a claimed task must carry a deadline")
	}
	// 租约到期时间应约等于"数据库当前时间 + 30s"，用数据库时钟判断。
	wantUntil := before.Add(30 * time.Second)
	if diff := row.leaseUntil.Time.Sub(wantUntil); diff < -2*time.Second || diff > 2*time.Second {
		t.Errorf("lease_until = %s, want about %s (diff %s)", row.leaseUntil.Time, wantUntil, diff)
	}
}

func TestClaimReturnsEmptyWhenNothingIsDue(t *testing.T) {
	c, _ := newTestClientAndDB(t, gorangetask.ClientConfig{})
	ctx := context.Background()

	t.Run("empty queue", func(t *testing.T) {
		tasks, err := c.Claim(ctx, gorangetask.ClaimSpec{
			Queue: "q-empty", Limit: 1, Owner: "w", LeaseDuration: time.Second,
		})
		if err != nil {
			t.Fatalf("Claim: %v", err)
		}
		if len(tasks) != 0 {
			t.Errorf("Claim returned %d tasks, want 0", len(tasks))
		}
	})

	t.Run("delayed task is not due yet", func(t *testing.T) {
		if _, err := c.Enqueue(ctx, gorangetask.Task{
			Queue:   "q-delayed",
			Type:    "t",
			Payload: json.RawMessage(`{}`),
			RunAt:   time.Now().UTC().Add(time.Hour),
		}); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}

		tasks, err := c.Claim(ctx, gorangetask.ClaimSpec{
			Queue: "q-delayed", Limit: 1, Owner: "w", LeaseDuration: time.Second,
		})
		if err != nil {
			t.Fatalf("Claim: %v", err)
		}
		if len(tasks) != 0 {
			t.Errorf("Claim returned %d tasks; a task that is not due yet must not be claimable", len(tasks))
		}
	})
}

func TestClaimOnlyTakesTasksFromTheRequestedQueue(t *testing.T) {
	c, _ := newTestClientAndDB(t, gorangetask.ClientConfig{})
	ctx := context.Background()

	for _, queue := range []string{"q-a", "q-b"} {
		if _, err := c.Enqueue(ctx, gorangetask.Task{Queue: queue, Type: "t", Payload: json.RawMessage(`{}`)}); err != nil {
			t.Fatalf("Enqueue(%s): %v", queue, err)
		}
	}

	got := claimOnce(t, c, "q-b", "w")
	if got.Queue != "q-b" {
		t.Errorf("claimed from queue %q, want q-b", got.Queue)
	}

	// 队列隔离的另一半：q-a 的那条仍可被领取，说明前一次没有把它带走。
	other := claimOnce(t, c, "q-a", "w")
	if other.Queue != "q-a" {
		t.Errorf("claimed from queue %q, want q-a", other.Queue)
	}
}

func TestClaimRespectsLimitAndOrder(t *testing.T) {
	c, db := newTestClientAndDB(t, gorangetask.ClientConfig{})
	ctx := context.Background()

	base := dbNow(t, db)
	ids := make([]gorangetask.TaskID, 0, 3)
	// 三条都已在过去到期，按 run_at 从早到晚入队，因此领取顺序应当是确定的。
	for i, offset := range []time.Duration{-3 * time.Second, -2 * time.Second, -1 * time.Second} {
		id, err := c.Enqueue(ctx, gorangetask.Task{
			Queue:   "q-order",
			Type:    fmt.Sprintf("t%d", i),
			Payload: json.RawMessage(`{}`),
			RunAt:   base.Add(offset),
		})
		if err != nil {
			t.Fatalf("Enqueue #%d: %v", i, err)
		}
		ids = append(ids, id)
	}

	tasks, err := c.Claim(ctx, gorangetask.ClaimSpec{
		Queue: "q-order", Limit: 2, Owner: "w", LeaseDuration: 30 * time.Second,
	})
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if len(tasks) != 2 {
		t.Fatalf("Claim returned %d tasks, want 2 (LIMIT)", len(tasks))
	}
	if tasks[0].ID != ids[0] || tasks[1].ID != ids[1] {
		t.Errorf("claimed %d,%d; want the two earliest by run_at: %d,%d",
			tasks[0].ID, tasks[1].ID, ids[0], ids[1])
	}

	// 剩下那条仍然可领。
	last := claimOnce(t, c, "q-order", "w")
	if last.ID != ids[2] {
		t.Errorf("remaining claim = %d, want %d", last.ID, ids[2])
	}
}

func TestClaimDoesNotReclaimRunningTasks(t *testing.T) {
	c, db := newTestClientAndDB(t, gorangetask.ClientConfig{})
	ctx := context.Background()

	first, err := c.Enqueue(ctx, gorangetask.Task{Queue: "q-running", Type: "t", Payload: json.RawMessage(`{}`)})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	second, err := c.Enqueue(ctx, gorangetask.Task{Queue: "q-running", Type: "t", Payload: json.RawMessage(`{}`)})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	claimed := claimOnce(t, c, "q-running", "w1")
	if claimed.ID != first {
		t.Fatalf("first claim = %d, want %d", claimed.ID, first)
	}

	// 第二次领取必须跳过已被 w1 持有的那条，而不是把它再领一遍。
	next := claimOnce(t, c, "q-running", "w2")
	if next.ID != second {
		t.Errorf("second claim = %d, want %d (已被领取的任务不应被再次领取)", next.ID, second)
	}

	// w1 那条的 epoch 必须仍是 1：没有被别人重新领取过。
	row := readTask(t, db, first)
	if row.leaseEpoch != 1 {
		t.Errorf("task %d lease_epoch = %d, want 1 (不得被重复领取)", first, row.leaseEpoch)
	}
	if !row.leaseOwner.Valid || row.leaseOwner.String != "w1" {
		t.Errorf("task %d lease_owner = %v, want w1", first, row.leaseOwner)
	}
	if row.attempt != 1 {
		t.Errorf("task %d attempt = %d, want 1", first, row.attempt)
	}
}

func TestClaimIncrementsEpochAndAttemptOnReclaim(t *testing.T) {
	c, db := newTestClientAndDB(t, gorangetask.ClientConfig{})
	ctx := context.Background()

	id, err := c.Enqueue(ctx, gorangetask.Task{Queue: "q-reclaim", Type: "t", Payload: json.RawMessage(`{}`), MaxAttempts: 5})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	first := claimOnce(t, c, "q-reclaim", "w1")
	if first.Epoch != 1 || first.Attempt != 1 {
		t.Fatalf("first claim epoch/attempt = %d/%d, want 1/1", first.Epoch, first.Attempt)
	}

	// 模拟"租约到期后被回收器放回队列"。
	rewindToPending(t, db, id)

	again := claimOnce(t, c, "q-reclaim", "w2")
	if again.ID != id {
		t.Fatalf("reclaim id = %d, want %d", again.ID, id)
	}
	// epoch 只增不减：这是 fencing 能区分"本次执行"和"上一次执行"的基础。
	if again.Epoch != 2 {
		t.Errorf("reclaim epoch = %d, want 2", again.Epoch)
	}
	if again.Attempt != 2 {
		t.Errorf("reclaim attempt = %d, want 2", again.Attempt)
	}
	if again.Owner != "w2" {
		t.Errorf("reclaim owner = %q, want w2", again.Owner)
	}
}

func TestClaimRejectsInvalidSpec(t *testing.T) {
	c, db := newTestClientAndDB(t, gorangetask.ClientConfig{})
	before := countTasks(t, db)

	tests := []struct {
		name string
		spec gorangetask.ClaimSpec
	}{
		{"empty queue", gorangetask.ClaimSpec{Queue: "", Limit: 1, Owner: "w", LeaseDuration: time.Second}},
		{"zero limit", gorangetask.ClaimSpec{Queue: "q", Limit: 0, Owner: "w", LeaseDuration: time.Second}},
		{"negative limit", gorangetask.ClaimSpec{Queue: "q", Limit: -1, Owner: "w", LeaseDuration: time.Second}},
		{"empty owner", gorangetask.ClaimSpec{Queue: "q", Limit: 1, Owner: "", LeaseDuration: time.Second}},
		{"zero lease", gorangetask.ClaimSpec{Queue: "q", Limit: 1, Owner: "w", LeaseDuration: 0}},
		{"negative lease", gorangetask.ClaimSpec{Queue: "q", Limit: 1, Owner: "w", LeaseDuration: -time.Second}},
		{"oversized owner", gorangetask.ClaimSpec{Queue: "q", Limit: 1, Owner: strings.Repeat("x", 129), LeaseDuration: time.Second}},
		{"oversized queue", gorangetask.ClaimSpec{Queue: strings.Repeat("q", 65), Limit: 1, Owner: "w", LeaseDuration: time.Second}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tasks, err := c.Claim(context.Background(), tt.spec)
			if !errors.Is(err, gorangetask.ErrInvalidConfig) {
				t.Fatalf("Claim() error = %v, want ErrInvalidConfig", err)
			}
			if tasks != nil {
				t.Errorf("Claim() returned %v, want nil on invalid spec", tasks)
			}
		})
	}

	if after := countTasks(t, db); after != before {
		t.Errorf("task count changed from %d to %d; an invalid spec must not touch the database", before, after)
	}
}

// TestClaimQueryUsesClaimIndex 是技术设计 §8.1 要求的证据：领取查询必须走
// ix_gotask_claim 并且是覆盖索引（Using index）。索引一旦失效，加锁范围就会
// 扩大到不符合条件的行，多 Worker 领取的并发性随之崩塌。
func TestClaimQueryUsesClaimIndex(t *testing.T) {
	_, db := newTestClientAndDB(t, gorangetask.ClientConfig{})

	// EXPLAIN 不接受占位符，因此把条件写成字面量；查询形状与 claimTaskIDsSQL 一致。
	// FOR UPDATE 子句不影响优化器的索引选择，这里省略它以便 EXPLAIN 稳定。
	const explain = `EXPLAIN SELECT id FROM ` + tasksTable + `
        WHERE queue_name = 'q-explain'
          AND status = 'pending'
          AND run_at <= UTC_TIMESTAMP(6)
        ORDER BY run_at, id
        LIMIT 1`

	rows, err := db.QueryContext(context.Background(), explain)
	if err != nil {
		t.Fatalf("EXPLAIN: %v", err)
	}
	defer func() { _ = rows.Close() }()

	columns, err := rows.Columns()
	if err != nil {
		t.Fatalf("read EXPLAIN columns: %v", err)
	}
	if !rows.Next() {
		t.Fatal("EXPLAIN returned no rows")
	}
	values := make([]any, len(columns))
	pointers := make([]any, len(columns))
	for i := range values {
		pointers[i] = &values[i]
	}
	if err := rows.Scan(pointers...); err != nil {
		t.Fatalf("scan EXPLAIN row: %v", err)
	}

	plan := make(map[string]string, len(columns))
	for i, name := range columns {
		if b, ok := values[i].([]byte); ok {
			plan[name] = string(b)
		}
	}
	t.Logf("claim query plan: key=%q type=%q Extra=%q", plan["key"], plan["type"], plan["Extra"])

	if plan["key"] != "ix_gotask_claim" {
		t.Errorf("claim query uses index %q, want ix_gotask_claim", plan["key"])
	}
	if !strings.Contains(plan["Extra"], "Using index") {
		t.Errorf("claim query Extra = %q, want it to contain \"Using index\" (覆盖索引)", plan["Extra"])
	}
}

// --- Succeed ---------------------------------------------------------------

func TestSucceedWritesTerminalState(t *testing.T) {
	c, db := newTestClientAndDB(t, gorangetask.ClientConfig{})
	ctx := context.Background()

	id, err := c.Enqueue(ctx, gorangetask.Task{Queue: "q-succeed", Type: "t", Payload: json.RawMessage(`{}`)})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	task := claimOnce(t, c, "q-succeed", "w1")

	before := dbNow(t, db)
	if err := c.Succeed(ctx, task); err != nil {
		t.Fatalf("Succeed: %v", err)
	}

	row := readTask(t, db, id)
	if row.status != string(gorangetask.StatusSucceeded) {
		t.Errorf("status = %q, want succeeded", row.status)
	}
	if row.leaseOwner.Valid || row.leaseUntil.Valid {
		t.Errorf("lease not cleared: owner=%v until=%v", row.leaseOwner, row.leaseUntil)
	}
	if row.lastError.Valid {
		t.Errorf("last_error = %q, want NULL after success", row.lastError.String)
	}
	if !row.finishedAt.Valid {
		t.Fatal("finished_at is NULL; a terminal state must stamp it")
	}
	if row.finishedAt.Time.Before(before) {
		t.Errorf("finished_at = %s is before the ack time %s", row.finishedAt.Time, before)
	}
}

func TestSucceedRejectsStaleLease(t *testing.T) {
	c, db := newTestClientAndDB(t, gorangetask.ClientConfig{})
	ctx := context.Background()

	t.Run("wrong owner", func(t *testing.T) {
		id, err := c.Enqueue(ctx, gorangetask.Task{Queue: "q-stale-owner", Type: "t", Payload: json.RawMessage(`{}`)})
		if err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
		task := claimOnce(t, c, "q-stale-owner", "w1")
		task.Owner = "someone-else"

		if err := c.Succeed(ctx, task); !errors.Is(err, gorangetask.ErrLeaseLost) {
			t.Fatalf("Succeed() error = %v, want ErrLeaseLost", err)
		}
		if row := readTask(t, db, id); row.status != string(gorangetask.StatusRunning) {
			t.Errorf("status = %q, want running (写回必须被拒绝)", row.status)
		}
	})

	t.Run("stale epoch", func(t *testing.T) {
		id, err := c.Enqueue(ctx, gorangetask.Task{Queue: "q-stale-epoch", Type: "t", Payload: json.RawMessage(`{}`)})
		if err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
		task := claimOnce(t, c, "q-stale-epoch", "w1")
		task.Epoch = 99

		if err := c.Succeed(ctx, task); !errors.Is(err, gorangetask.ErrLeaseLost) {
			t.Fatalf("Succeed() error = %v, want ErrLeaseLost", err)
		}
		if row := readTask(t, db, id); row.status != string(gorangetask.StatusRunning) {
			t.Errorf("status = %q, want running", row.status)
		}
	})

	t.Run("expired lease", func(t *testing.T) {
		id, err := c.Enqueue(ctx, gorangetask.Task{Queue: "q-stale-expired", Type: "t", Payload: json.RawMessage(`{}`)})
		if err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
		task := claimOnce(t, c, "q-stale-expired", "w1")
		expireLease(t, db, id)

		// 这是"Handler 跑太久、租约已过期"的场景：结果不得写回终态。
		if err := c.Succeed(ctx, task); !errors.Is(err, gorangetask.ErrLeaseLost) {
			t.Fatalf("Succeed() error = %v, want ErrLeaseLost", err)
		}
		if row := readTask(t, db, id); row.status != string(gorangetask.StatusRunning) {
			t.Errorf("status = %q, want running", row.status)
		}
	})
}

// --- Retry -----------------------------------------------------------------

func TestRetryReturnsTaskToPending(t *testing.T) {
	c, db := newTestClientAndDB(t, gorangetask.ClientConfig{})
	ctx := context.Background()

	id, err := c.Enqueue(ctx, gorangetask.Task{Queue: "q-retry", Type: "t", Payload: json.RawMessage(`{}`), MaxAttempts: 3})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	task := claimOnce(t, c, "q-retry", "w1")

	if err := c.Retry(ctx, task, 0, "smtp unavailable"); err != nil {
		t.Fatalf("Retry: %v", err)
	}

	row := readTask(t, db, id)
	if row.status != string(gorangetask.StatusPending) {
		t.Errorf("status = %q, want pending", row.status)
	}
	if row.leaseOwner.Valid || row.leaseUntil.Valid {
		t.Errorf("lease not cleared: owner=%v until=%v", row.leaseOwner, row.leaseUntil)
	}
	if !row.lastError.Valid || row.lastError.String != "smtp unavailable" {
		t.Errorf("last_error = %q, want the handler error summary", row.lastError.String)
	}
	if row.finishedAt.Valid {
		t.Error("finished_at must stay NULL for a retryable failure")
	}
	// run_at 已被推到"数据库当前时间"，因此应当立即可再次领取。
	again := claimOnce(t, c, "q-retry", "w2")
	if again.ID != id {
		t.Fatalf("reclaim id = %d, want %d", again.ID, id)
	}
	if again.Attempt != 2 {
		t.Errorf("reclaim attempt = %d, want 2", again.Attempt)
	}
}

func TestRetryHonoursDelay(t *testing.T) {
	c, db := newTestClientAndDB(t, gorangetask.ClientConfig{})
	ctx := context.Background()

	id, err := c.Enqueue(ctx, gorangetask.Task{Queue: "q-retry-delay", Type: "t", Payload: json.RawMessage(`{}`)})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	task := claimOnce(t, c, "q-retry-delay", "w1")

	before := dbNow(t, db)
	if err := c.Retry(ctx, task, time.Hour, "later"); err != nil {
		t.Fatalf("Retry: %v", err)
	}

	// 延迟必须由数据库时钟计算，而不是应用机器时钟。
	row := readTask(t, db, id)
	want := before.Add(time.Hour)
	if diff := row.runAt.Sub(want); diff < -2*time.Second || diff > 2*time.Second {
		t.Errorf("run_at = %s, want about %s (数据库时钟 + 1h, diff %s)", row.runAt, want, diff)
	}

	tasks, err := c.Claim(ctx, gorangetask.ClaimSpec{
		Queue: "q-retry-delay", Limit: 1, Owner: "w2", LeaseDuration: time.Second,
	})
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if len(tasks) != 0 {
		t.Errorf("Claim returned %d tasks; a task delayed by 1h must not be claimable now", len(tasks))
	}
}

func TestRetryTruncatesLongError(t *testing.T) {
	c, db := newTestClientAndDB(t, gorangetask.ClientConfig{})
	ctx := context.Background()

	id, err := c.Enqueue(ctx, gorangetask.Task{Queue: "q-retry-long", Type: "t", Payload: json.RawMessage(`{}`)})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	task := claimOnce(t, c, "q-retry-long", "w1")

	// 用多字节字符，顺带确认截断是按字符数而不是字节数。
	long := strings.Repeat("错误", 3000) // 6000 个字符
	if err := c.Retry(ctx, task, 0, long); err != nil {
		t.Fatalf("Retry: %v", err)
	}

	row := readTask(t, db, id)
	if !row.lastError.Valid {
		t.Fatal("last_error is NULL, want the truncated summary")
	}
	if n := len([]rune(row.lastError.String)); n != lastErrorColumnLen {
		t.Errorf("last_error length = %d characters, want %d (VARCHAR 列宽)", n, lastErrorColumnLen)
	}
	if !strings.HasPrefix(row.lastError.String, "错误错误") {
		t.Errorf("last_error = %.12q..., want it to keep the leading characters", row.lastError.String)
	}
}

func TestRetryRejectsNegativeDelay(t *testing.T) {
	c, _ := newTestClientAndDB(t, gorangetask.ClientConfig{})
	ctx := context.Background()

	if _, err := c.Enqueue(ctx, gorangetask.Task{Queue: "q-retry-neg", Type: "t", Payload: json.RawMessage(`{}`)}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	task := claimOnce(t, c, "q-retry-neg", "w1")

	err := c.Retry(ctx, task, -time.Second, "x")
	if !errors.Is(err, gorangetask.ErrInvalidTask) {
		t.Fatalf("Retry(negative delay) error = %v, want ErrInvalidTask", err)
	}
}

// --- MarkDead --------------------------------------------------------------

func TestMarkDeadWritesTerminalState(t *testing.T) {
	c, db := newTestClientAndDB(t, gorangetask.ClientConfig{})
	ctx := context.Background()

	id, err := c.Enqueue(ctx, gorangetask.Task{Queue: "q-dead", Type: "t", Payload: json.RawMessage(`{}`), MaxAttempts: 1})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	task := claimOnce(t, c, "q-dead", "w1")

	if err := c.MarkDead(ctx, task, "permanent: order does not exist"); err != nil {
		t.Fatalf("MarkDead: %v", err)
	}

	row := readTask(t, db, id)
	if row.status != string(gorangetask.StatusDead) {
		t.Errorf("status = %q, want dead", row.status)
	}
	if row.leaseOwner.Valid || row.leaseUntil.Valid {
		t.Errorf("lease not cleared: owner=%v until=%v", row.leaseOwner, row.leaseUntil)
	}
	if !row.finishedAt.Valid {
		t.Error("finished_at is NULL; dead is a terminal state")
	}
	if row.lastError.String != "permanent: order does not exist" {
		t.Errorf("last_error = %q, want the failure summary", row.lastError.String)
	}

	// 死信不再被领取。
	tasks, err := c.Claim(ctx, gorangetask.ClaimSpec{
		Queue: "q-dead", Limit: 1, Owner: "w2", LeaseDuration: time.Second,
	})
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if len(tasks) != 0 {
		t.Errorf("Claim returned %d tasks; a dead task must not be reclaimable", len(tasks))
	}
}

func TestMarkDeadRejectsStaleLease(t *testing.T) {
	c, db := newTestClientAndDB(t, gorangetask.ClientConfig{})
	ctx := context.Background()

	id, err := c.Enqueue(ctx, gorangetask.Task{Queue: "q-dead-stale", Type: "t", Payload: json.RawMessage(`{}`)})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	task := claimOnce(t, c, "q-dead-stale", "w1")
	task.Epoch = task.Epoch + 1

	if err := c.MarkDead(ctx, task, "boom"); !errors.Is(err, gorangetask.ErrLeaseLost) {
		t.Fatalf("MarkDead() error = %v, want ErrLeaseLost", err)
	}
	if row := readTask(t, db, id); row.status != string(gorangetask.StatusRunning) {
		t.Errorf("status = %q, want running", row.status)
	}
}

func TestWriteBackRejectsMalformedLease(t *testing.T) {
	c, _ := newTestClientAndDB(t, gorangetask.ClientConfig{})
	ctx := context.Background()

	// 没带 task id 的凭据：只能用错了来源，直接拒绝而不是发一条必定影响 0 行的 UPDATE。
	if err := c.Succeed(ctx, gorangetask.ClaimedTask{Owner: "w"}); !errors.Is(err, gorangetask.ErrInvalidTask) {
		t.Errorf("Succeed(zero id) error = %v, want ErrInvalidTask", err)
	}
	if err := c.Retry(ctx, gorangetask.ClaimedTask{ID: 1}, 0, "x"); !errors.Is(err, gorangetask.ErrInvalidTask) {
		t.Errorf("Retry(empty owner) error = %v, want ErrInvalidTask", err)
	}
	if err := c.MarkDead(ctx, gorangetask.ClaimedTask{}, "x"); !errors.Is(err, gorangetask.ErrInvalidTask) {
		t.Errorf("MarkDead(empty lease) error = %v, want ErrInvalidTask", err)
	}
}

// TestStoreSurfacesDatabaseErrorsInsteadOfLeaseLost 固定一条重要的区分：
// **数据库不可用不是"租约丢失"**。把两者混为一谈会把一次数据库抖动伪装成
// "任务已被别人接手"，写回失败就被静默咽掉了——所以这里要求错误原样冒泡。
func TestStoreSurfacesDatabaseErrorsInsteadOfLeaseLost(t *testing.T) {
	c, db := newTestClientAndDB(t, gorangetask.ClientConfig{})
	ctx := context.Background()

	if err := db.Close(); err != nil {
		t.Fatalf("close database: %v", err)
	}

	_, claimErr := c.Claim(ctx, gorangetask.ClaimSpec{
		Queue: "q", Limit: 1, Owner: "w", LeaseDuration: time.Second,
	})
	task := gorangetask.ClaimedTask{ID: 1, Owner: "w", Epoch: 1}

	tests := []struct {
		name string
		err  error
		want string
	}{
		{"Claim", claimErr, "begin claim transaction"},
		{"Succeed", c.Succeed(ctx, task), "succeed task"},
		{"Retry", c.Retry(ctx, task, 0, "x"), "retry task"},
		{"MarkDead", c.MarkDead(ctx, task, "x"), "mark task dead"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.err == nil {
				t.Fatal("expected an error from a closed database")
			}
			if errors.Is(tt.err, gorangetask.ErrLeaseLost) {
				t.Errorf("error = %v must not be reported as ErrLeaseLost", tt.err)
			}
			if !strings.Contains(tt.err.Error(), tt.want) {
				t.Errorf("error = %v, want it to mention %q", tt.err, tt.want)
			}
		})
	}
}

// --- Worker 端到端 ---------------------------------------------------------

// TestWorkerRunExecutesTaskEndToEnd 是这一项的核心证据：一个 Worker 能在真库上
// 把任务从 pending 推到 succeeded，并且 Handler 运行时任务**已经在库里是 running**
// ——这正是"claim 事务先提交、Handler 在事务外运行"的直接证明。
func TestWorkerRunExecutesTaskEndToEnd(t *testing.T) {
	c, db := newTestClientAndDB(t, gorangetask.ClientConfig{})
	ctx := context.Background()

	const queue = "q-worker-e2e"
	id, err := c.Enqueue(ctx, gorangetask.Task{
		Queue:   queue,
		Type:    "e2e.task",
		Payload: json.RawMessage(`{"n":1}`),
	})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// Handler 在读库时用的是另一条连接：如果它跑在 claim 事务里，这里会读到
	// pending（甚至读不到），那就说明锁没放开。
	var statusSeenByHandler atomic.Value
	var payloadSeen atomic.Value
	reg := gorangetask.NewRegistry()
	mustRegister(t, reg, "e2e.task", func(hctx context.Context, payload json.RawMessage) error {
		payloadSeen.Store(string(payload))
		var status string
		if err := db.QueryRowContext(hctx,
			"SELECT status FROM "+tasksTable+" WHERE id = ?", int64(id)).Scan(&status); err != nil {
			return err
		}
		statusSeenByHandler.Store(status)
		return nil
	})

	w, err := gorangetask.NewWorker(c, reg, workerConfig(queue))
	if err != nil {
		t.Fatalf("NewWorker: %v", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	runDone := make(chan error, 1)
	go func() { runDone <- w.Run(runCtx) }()

	row := waitForStatus(t, db, id, string(gorangetask.StatusSucceeded), 10*time.Second)
	cancel()
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run() = %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run() did not return after cancellation")
	}

	if got, _ := statusSeenByHandler.Load().(string); got != string(gorangetask.StatusRunning) {
		t.Errorf("handler observed status %q, want running (Handler 必须在 claim 事务提交后运行)", got)
	}
	if got, _ := payloadSeen.Load().(string); !jsonEqual(mustDecode(t, json.RawMessage(got)), mustDecode(t, json.RawMessage(`{"n":1}`))) {
		t.Errorf("handler payload = %s, want the enqueued payload", got)
	}
	if row.attempt != 1 {
		t.Errorf("attempt = %d, want 1", row.attempt)
	}
	if row.leaseOwner.Valid || row.leaseUntil.Valid {
		t.Errorf("lease not cleared after success: owner=%v until=%v", row.leaseOwner, row.leaseUntil)
	}
}

// TestWorkerRunRetriesThenSucceeds 覆盖"失败 → 回到 pending → 被重新领取 → 成功"。
func TestWorkerRunRetriesThenSucceeds(t *testing.T) {
	c, db := newTestClientAndDB(t, gorangetask.ClientConfig{})
	ctx := context.Background()

	const queue = "q-worker-retry"
	id, err := c.Enqueue(ctx, gorangetask.Task{
		Queue:       queue,
		Type:        "flaky.task",
		Payload:     json.RawMessage(`{}`),
		MaxAttempts: 3,
	})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	var calls atomic.Int32
	reg := gorangetask.NewRegistry()
	mustRegister(t, reg, "flaky.task", func(context.Context, json.RawMessage) error {
		if calls.Add(1) == 1 {
			return errors.New("first attempt fails")
		}
		return nil
	})

	w, err := gorangetask.NewWorker(c, reg, workerConfig(queue))
	if err != nil {
		t.Fatalf("NewWorker: %v", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	runDone := make(chan error, 1)
	go func() { runDone <- w.Run(runCtx) }()
	defer func() {
		cancel()
		<-runDone
	}()

	waitForStatus(t, db, id, string(gorangetask.StatusSucceeded), 10*time.Second)

	if got := calls.Load(); got != 2 {
		t.Errorf("handler called %d times, want 2 (第一次失败后应被重试一次)", got)
	}
	row := readTask(t, db, id)
	if row.attempt != 2 {
		t.Errorf("attempt = %d, want 2", row.attempt)
	}
	if row.lastError.Valid {
		t.Errorf("last_error = %q, want NULL after the task finally succeeded", row.lastError.String)
	}
}

// TestWorkerRunMarksDeadWhenAttemptsExhausted 覆盖"重试到次数耗尽 → dead"。
func TestWorkerRunMarksDeadWhenAttemptsExhausted(t *testing.T) {
	c, db := newTestClientAndDB(t, gorangetask.ClientConfig{})
	ctx := context.Background()

	const queue = "q-worker-dead"
	id, err := c.Enqueue(ctx, gorangetask.Task{
		Queue:       queue,
		Type:        "always-fails",
		Payload:     json.RawMessage(`{}`),
		MaxAttempts: 2,
	})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	reg := gorangetask.NewRegistry()
	mustRegister(t, reg, "always-fails", func(context.Context, json.RawMessage) error {
		return errors.New("still broken")
	})

	w, err := gorangetask.NewWorker(c, reg, workerConfig(queue))
	if err != nil {
		t.Fatalf("NewWorker: %v", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	runDone := make(chan error, 1)
	go func() { runDone <- w.Run(runCtx) }()
	defer func() {
		cancel()
		<-runDone
	}()

	row := waitForStatus(t, db, id, string(gorangetask.StatusDead), 10*time.Second)
	if row.attempt != 2 {
		t.Errorf("attempt = %d, want 2 (MaxAttempts 用满)", row.attempt)
	}
	if !strings.Contains(row.lastError.String, "still broken") {
		t.Errorf("last_error = %q, want the handler failure summary", row.lastError.String)
	}
	if !row.finishedAt.Valid {
		t.Error("finished_at is NULL; dead is terminal")
	}
}

// --- 小工具 ---------------------------------------------------------------

func mustDecode(t *testing.T, raw json.RawMessage) any {
	t.Helper()

	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatalf("decode %s: %v", raw, err)
	}
	return v
}
