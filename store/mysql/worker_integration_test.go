package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
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

// --- 多 Worker 并发领取 -----------------------------------------------------

// leaseState 是一条任务在库里的租约状态，用来断言"只被领取过一次"。
type leaseState struct {
	epoch   int64
	attempt int64
	status  string
	owner   sql.NullString
}

func readLeaseStates(t *testing.T, db *sql.DB, queue string) map[int64]leaseState {
	t.Helper()

	rows, err := db.QueryContext(context.Background(),
		"SELECT id, lease_epoch, attempt, status, lease_owner FROM "+tasksTable+" WHERE queue_name = ?", queue)
	if err != nil {
		t.Fatalf("read lease states: %v", err)
	}
	defer func() { _ = rows.Close() }()

	out := make(map[int64]leaseState)
	for rows.Next() {
		var (
			id int64
			s  leaseState
		)
		if err := rows.Scan(&id, &s.epoch, &s.attempt, &s.status, &s.owner); err != nil {
			t.Fatalf("scan lease state: %v", err)
		}
		out[id] = s
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate lease states: %v", err)
	}
	return out
}

// TestConcurrentClaimNeverDoubleClaims 是本项的核心证据：多个领取者同时抢同一批
// 任务时，**每条任务只能被领走一次**。
//
// 断言分两层：
//   - 进程侧：所有领取者拿到的 task id 两两不重复，且合起来恰好覆盖全部任务；
//   - 数据库侧：每条任务的 lease_epoch 与 attempt 都还是 1 —— epoch 每次领取 +1，
//     所以 1 就等于"只被领取过一次"。第二层不依赖进程侧的记录，即使进程侧代码
//     写错了也抓得住。
//
// 领取者故意用不同的 Limit（1..5），顺带覆盖"一次领一批"的路径：
// 批量领取在一个事务里逐条 UPDATE，是并发下最容易出问题的地方。
func TestConcurrentClaimNeverDoubleClaims(t *testing.T) {
	c, db := newTestClientAndDB(t, gorangetask.ClientConfig{})
	ctx := context.Background()

	const (
		queue      = "q-concurrent-claim"
		taskCount  = 40
		claimerNum = 8
	)

	want := make(map[gorangetask.TaskID]bool, taskCount)
	for i := 0; i < taskCount; i++ {
		id, err := c.Enqueue(ctx, gorangetask.Task{
			Queue:   queue,
			Type:    "t",
			Payload: json.RawMessage(`{}`),
		})
		if err != nil {
			t.Fatalf("Enqueue #%d: %v", i, err)
		}
		want[id] = true
	}

	start := make(chan struct{})
	var (
		wg         sync.WaitGroup
		mu         sync.Mutex
		claimedBy  = make(map[gorangetask.TaskID]string, taskCount)
		violations []string
	)

	for i := 0; i < claimerNum; i++ {
		owner := fmt.Sprintf("worker-%d", i)
		limit := 1 + i%5

		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start // 所有领取者尽量在同一时刻出发，把竞争窗口放到最大

			for {
				got, err := c.Claim(ctx, gorangetask.ClaimSpec{
					Queue:         queue,
					Limit:         limit,
					Owner:         owner,
					LeaseDuration: time.Minute, // 租约远长于用例时长，排除"租约过期被重领"
				})
				if err != nil {
					// 并发领取不该报错：SKIP LOCKED 不等待，因此不存在死锁(1213)。
					mu.Lock()
					violations = append(violations, fmt.Sprintf("%s: Claim failed: %v", owner, err))
					mu.Unlock()
					return
				}
				if len(got) == 0 {
					return
				}

				mu.Lock()
				for _, task := range got {
					if prev, exists := claimedBy[task.ID]; exists {
						violations = append(violations,
							fmt.Sprintf("task %d claimed twice: %s and %s", task.ID, prev, owner))
					}
					claimedBy[task.ID] = owner
				}
				mu.Unlock()
			}
		}()
	}
	close(start)
	wg.Wait()

	if len(violations) > 0 {
		t.Fatalf("concurrent claim violations:\n  %s", strings.Join(violations, "\n  "))
	}
	if len(claimedBy) != taskCount {
		t.Errorf("claimed %d distinct tasks, want %d", len(claimedBy), taskCount)
	}
	for id := range want {
		if _, ok := claimedBy[id]; !ok {
			t.Errorf("task %d was never claimed (任务丢失)", id)
		}
	}

	// 记录领取的分布：确认任务确实被多个领取者分散拿走，而不是被一个包圆
	// （用日志而非断言，避免依赖 goroutine 调度的时序而变成 flaky）。
	perOwner := make(map[string]int, claimerNum)
	for _, owner := range claimedBy {
		perOwner[owner]++
	}
	t.Logf("claim distribution (owners=%d): %v", len(perOwner), perOwner)

	// 数据库侧的独立复核：epoch/attempt 仍是 1 ⇒ 每条只被领取过一次。
	states := readLeaseStates(t, db, queue)
	if len(states) != taskCount {
		t.Fatalf("read %d task rows, want %d", len(states), taskCount)
	}
	for id, state := range states {
		if state.epoch != 1 {
			t.Errorf("task %d lease_epoch = %d, want 1 (被领取了不止一次)", id, state.epoch)
		}
		if state.attempt != 1 {
			t.Errorf("task %d attempt = %d, want 1", id, state.attempt)
		}
		if state.status != string(gorangetask.StatusRunning) {
			t.Errorf("task %d status = %q, want running", id, state.status)
		}
		if !state.owner.Valid {
			t.Errorf("task %d has no lease owner", id)
		}
	}
}

// TestClaimSkipsRowsLockedByAnotherSession 直接验证 SKIP LOCKED 的语义：
// 另一会话持有某行锁时，Claim 必须**跳过它**拿走其余任务，而不是阻塞等待。
// 这条性质是多 Worker 能并行领取的前提——没有它，抢占就退化成一串串行等待。
func TestClaimSkipsRowsLockedByAnotherSession(t *testing.T) {
	c, db := newTestClientAndDB(t, gorangetask.ClientConfig{})
	ctx := context.Background()

	const queue = "q-skip-locked"
	ids := make([]gorangetask.TaskID, 0, 3)
	for i := 0; i < 3; i++ {
		id, err := c.Enqueue(ctx, gorangetask.Task{Queue: queue, Type: "t", Payload: json.RawMessage(`{}`)})
		if err != nil {
			t.Fatalf("Enqueue #%d: %v", i, err)
		}
		ids = append(ids, id)
	}

	// 在另一条连接上锁住"本该第一个被领走"的那行。这里刻意用普通 FOR UPDATE
	// （不带 SKIP LOCKED），所以它是真的把锁握住了。
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin locking transaction: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	var lockedID int64
	if err := tx.QueryRowContext(ctx,
		"SELECT id FROM "+tasksTable+
			" WHERE queue_name = ? AND status = 'pending' ORDER BY run_at, id LIMIT 1 FOR UPDATE",
		queue).Scan(&lockedID); err != nil {
		t.Fatalf("lock first claimable row: %v", err)
	}

	claimCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	started := time.Now()
	tasks, err := c.Claim(claimCtx, gorangetask.ClaimSpec{
		Queue: queue, Limit: 3, Owner: "w", LeaseDuration: time.Minute,
	})
	elapsed := time.Since(started)
	if err != nil {
		t.Fatalf("Claim failed (被锁的行不该让它阻塞或报错): %v", err)
	}
	if elapsed > 3*time.Second {
		t.Errorf("Claim took %s; SKIP LOCKED should not wait for the locked row", elapsed)
	}

	// 只该拿到未锁定的两条。
	got := make(map[gorangetask.TaskID]bool, len(tasks))
	for _, task := range tasks {
		got[task.ID] = true
	}
	if len(tasks) != len(ids)-1 {
		t.Errorf("claimed %d tasks, want %d (被锁的行必须被跳过)", len(tasks), len(ids)-1)
	}
	if got[gorangetask.TaskID(lockedID)] {
		t.Errorf("claimed the locked task %d; SKIP LOCKED must skip it", lockedID)
	}
	for _, id := range ids {
		if int64(id) == lockedID {
			continue
		}
		if !got[id] {
			t.Errorf("task %d was not claimed, want it in the batch", id)
		}
	}

	// 锁一释放，那条又可被领取 ⇒ 之前不是"领不到"，只是"被跳过"。
	if err := tx.Rollback(); err != nil {
		t.Fatalf("release lock: %v", err)
	}
	rest := claimOnce(t, c, queue, "w2")
	if int64(rest.ID) != lockedID {
		t.Errorf("after the lock is released, claimed %d, want the previously locked %d", rest.ID, lockedID)
	}
}

// TestMultipleWorkersProcessEachTaskExactlyOnce 把并发领取放到 Worker 层面验证：
// 多个 Worker 实例同时跑，每条任务必须**恰好被执行一次**。若两个 Worker 能同时
// 持有同一条任务的有效租约，这里的计数就会大于 1。
func TestMultipleWorkersProcessEachTaskExactlyOnce(t *testing.T) {
	c, db := newTestClientAndDB(t, gorangetask.ClientConfig{})
	ctx := context.Background()

	const (
		queue     = "q-multi-worker"
		workerNum = 4
		taskCount = 24
		taskType  = "multi.task"
		maxWait   = 30 * time.Second
	)

	for i := 0; i < taskCount; i++ {
		if _, err := c.Enqueue(ctx, gorangetask.Task{
			Queue:   queue,
			Type:    taskType,
			Payload: json.RawMessage(fmt.Sprintf(`{"n":%d}`, i)),
		}); err != nil {
			t.Fatalf("Enqueue #%d: %v", i, err)
		}
	}

	// counts[n] 记录第 n 条任务被 Handler 执行了几次。
	var counts sync.Map
	reg := gorangetask.NewRegistry()
	mustRegister(t, reg, taskType, func(_ context.Context, payload json.RawMessage) error {
		var in struct {
			N int `json:"n"`
		}
		if err := json.Unmarshal(payload, &in); err != nil {
			return err
		}
		counter, _ := counts.LoadOrStore(in.N, new(atomic.Int32))
		counter.(*atomic.Int32).Add(1)
		return nil
	})

	// 每个 Worker 自己再开 2 个并发协程，让同时竞争领取的协程数达到 8。
	cfg := workerConfig(queue)
	cfg.Concurrency = 2

	runCtx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	runErrs := make(chan error, workerNum)

	for i := 0; i < workerNum; i++ {
		w, err := gorangetask.NewWorker(c, reg, cfg)
		if err != nil {
			t.Fatalf("NewWorker #%d: %v", i, err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			runErrs <- w.Run(runCtx)
		}()
	}

	waitForSucceededCount(t, db, queue, taskCount, maxWait)
	cancel()
	wg.Wait()
	close(runErrs)

	for err := range runErrs {
		if err != nil {
			t.Errorf("Run() = %v, want nil", err)
		}
	}

	for i := 0; i < taskCount; i++ {
		value, ok := counts.Load(i)
		if !ok {
			t.Errorf("task #%d was never handled", i)
			continue
		}
		if got := value.(*atomic.Int32).Load(); got != 1 {
			t.Errorf("task #%d handled %d times, want exactly 1 (并发下不得双重持有租约)", i, got)
		}
	}
}

// waitForSucceededCount 等到队列里进入 succeeded 的任务数达到 want。
func waitForSucceededCount(t *testing.T, db *sql.DB, queue string, want int, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for {
		var got int
		if err := db.QueryRowContext(context.Background(),
			"SELECT COUNT(*) FROM "+tasksTable+" WHERE queue_name = ? AND status = 'succeeded'",
			queue).Scan(&got); err != nil {
			t.Fatalf("count succeeded tasks: %v", err)
		}
		if got >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d/%d tasks succeeded after %s", got, want, timeout)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestClaimRestoresPerTaskTimeout 固定"领取后能还原逐任务超时"：入队时设置的超时
// 必须随 Claim 回到 Worker 手里。这条不能省——超时只存在于任务行上，进程重启或
// 任务被重新领取之后，除了从行里读回来别无他法；读不回来就只剩 Worker 的默认值，
// 长任务和短任务会被一视同仁地对待。没设置的任务回到 0，表示"用默认值"。
func TestClaimRestoresPerTaskTimeout(t *testing.T) {
	c, _ := newTestClientAndDB(t, gorangetask.ClientConfig{})
	ctx := context.Background()

	const (
		queue = "q-claim-timeout"
		want  = 12 * time.Second
	)

	withTimeout, err := c.Enqueue(ctx, gorangetask.Task{
		Queue:   queue,
		Type:    "slow.task",
		Payload: json.RawMessage(`{}`),
		Timeout: want,
	})
	if err != nil {
		t.Fatalf("Enqueue(with timeout): %v", err)
	}
	withoutTimeout, err := c.Enqueue(ctx, gorangetask.Task{
		Queue:   queue,
		Type:    "slow.task",
		Payload: json.RawMessage(`{}`),
	})
	if err != nil {
		t.Fatalf("Enqueue(without timeout): %v", err)
	}

	claimed, err := c.Claim(ctx, gorangetask.ClaimSpec{
		Queue:         queue,
		Limit:         2,
		Owner:         "claim-timeout-owner",
		LeaseDuration: 30 * time.Second,
	})
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if len(claimed) != 2 {
		t.Fatalf("Claim returned %d tasks, want 2", len(claimed))
	}

	byID := make(map[gorangetask.TaskID]gorangetask.ClaimedTask, len(claimed))
	for _, task := range claimed {
		byID[task.ID] = task
	}

	if got := byID[withTimeout].Timeout; got != want {
		t.Errorf("claimed timeout = %s, want %s (逐任务超时必须随 Claim 还原)", got, want)
	}
	if got := byID[withoutTimeout].Timeout; got != 0 {
		t.Errorf("claimed timeout = %s, want 0 for a task without a timeout", got)
	}
}

// --- 租约续期 ---------------------------------------------------------------

// TestRenewLeaseExtendsLeaseUntilFromDatabaseClock 固定续租的两件事：新的到期时间
// 从**数据库当前时间**起算，且除租约本身之外什么都不动。
//
// "从数据库当前时间起算"不是风格问题：若在旧的 lease_until 上累加，租约会随每次
// 续租越推越远，一次续租失败就能把任务锁住远超预期的时间。两种算法的结果相差整整
// 一个原始租约长度（30s），所以区间断言能把累加的实现明确排除掉。
func TestRenewLeaseExtendsLeaseUntilFromDatabaseClock(t *testing.T) {
	c, db := newTestClientAndDB(t, gorangetask.ClientConfig{})
	ctx := context.Background()

	const (
		queue     = "q-renew"
		extension = 45 * time.Second
	)

	id, err := c.Enqueue(ctx, gorangetask.Task{Queue: queue, Type: "t", Payload: json.RawMessage(`{}`)})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	task := claimOnce(t, c, queue, "renew-owner")
	if task.ID != id {
		t.Fatalf("claimed task %d, want %d", task.ID, id)
	}

	before := readTask(t, db, id)
	renewAt := dbNow(t, db)
	if err := c.RenewLease(ctx, task, extension); err != nil {
		t.Fatalf("RenewLease: %v", err)
	}
	after := readTask(t, db, id)

	if after.status != string(gorangetask.StatusRunning) {
		t.Errorf("status = %q after renewing, want running", after.status)
	}
	if after.leaseEpoch != before.leaseEpoch {
		t.Errorf("lease_epoch = %d after renewing, want %d unchanged", after.leaseEpoch, before.leaseEpoch)
	}
	if after.attempt != before.attempt {
		t.Errorf("attempt = %d after renewing, want %d unchanged", after.attempt, before.attempt)
	}
	if after.leaseOwner.String != before.leaseOwner.String {
		t.Errorf("lease_owner = %q after renewing, want %q unchanged", after.leaseOwner.String, before.leaseOwner.String)
	}
	if !after.leaseUntil.Valid {
		t.Fatal("lease_until became NULL after renewing")
	}
	if !after.leaseUntil.Time.After(before.leaseUntil.Time) {
		t.Errorf("lease_until = %v after renewing, want later than %v", after.leaseUntil.Time, before.leaseUntil.Time)
	}

	// 允许取时间与执行 UPDATE 之间有一点推进；累加的实现会落在 renewAt+75s 附近，
	// 远在这个区间之外。
	earliest, latest := renewAt.Add(extension-time.Second), renewAt.Add(extension+5*time.Second)
	if after.leaseUntil.Time.Before(earliest) || after.leaseUntil.Time.After(latest) {
		t.Errorf("lease_until = %v after renewing, want it within [%v, %v] (数据库当前时间 + %s)",
			after.leaseUntil.Time, earliest, latest, extension)
	}
	if after.updatedAt.Before(before.updatedAt) {
		t.Errorf("updated_at went backwards: %v -> %v", before.updatedAt, after.updatedAt)
	}
}

// TestRenewLeaseRejectsStaleCredentialsAndExpiredLease 把"续租也要过 fencing"钉在
// 真库上（需求文档 §4.4「过期 Worker 的更新必须影响 0 行」）。旧持有者若还能续租，
// 就能把自己"续"回有效状态，fencing 也就形同虚设。
//
// 每个子用例都用自己的任务：这类断言的失败方式恰恰是"什么都没发生"，
// 复用同一条任务会让前一个子用例留下的状态混进来。
func TestRenewLeaseRejectsStaleCredentialsAndExpiredLease(t *testing.T) {
	c, db := newTestClientAndDB(t, gorangetask.ClientConfig{})
	ctx := context.Background()

	const (
		queue = "q-renew-fencing"
		lease = 30 * time.Second
	)

	freshTask := func(t *testing.T, owner string) gorangetask.ClaimedTask {
		t.Helper()

		if _, err := c.Enqueue(ctx, gorangetask.Task{
			Queue: queue, Type: "t", Payload: json.RawMessage(`{}`),
		}); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
		tasks, err := c.Claim(ctx, gorangetask.ClaimSpec{
			Queue: queue, Limit: 1, Owner: owner, LeaseDuration: lease,
		})
		if err != nil {
			t.Fatalf("Claim: %v", err)
		}
		if len(tasks) != 1 {
			t.Fatalf("Claim returned %d tasks, want 1", len(tasks))
		}
		return tasks[0]
	}

	// assertRefused 断言这次续租被拒，且**没有**在库里留下任何痕迹。
	assertRefused := func(t *testing.T, stale gorangetask.ClaimedTask) {
		t.Helper()

		before := readTask(t, db, stale.ID)
		if err := c.RenewLease(ctx, stale, lease); !errors.Is(err, gorangetask.ErrLeaseLost) {
			t.Fatalf("RenewLease() error = %v, want ErrLeaseLost", err)
		}
		after := readTask(t, db, stale.ID)
		if !after.leaseUntil.Time.Equal(before.leaseUntil.Time) {
			t.Errorf("lease_until moved from %v to %v even though the renewal was refused",
				before.leaseUntil.Time, after.leaseUntil.Time)
		}
	}

	t.Run("stale owner", func(t *testing.T) {
		stale := freshTask(t, "renew-owner-a")
		stale.Owner = "renew-owner-b"
		assertRefused(t, stale)
	})

	t.Run("stale epoch", func(t *testing.T) {
		stale := freshTask(t, "renew-owner-a")
		stale.Epoch++
		assertRefused(t, stale)
	})

	t.Run("expired lease", func(t *testing.T) {
		task := freshTask(t, "renew-owner-a")
		expireLease(t, db, task.ID)
		assertRefused(t, task)
	})

	t.Run("task already finished", func(t *testing.T) {
		task := freshTask(t, "renew-owner-a")
		if err := c.Succeed(ctx, task); err != nil {
			t.Fatalf("Succeed: %v", err)
		}
		// 终态任务不能被续租"复活"。
		assertRefused(t, task)
	})

	t.Run("unknown task", func(t *testing.T) {
		// 不存在的任务没法用 readTask 比较，单独断言错误分类即可。
		ghost := freshTask(t, "renew-owner-a")
		ghost.ID = gorangetask.TaskID(1) << 40
		if err := c.RenewLease(ctx, ghost, lease); !errors.Is(err, gorangetask.ErrLeaseLost) {
			t.Fatalf("RenewLease(unknown task) error = %v, want ErrLeaseLost", err)
		}
	})
}

// TestWorkerRenewsLeaseSoHandlerCanOutliveOneLease 是租约续期的端到端证据：
// Handler 刻意跑得比一次 LeaseDuration 更长，仍然必须成功写回。
//
// 它同时证明 §3 留下的缺口已经闭合——没有续租时，租约会在 Handler 中途过期，
// 最后那次"成功"写回被 fencing 拒掉，任务永远停在 running（回收器属 §4 后续项，
// 目前没人会去捡它）。
//
// 用例不靠"睡够两倍租约"来构造前提，而是让 Handler 轮询数据库直到**亲眼看到**
// 租约被续到比初始更晚、且数据库时钟已越过初始到期时间，再返回。宿主负载高时
// 它只会变慢，不会把"续租没发生"误报成通过。
func TestWorkerRenewsLeaseSoHandlerCanOutliveOneLease(t *testing.T) {
	c, db := newTestClientAndDB(t, gorangetask.ClientConfig{})
	ctx := context.Background()

	const (
		queue = "q-long-handler"
		lease = 3 * time.Second
	)

	id, err := c.Enqueue(ctx, gorangetask.Task{
		Queue: queue, Type: "long.task", Payload: json.RawMessage(`{}`),
	})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// Handler 与 test goroutine 不同，这些值在 Run 返回后才读取（有 happens-before）。
	var (
		firstUntil  time.Time
		seenUntil   time.Time
		seenAt      time.Time
		handlerTime time.Duration
	)

	reg := gorangetask.NewRegistry()
	mustRegister(t, reg, "long.task", func(hctx context.Context, _ json.RawMessage) error {
		start := time.Now()
		for {
			var until, now time.Time
			if err := db.QueryRowContext(hctx,
				"SELECT lease_until, UTC_TIMESTAMP(6) FROM "+tasksTable+" WHERE id = ?",
				int64(id)).Scan(&until, &now); err != nil {
				return fmt.Errorf("read lease while running: %w", err)
			}
			until, now = until.UTC(), now.UTC()
			if firstUntil.IsZero() {
				firstUntil = until
			}
			// 两件事都要满足：租约被延到了比初始更晚的时刻，且数据库时钟已经越过
			// 初始到期时间——也就是"确实跑得比一次租约更长"。
			if until.After(firstUntil) && now.After(firstUntil) {
				seenUntil, seenAt, handlerTime = until, now, time.Since(start)
				return nil
			}
			select {
			case <-hctx.Done():
				handlerTime = time.Since(start)
				return fmt.Errorf("handler was cut short after %s, before the lease could be seen as renewed: %w",
					handlerTime, context.Cause(hctx))
			case <-time.After(50 * time.Millisecond):
			}
		}
	})

	cfg := workerConfig(queue)
	cfg.LeaseDuration = lease
	cfg.HeartbeatInterval = 500 * time.Millisecond
	// 上限放宽到远超租约：这条用例要证明的是"续租让长任务活下来"，
	// 不该被 Handler 超时提前打断。
	cfg.HandlerTimeout = 20 * time.Second

	w, err := gorangetask.NewWorker(c, reg, cfg)
	if err != nil {
		t.Fatalf("NewWorker: %v", err)
	}

	runCtx, cancelRun := context.WithCancel(ctx)
	// 用例中途失败时也要让 Worker 停下来，避免留下一个还在轮询的 goroutine。
	t.Cleanup(cancelRun)

	runDone := make(chan error, 1)
	go func() { runDone <- w.Run(runCtx) }()

	row := waitForStatus(t, db, id, string(gorangetask.StatusSucceeded), 25*time.Second)

	if row.attempt != 1 {
		t.Errorf("attempt = %d, want 1: 续租正常时不该有第二次领取", row.attempt)
	}
	if row.leaseEpoch != 1 {
		t.Errorf("lease_epoch = %d, want 1: 续租不该改变租约代数", row.leaseEpoch)
	}
	if row.leaseOwner.Valid || row.leaseUntil.Valid {
		t.Errorf("lease not cleared on success: owner=%v until=%v", row.leaseOwner, row.leaseUntil)
	}
	if !row.finishedAt.Valid {
		t.Error("finished_at is NULL after a successful task")
	}

	cancelRun()
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run() = %v, want nil", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run() did not return after shutdown")
	}

	if !seenUntil.After(firstUntil) {
		t.Errorf("observed lease_until %v was never extended beyond the first value %v", seenUntil, firstUntil)
	}
	if !seenAt.After(firstUntil) {
		t.Errorf("handler finished at %v without outliving the first lease (until %v)", seenAt, firstUntil)
	}
	t.Logf("handler ran %s; first lease_until=%v, extended lease_until=%v, db now at that point=%v",
		handlerTime, firstUntil, seenUntil, seenAt)
}

// --- 优雅停机 ---------------------------------------------------------------

// TestShutdownLeavesCancelledTaskRunningForTheReaper 把"停机不提前释放租约"钉在
// 真库上（需求文档 §4.6）。
//
// Worker 停机时正在执行的 Handler 会被取消。它的任务必须**留在 running**：
//   - 不能写回 pending——那会让另一个 Worker 在旧副作用可能仍在途时立刻重跑；
//   - 也不能写为终态——它并没有成功。
//
// 租约保持有效直到自然过期，之后交给回收器（§4 待实现）。用例顺带固定"停机不消耗
// 尝试次数、不写 last_error"：被取消的执行不是一次真正的失败。
func TestShutdownLeavesCancelledTaskRunningForTheReaper(t *testing.T) {
	c, db := newTestClientAndDB(t, gorangetask.ClientConfig{})
	ctx := context.Background()

	const queue = "q-shutdown"
	id, err := c.Enqueue(ctx, gorangetask.Task{
		Queue:   queue,
		Type:    "slow.task",
		Payload: json.RawMessage(`{}`),
	})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	started := make(chan struct{})
	reg := gorangetask.NewRegistry()
	mustRegister(t, reg, "slow.task", func(hctx context.Context, _ json.RawMessage) error {
		close(started)
		<-hctx.Done() // 只等停机取消，不做业务
		return hctx.Err()
	})

	w, err := gorangetask.NewWorker(c, reg, workerConfig(queue))
	if err != nil {
		t.Fatalf("NewWorker: %v", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	runDone := make(chan error, 1)
	go func() { runDone <- w.Run(runCtx) }()

	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("handler never started")
	}

	// 停机前先确认它真的已经被领走：status=running 且带租约。
	if before := readTask(t, db, id); before.status != string(gorangetask.StatusRunning) {
		t.Fatalf("status before shutdown = %q, want running", before.status)
	}

	cancel()
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run() = %v, want nil (the handler honoured the cancellation)", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run() did not return after shutdown")
	}

	row := readTask(t, db, id)
	if row.status != string(gorangetask.StatusRunning) {
		t.Errorf("status after shutdown = %q, want running: 停机不能把任务提前释放", row.status)
	}
	if !row.leaseOwner.Valid || !row.leaseUntil.Valid {
		t.Errorf("lease cleared after shutdown: owner=%v until=%v, want it kept for the reaper",
			row.leaseOwner, row.leaseUntil)
	}
	if row.finishedAt.Valid {
		t.Errorf("finished_at = %v after shutdown, want NULL", row.finishedAt.Time)
	}
	if row.attempt != 1 {
		t.Errorf("attempt = %d, want 1: 被取消的执行不该额外消耗一次尝试", row.attempt)
	}
	if row.leaseEpoch != 1 {
		t.Errorf("lease_epoch = %d, want 1: 停机不该改变租约代数", row.leaseEpoch)
	}
	if row.lastError.Valid {
		t.Errorf("last_error = %q after shutdown, want NULL: 被取消不等于失败", row.lastError.String)
	}
}

// --- 过期租约回收（ReapExpiredLeases） --------------------------------------

// 这个方法是"持有者已经消失"的唯一出口，所以用例的取向是把它**写回的每一个字段**
// 和它**不动的每一个字段**都钉在真库上：静态字段错了会让恢复出来的任务带着脏状态
// 重新执行，而"没被碰"的那一侧错了会让还有活人的租约被抢走、造成并行执行。

// enqueueForReap 造一条待回收场景用的任务。
func enqueueForReap(t *testing.T, c *Client, queue string, maxAttempts int) gorangetask.TaskID {
	t.Helper()

	id, err := c.Enqueue(context.Background(), gorangetask.Task{
		Queue:       queue,
		Type:        "email.send",
		Payload:     json.RawMessage(`{"to":"a@example.com"}`),
		MaxAttempts: maxAttempts,
	})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	return id
}

// leaseUntilLiteral 读出某行 lease_until 的 DATETIME(6) 字面量，取值方式与回收器
// 内部完全一致（DATE_FORMAT），供守卫用例原样带回去做等值比较。
func leaseUntilLiteral(t *testing.T, db *sql.DB, id gorangetask.TaskID) string {
	t.Helper()

	var lit string
	err := db.QueryRowContext(context.Background(),
		"SELECT DATE_FORMAT(lease_until, '%Y-%m-%d %H:%i:%s.%f') FROM "+tasksTable+" WHERE id = ?",
		int64(id)).Scan(&lit)
	if err != nil {
		t.Fatalf("read lease_until literal of task %d: %v", id, err)
	}
	return lit
}

// execReapPendingSQL 直接执行产品的"回收回 pending"语句，返回影响行数。
func execReapPendingSQL(t *testing.T, db *sql.DB, id gorangetask.TaskID, leaseLiteral string) int64 {
	t.Helper()

	res, err := db.ExecContext(context.Background(), reapToPendingSQL, reapPendingError, int64(id), leaseLiteral)
	if err != nil {
		t.Fatalf("exec reap-to-pending for task %d: %v", id, err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		t.Fatalf("rows affected for task %d: %v", id, err)
	}
	return affected
}

func countTasksWithStatus(t *testing.T, db *sql.DB, status string) int {
	t.Helper()

	var n int
	if err := db.QueryRowContext(context.Background(),
		"SELECT COUNT(*) FROM "+tasksTable+" WHERE status = ?", status).Scan(&n); err != nil {
		t.Fatalf("count tasks with status %q: %v", status, err)
	}
	return n
}

// TestReapReturnsExpiredTaskToPending 固定"次数未耗尽"这一支的完整结果。
//
// attempt 与 lease_epoch 都必须**不动**：这次领取已经用掉一次机会，回收只是给下
// 一次让路；epoch 属于"领取"这个动作，回收不是一次领取。用例最后真的把它重新领
// 出来，验证两个计数器是在领取时才各自 +1。
func TestReapReturnsExpiredTaskToPending(t *testing.T) {
	c, db := newTestClientAndDB(t, gorangetask.ClientConfig{})
	ctx := context.Background()
	const queue = "q-reap-pending"

	id := enqueueForReap(t, c, queue, 5)
	claimOnce(t, c, queue, "worker-a")
	expireLease(t, db, id)

	result, err := c.ReapExpiredLeases(ctx, 100)
	if err != nil {
		t.Fatalf("ReapExpiredLeases: %v", err)
	}
	if result.ToPending != 1 || result.ToDead != 0 {
		t.Fatalf("ReapResult = %+v, want {ToPending:1 ToDead:0}", result)
	}

	row := readTask(t, db, id)
	if row.status != "pending" {
		t.Errorf("status = %q, want pending", row.status)
	}
	if row.attempt != 1 {
		t.Errorf("attempt = %d, want 1: 回收不动尝试次数", row.attempt)
	}
	if row.leaseEpoch != 1 {
		t.Errorf("lease_epoch = %d, want 1: 回收不是一次领取，不推进代数", row.leaseEpoch)
	}
	if row.leaseOwner.Valid || row.leaseUntil.Valid {
		t.Errorf("lease = (owner %v, until %v), want both NULL", row.leaseOwner, row.leaseUntil)
	}
	if row.finishedAt.Valid {
		t.Errorf("finished_at = %s, want NULL: 回到队列不是终态", row.finishedAt.Time)
	}
	if row.lastError.String != reapPendingError {
		t.Errorf("last_error = %q, want %q", row.lastError.String, reapPendingError)
	}
	if row.runAt.After(dbNow(t, db)) {
		t.Errorf("run_at = %s is still in the future; a reaped task must be claimable at once", row.runAt)
	}

	// 回收出来的任务必须真的能被再领一次，而且那才是 attempt/epoch 各自 +1 的时刻。
	again := claimOnce(t, c, queue, "worker-b")
	if again.ID != id {
		t.Fatalf("re-claimed id = %d, want %d", again.ID, id)
	}
	if again.Attempt != 2 || again.Epoch != 2 {
		t.Errorf("re-claimed attempt/epoch = %d/%d, want 2/2", again.Attempt, again.Epoch)
	}
}

// TestReapMarksExhaustedTaskDead 固定"次数已耗尽"这一支：任务进死信、写完成时间，
// 且此后不再被领取（否则"次数上限"就形同虚设）。
func TestReapMarksExhaustedTaskDead(t *testing.T) {
	c, db := newTestClientAndDB(t, gorangetask.ClientConfig{})
	ctx := context.Background()
	const queue = "q-reap-dead"

	// MaxAttempts = 1：领取时 attempt 变成 1，已经用完了全部机会。
	id := enqueueForReap(t, c, queue, 1)
	claimOnce(t, c, queue, "worker-a")
	expireLease(t, db, id)

	result, err := c.ReapExpiredLeases(ctx, 100)
	if err != nil {
		t.Fatalf("ReapExpiredLeases: %v", err)
	}
	if result.ToPending != 0 || result.ToDead != 1 {
		t.Fatalf("ReapResult = %+v, want {ToPending:0 ToDead:1}", result)
	}

	row := readTask(t, db, id)
	if row.status != "dead" {
		t.Errorf("status = %q, want dead", row.status)
	}
	if row.attempt != 1 {
		t.Errorf("attempt = %d, want 1: 回收不动尝试次数", row.attempt)
	}
	if !row.finishedAt.Valid {
		t.Error("finished_at is NULL, want a timestamp: 死信是终态")
	}
	if row.leaseOwner.Valid || row.leaseUntil.Valid {
		t.Errorf("lease = (owner %v, until %v), want both NULL", row.leaseOwner, row.leaseUntil)
	}
	if row.lastError.String != reapDeadError {
		t.Errorf("last_error = %q, want %q", row.lastError.String, reapDeadError)
	}

	tasks, err := c.Claim(ctx, gorangetask.ClaimSpec{
		Queue: queue, Limit: 10, Owner: "worker-b", LeaseDuration: time.Minute,
	})
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if len(tasks) != 0 {
		t.Errorf("Claim returned %d task(s), want 0: 死信不该被重新领取", len(tasks))
	}
}

// TestReapLeavesUnexpiredLeasesAlone 是这套用例里最要紧的一条：回收器绝不能碰
// 租约还有效的行，否则它会把一个正在执行的任务抢走，直接造成并行执行。
//
// 用 updated_at 是否变化来判断"有没有被碰过"——只断言 status 不变是不够的，
// 一次"改了又改回去"的更新同样会破坏租约。
func TestReapLeavesUnexpiredLeasesAlone(t *testing.T) {
	c, db := newTestClientAndDB(t, gorangetask.ClientConfig{})
	ctx := context.Background()
	const queue = "q-reap-unexpired"

	id := enqueueForReap(t, c, queue, 5)
	claimed := claimOnce(t, c, queue, "worker-a") // 租约 30s，远未过期
	before := readTask(t, db, id)

	result, err := c.ReapExpiredLeases(ctx, 100)
	if err != nil {
		t.Fatalf("ReapExpiredLeases: %v", err)
	}
	if result.ToPending != 0 || result.ToDead != 0 {
		t.Fatalf("ReapResult = %+v, want zero: 租约还有效，回收器不该动手", result)
	}

	after := readTask(t, db, id)
	if after.status != "running" {
		t.Errorf("status = %q, want running", after.status)
	}
	if after.leaseOwner.String != claimed.Owner || uint64(after.leaseEpoch) != claimed.Epoch {
		t.Errorf("lease owner/epoch = %q/%d, want %q/%d",
			after.leaseOwner.String, after.leaseEpoch, claimed.Owner, claimed.Epoch)
	}
	if !after.leaseUntil.Valid || !after.leaseUntil.Time.Equal(before.leaseUntil.Time) {
		t.Errorf("lease_until = %v, want it unchanged (%v)", after.leaseUntil, before.leaseUntil.Time)
	}
	if !after.updatedAt.Equal(before.updatedAt) {
		t.Errorf("updated_at moved from %s to %s: 未过期的行被碰过了", before.updatedAt, after.updatedAt)
	}
}

// TestReapIgnoresNonRunningTasks 把 status 这一层挡住的事钉下来：五种状态各造一行，
// **每一行**的 lease_until 都改成过去——这样"没被回收"就不能用"租约还没到期"解释，
// 只能是 status 起的作用。
//
// 每种状态各占一个队列：领取的语义是"从该队列里挑 run_at 最早的一条"，同一个队列
// 里连着造几条会让后面的 claimOnce 领走前面那条，状态与 id 会整体错位。
func TestReapIgnoresNonRunningTasks(t *testing.T) {
	c, db := newTestClientAndDB(t, gorangetask.ClientConfig{})
	ctx := context.Background()
	const queue = "q-reap-status"

	pendingID := enqueueForReap(t, c, queue+"-pending", 5)

	runningID := enqueueForReap(t, c, queue+"-running", 5)
	claimOnce(t, c, queue+"-running", "worker-a")

	succeededID := enqueueForReap(t, c, queue+"-succeeded", 5)
	succeeded := claimOnce(t, c, queue+"-succeeded", "worker-a")
	if err := c.Succeed(ctx, succeeded); err != nil {
		t.Fatalf("Succeed: %v", err)
	}

	canceledID := enqueueForReap(t, c, queue+"-canceled", 5)
	if err := c.Cancel(ctx, canceledID); err != nil {
		t.Fatalf("Cancel: %v", err)
	}

	deadID := enqueueForReap(t, c, queue+"-dead", 1)
	dead := claimOnce(t, c, queue+"-dead", "worker-a")
	if err := c.MarkDead(ctx, dead, "handler gave up"); err != nil {
		t.Fatalf("MarkDead: %v", err)
	}

	type want struct {
		id     gorangetask.TaskID
		status string
	}
	rows := []want{
		{pendingID, "pending"},
		{runningID, "running"},
		{succeededID, "succeeded"},
		{canceledID, "canceled"},
		{deadID, "dead"},
	}

	// 把每一行的租约都改成过去，让 status 成为唯一的解释。
	updated := make(map[gorangetask.TaskID]time.Time, len(rows))
	for _, r := range rows {
		expireLease(t, db, r.id)
		updated[r.id] = readTask(t, db, r.id).updatedAt
	}

	// 只把 running 那行的租约恢复成"刚刚过期"是做不到的（它本来就已经过期），
	// 所以这里直接调用回收器：唯一该被回收的就是 running 那一行。
	result, err := c.ReapExpiredLeases(ctx, 100)
	if err != nil {
		t.Fatalf("ReapExpiredLeases: %v", err)
	}
	if result.ToPending != 1 || result.ToDead != 0 {
		t.Fatalf("ReapResult = %+v, want only the running row reaped", result)
	}

	for _, r := range rows {
		row := readTask(t, db, r.id)
		wantStatus := r.status
		if r.id == runningID {
			// 唯一被回收的那一行：过期 + 次数未耗尽 ⇒ 回到队列。
			wantStatus = "pending"
		}
		if row.status != wantStatus {
			t.Errorf("task %d status = %q, want %q", r.id, row.status, wantStatus)
		}
		if r.id == runningID {
			continue
		}
		if !row.updatedAt.Equal(updated[r.id]) {
			t.Errorf("task %d (%s) was touched by the reaper: updated_at %s -> %s",
				r.id, r.status, updated[r.id], row.updatedAt)
		}
	}
}

// TestReapGuardRejectsStaleLeaseValue 固定写回守卫的语义：用**读到的那个
// lease_until 原值**做等值比较，续租之后旧值不再命中。
//
// 为什么值得单独测：真实调用链里这一行被 FOR UPDATE 锁着，没人能在这中间改动它，
// 所以守卫是第二道防线。但它是"判错了就静默什么都不做"的地方——万一 DATE_FORMAT
// 出来的字面量与列值对不上，回收器会一声不响地永远回收不了任何任务，而所有
// "不该被回收的行没被回收"的断言照样全绿。这里用产品语句验证它"该命中时命中、
// 续租之后不命中"，把这个静默失败的可能性关掉。
func TestReapGuardRejectsStaleLeaseValue(t *testing.T) {
	c, db := newTestClientAndDB(t, gorangetask.ClientConfig{})
	ctx := context.Background()
	const queue = "q-reap-guard"

	id := enqueueForReap(t, c, queue, 5)
	claimed := claimOnce(t, c, queue, "worker-a")
	original := leaseUntilLiteral(t, db, id)

	// 续租：lease_until 换成未来。守卫拿的是续租前的旧值，必须一行都不改。
	if err := c.RenewLease(ctx, claimed, time.Hour); err != nil {
		t.Fatalf("RenewLease: %v", err)
	}
	renewed := leaseUntilLiteral(t, db, id)
	if renewed == original {
		t.Fatalf("lease_until did not change after a renewal (%s)", renewed)
	}
	if n := execReapPendingSQL(t, db, id, original); n != 0 {
		t.Errorf("rows affected with the pre-renewal lease value = %d, want 0", n)
	}
	row := readTask(t, db, id)
	if row.status != "running" || leaseUntilLiteral(t, db, id) != renewed {
		t.Errorf("a renewed lease was disturbed: status = %q, lease_until = %s",
			row.status, leaseUntilLiteral(t, db, id))
	}

	// 让租约过期：此时拿续租后的值做守卫就必须命中。
	expireLease(t, db, id)
	expired := leaseUntilLiteral(t, db, id)
	if n := execReapPendingSQL(t, db, id, renewed); n != 0 {
		t.Errorf("rows affected with the pre-expiry lease value = %d, want 0", n)
	}
	if n := execReapPendingSQL(t, db, id, expired); n != 1 {
		t.Fatalf("rows affected with the current (expired) lease value = %d, want 1", n)
	}

	row = readTask(t, db, id)
	if row.status != "pending" || row.lastError.String != reapPendingError {
		t.Errorf("after the guard matched: status = %q, last_error = %q; want pending / %q",
			row.status, row.lastError.String, reapPendingError)
	}
}

// TestReapRespectsLimit 固定 limit 是"最多处理几行"：剩下的留到下一轮。
func TestReapRespectsLimit(t *testing.T) {
	c, db := newTestClientAndDB(t, gorangetask.ClientConfig{})
	ctx := context.Background()
	const queue = "q-reap-limit"

	for i := 0; i < 3; i++ {
		id := enqueueForReap(t, c, queue, 5)
		claimOnce(t, c, queue, "worker-a")
		expireLease(t, db, id)
	}

	result, err := c.ReapExpiredLeases(ctx, 2)
	if err != nil {
		t.Fatalf("ReapExpiredLeases: %v", err)
	}
	if result.ToPending != 2 || result.ToDead != 0 {
		t.Fatalf("ReapResult = %+v, want {ToPending:2 ToDead:0}", result)
	}
	if n := countTasksWithStatus(t, db, "running"); n != 1 {
		t.Errorf("%d task(s) still running, want 1: limit 之外的行要留给下一轮", n)
	}

	// 下一轮把剩下的收掉，证明它是"留到下一轮"而不是"漏掉"。
	result, err = c.ReapExpiredLeases(ctx, 2)
	if err != nil {
		t.Fatalf("second ReapExpiredLeases: %v", err)
	}
	if result.ToPending != 1 {
		t.Errorf("second ReapResult = %+v, want {ToPending:1}", result)
	}
	if n := countTasksWithStatus(t, db, "running"); n != 0 {
		t.Errorf("%d task(s) still running after the second round", n)
	}
}

// explainPlan 执行 EXPLAIN，取出计划里实际选用的索引与 Extra 备注。
// 用于钉住"某条读必须走哪条索引"——这在本项目里是语义问题，不只是性能问题。
func explainPlan(t *testing.T, db *sql.DB, query string, args ...any) (key, extra string) {
	t.Helper()

	rows, err := db.QueryContext(context.Background(), "EXPLAIN "+query, args...)
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	defer func() { _ = rows.Close() }()

	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("explain columns: %v", err)
	}
	if !rows.Next() {
		t.Fatalf("explain returned no rows")
	}
	raw := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range raw {
		ptrs[i] = &raw[i]
	}
	if err := rows.Scan(ptrs...); err != nil {
		t.Fatalf("explain scan: %v", err)
	}
	for i, name := range cols {
		var s string
		switch v := raw[i].(type) {
		case []byte:
			s = string(v)
		case string:
			s = v
		}
		switch name {
		case "key":
			key = s
		case "Extra":
			extra = s
		}
	}
	return key, extra
}

// TestReapLockingReadStaysOnLeaseIndex 钉住回收器那条加锁读的**加锁范围**。
//
// 为什么值得单独一条用例：这条读要连带取回 attempt / max_attempts，而这两列不在
// ix_gotask_lease 里，所以它不是覆盖索引。实测（MySQL 8.0.46，12 条过期任务）优化器
// 会改选 `type=ALL, key=NULL, Using filesort`——全表扫描 + 排序。此时 FOR UPDATE 在
// 扫描过程中就把整个积压锁住，LIMIT 不再限制加锁范围，SKIP LOCKED 也失去意义：
//
//	tx1（LIMIT 3）返回 [1 2 3]，data_locks 显示它在 PRIMARY 上锁了全部 12 行；
//	tx2 执行同一条读返回 **0 行**，却同样把 ix_gotask_lease 上 12 条索引项全锁一遍。
//
// 后果是并发回收器互相把对方挡住，一轮下来只有一个能推进（12 条任务只回收 3 条），
// 而且积压越大锁得越多。这不是"慢一点"，是"多回收器等于没并发"。修复办法是在 SQL 里
// 显式 FORCE INDEX (ix_gotask_lease)，让计划稳定停在 lease 索引上（实测锁从 13 条降到 7 条）。
//
// 断言分两层，任一层被破坏都能立刻抓到：
//
//  1. 计划层：EXPLAIN 的 key 必须是 ix_gotask_lease。去掉 FORCE INDEX 后这一层先失败。
//  2. 行为层：tx1 持有 limit 行的锁且未提交时，tx2 执行同一条读仍应拿到 limit 行、
//     且与 tx1 不相交。去掉 FORCE INDEX 后 tx2 返回 0 行，这一层也失败。
//
// 刻意用两个手写事务而不是两个 Worker：要的是**确定性**复现"锁范围失控"，
// 不能把这条用例的成败压在 goroutine 调度上。
func TestReapLockingReadStaysOnLeaseIndex(t *testing.T) {
	c, db := newTestClientAndDB(t, gorangetask.ClientConfig{})
	ctx := context.Background()
	const (
		queue = "q-reap-lock-scope"
		tasks = 12
		limit = 3
	)

	for i := 0; i < tasks; i++ {
		id := enqueueForReap(t, c, queue, 5)
		claimOnce(t, c, queue, "worker-a")
		expireLease(t, db, id)
	}

	if key, extra := explainPlan(t, db, reapExpiredIDsSQL, limit); key != "ix_gotask_lease" {
		t.Errorf("回收器加锁读选用的索引 = %q (Extra=%q), want ix_gotask_lease: "+
			"计划一旦离开 lease 索引，FOR UPDATE 会把整个积压锁住", key, extra)
	}

	// 开事务、跑完加锁读后**不提交**：锁保留到用例结束（Cleanup 里 Rollback）。
	lockAndRead := func(t *testing.T, label string) []gorangetask.TaskID {
		t.Helper()

		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("%s begin: %v", label, err)
		}
		t.Cleanup(func() { _ = tx.Rollback() })

		rows, err := tx.QueryContext(ctx, reapExpiredIDsSQL, limit)
		if err != nil {
			t.Fatalf("%s query: %v", label, err)
		}
		defer func() { _ = rows.Close() }()

		var ids []gorangetask.TaskID
		for rows.Next() {
			var id int64
			var attempt, maxAttempts int
			var leaseUntil string
			if err := rows.Scan(&id, &attempt, &maxAttempts, &leaseUntil); err != nil {
				t.Fatalf("%s scan: %v", label, err)
			}
			ids = append(ids, gorangetask.TaskID(id))
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("%s read rows: %v", label, err)
		}
		return ids
	}

	first := lockAndRead(t, "tx1")
	if len(first) != limit {
		t.Fatalf("tx1 选中 %d 行, want %d", len(first), limit)
	}

	second := lockAndRead(t, "tx2")
	if len(second) != limit {
		t.Errorf("tx1 已锁 %v 时 tx2 只选中 %d 行, want %d: "+
			"加锁读越过了 LIMIT，把 tx1 之外的行也锁住了（SKIP LOCKED 失效）",
			first, len(second), limit)
	}

	seen := make(map[gorangetask.TaskID]bool, limit)
	for _, id := range first {
		seen[id] = true
	}
	for _, id := range second {
		if seen[id] {
			t.Errorf("task %d 被两个加锁读同时选中：SKIP LOCKED 失效", id)
		}
	}
}

// TestReapIsSafeWithConcurrentReapers 验证多个回收器同时跑不会重复处理同一行。
//
// 断言分两层，和并发领取那套一致：进程侧看**回收总数恰好等于任务数**；库里再独立
// 复核一次——全部落到 pending，且 lease_epoch 全为 1，说明没有任何一行被重新领取过。
//
// 每轮的 limit 刻意取 tasks/reapers：一个回收器一轮吃不下全部任务，凑够总数就必须
// 几个回收器都真的干了活。若把 limit 放大到 tasks，实测会变成一个回收器抢走全部 12
// 行、其余三个空手而归——那样断言照样全绿，却什么并发都没验证到（分布用 t.Logf
// 记下来，不做断言：它取决于 goroutine 调度）。
func TestReapIsSafeWithConcurrentReapers(t *testing.T) {
	c, db := newTestClientAndDB(t, gorangetask.ClientConfig{})
	ctx := context.Background()
	const (
		queue   = "q-reap-concurrent"
		tasks   = 12
		reapers = 4
	)

	for i := 0; i < tasks; i++ {
		id := enqueueForReap(t, c, queue, 5)
		claimOnce(t, c, queue, "worker-a")
		expireLease(t, db, id)
	}

	// barrier：让所有回收器在同一瞬间冲进去，把竞争窗口开到最大。
	start := make(chan struct{})
	results := make([]gorangetask.ReapResult, reapers)
	errs := make([]error, reapers)

	var wg sync.WaitGroup
	wg.Add(reapers)
	for i := 0; i < reapers; i++ {
		go func(i int) {
			defer wg.Done()
			<-start
			results[i], errs[i] = c.ReapExpiredLeases(ctx, tasks/reapers)
		}(i)
	}
	close(start)
	wg.Wait()

	total := 0
	for i, err := range errs {
		if err != nil {
			t.Errorf("reaper %d failed: %v", i, err)
			continue
		}
		t.Logf("reaper %d reaped %+v", i, results[i])
		total += results[i].ToPending + results[i].ToDead
	}
	if total != tasks {
		t.Errorf("reapers reaped %d row(s) in total, want %d: 同一行被处理了两次或漏掉了", total, tasks)
	}

	var pending, running int
	var maxEpoch int64
	if err := db.QueryRowContext(ctx,
		"SELECT SUM(status = 'pending'), SUM(status = 'running'), MAX(lease_epoch) FROM "+tasksTable).
		Scan(&pending, &running, &maxEpoch); err != nil {
		t.Fatalf("summarize table: %v", err)
	}
	if pending != tasks || running != 0 {
		t.Errorf("pending/running = %d/%d, want %d/0", pending, running, tasks)
	}
	if maxEpoch != 1 {
		t.Errorf("max lease_epoch = %d, want 1: 有任务被重新领取过，说明回收重复处理了", maxEpoch)
	}
}

// TestWorkerReapsTaskAbandonedByShutdown 是回收器的端到端验收：一个 Handler 不
// 理会取消的 Worker 在停机后把任务留在 running，租约自然过期，另一个 Worker 把它
// 捡回来跑完。这正是 §6「故障注入：claim 后退出」要的场景。
//
// 整个过程只靠产品路径，不手工改租约：Worker A 的续租看护随停机一起停掉，租约按
// 配置的 LeaseDuration 自然过期。
func TestWorkerReapsTaskAbandonedByShutdown(t *testing.T) {
	c, db := newTestClientAndDB(t, gorangetask.ClientConfig{})
	ctx := context.Background()
	const queue = "q-reap-e2e"

	id := enqueueForReap(t, c, queue, 5)

	// --- Worker A：Handler 无视 ctx，停机只能超时放弃，任务留在 running。 ---
	// 必须等 Handler **真的进去了**再停机：只等库里的 status 变成 running 是不够的，
	// claim 提交与 Handler 启动之间有个窗口，在那里停机会让 runOnce 按设计跳过整批
	// （剩余任务不启动 Handler），于是 Run 干净返回 nil——那是正确行为，却会让这条
	// 用例测不到它想测的东西。
	handlerEntered := make(chan struct{}, 1)
	releaseHandler := make(chan struct{})
	t.Cleanup(func() { close(releaseHandler) })

	regA := gorangetask.NewRegistry()
	mustRegister(t, regA, "email.send", func(context.Context, json.RawMessage) error {
		select {
		case handlerEntered <- struct{}{}:
		default:
		}
		<-releaseHandler
		return errors.New("released after worker A gave up")
	})

	cfgA := workerConfig(queue)
	cfgA.LeaseDuration = time.Second
	cfgA.HeartbeatInterval = 300 * time.Millisecond
	cfgA.ShutdownTimeout = 100 * time.Millisecond

	wA, err := gorangetask.NewWorker(c, regA, cfgA)
	if err != nil {
		t.Fatalf("NewWorker(A): %v", err)
	}

	ctxA, cancelA := context.WithCancel(ctx)
	aDone := make(chan error, 1)
	go func() { aDone <- wA.Run(ctxA) }()

	waitForStatus(t, db, id, "running", 5*time.Second)
	select {
	case <-handlerEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("worker A never started the handler")
	}
	cancelA()
	select {
	case err := <-aDone:
		if !errors.Is(err, gorangetask.ErrShutdownTimeout) {
			t.Fatalf("worker A Run() = %v, want ErrShutdownTimeout", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("worker A did not return")
	}
	// A 停了，但任务还在 running —— 这正是回收器存在的理由。
	if row := readTask(t, db, id); row.status != "running" {
		t.Fatalf("task status = %q after A gave up, want running", row.status)
	}

	// --- Worker B：同一个库上的另一个 Worker，带着回收器，Handler 正常返回。 ---
	regB := gorangetask.NewRegistry()
	mustRegister(t, regB, "email.send", func(_ context.Context, payload json.RawMessage) error {
		mustDecode(t, payload)
		return nil
	})

	cfgB := workerConfig(queue)
	cfgB.LeaseDuration = time.Second
	cfgB.HeartbeatInterval = 300 * time.Millisecond
	cfgB.ReapInterval = 20 * time.Millisecond

	wB, err := gorangetask.NewWorker(c, regB, cfgB)
	if err != nil {
		t.Fatalf("NewWorker(B): %v", err)
	}

	ctxB, cancelB := context.WithCancel(ctx)
	defer cancelB()
	bDone := make(chan error, 1)
	go func() { bDone <- wB.Run(ctxB) }()

	row := waitForStatus(t, db, id, "succeeded", 20*time.Second)
	if row.attempt != 2 {
		t.Errorf("attempt = %d, want 2: 回收后的那次重新领取应该再计一次尝试", row.attempt)
	}
	if row.leaseEpoch != 2 {
		t.Errorf("lease_epoch = %d, want 2: 两次领取 ⇒ 两个代数", row.leaseEpoch)
	}

	cancelB()
	select {
	case err := <-bDone:
		if err != nil {
			t.Fatalf("worker B Run() = %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("worker B did not return")
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
