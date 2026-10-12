package mysql

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	gorangetask "github.com/Tidehyc/gorangetask"
)

// Get 与 Cancel 的行为都用真 MySQL 验证。复用 client_integration_test.go 里的
// newTestClientAndDB / readTask / jsonEqual 等辅助函数（同包）。
// "已被 Worker 领取"与"已进入终态"这两种状态在本阶段还没有写回路径，
// 因此相关用例直接用 UPDATE 改库来构造前提，不假装走产品路径。

func TestGetReturnsFreshPendingTask(t *testing.T) {
	c, _ := newTestClientAndDB(t, gorangetask.ClientConfig{})
	ctx := context.Background()

	id, err := c.Enqueue(ctx, gorangetask.Task{
		Queue:   "orders",
		Type:    "send_receipt",
		Payload: []byte(`{"order_id":7}`),
	})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	info, err := c.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if info.ID != id {
		t.Errorf("ID = %d, want %d", info.ID, id)
	}
	if info.Queue != "orders" {
		t.Errorf("Queue = %q, want %q", info.Queue, "orders")
	}
	if info.Type != "send_receipt" {
		t.Errorf("Type = %q, want %q", info.Type, "send_receipt")
	}
	if info.Status != gorangetask.StatusPending {
		t.Errorf("Status = %q, want %q", info.Status, gorangetask.StatusPending)
	}
	if info.Attempt != 0 {
		t.Errorf("Attempt = %d, want 0", info.Attempt)
	}
	if info.MaxAttempts != gorangetask.DefaultMaxAttempts {
		t.Errorf("MaxAttempts = %d, want %d", info.MaxAttempts, gorangetask.DefaultMaxAttempts)
	}
	if info.IdempotencyKey != "" {
		t.Errorf("IdempotencyKey = %q, want empty", info.IdempotencyKey)
	}
	if info.LastError != "" {
		t.Errorf("LastError = %q, want empty", info.LastError)
	}
	if !info.FinishedAt.IsZero() {
		t.Errorf("FinishedAt = %s, want zero", info.FinishedAt)
	}

	// 时间字段必须带 UTC 位置，且与落库时一致。
	for name, got := range map[string]time.Time{
		"RunAt":     info.RunAt,
		"CreatedAt": info.CreatedAt,
		"UpdatedAt": info.UpdatedAt,
	} {
		if got.Location() != time.UTC {
			t.Errorf("%s location = %v, want UTC", name, got.Location())
		}
	}
	if !info.RunAt.Equal(info.CreatedAt) {
		t.Errorf("RunAt = %s, want to equal CreatedAt %s", info.RunAt, info.CreatedAt)
	}

	var got, want any
	if err := json.Unmarshal(info.Payload, &got); err != nil {
		t.Fatalf("Payload is not valid JSON: %v", err)
	}
	if err := json.Unmarshal([]byte(`{"order_id":7}`), &want); err != nil {
		t.Fatal(err)
	}
	if !jsonEqual(got, want) {
		t.Errorf("Payload = %s, want {\"order_id\": 7}", info.Payload)
	}
}

func TestGetReturnsDelayedTaskRunAtAndIdempotencyKey(t *testing.T) {
	c, _ := newTestClientAndDB(t, gorangetask.ClientConfig{})
	ctx := context.Background()

	wantRunAt := time.Date(2026, 10, 13, 3, 15, 0, 0, time.UTC)
	id, err := c.Enqueue(ctx, gorangetask.Task{
		Queue:          "reports",
		Type:           "build_report",
		Payload:        []byte(`{}`),
		RunAt:          wantRunAt,
		MaxAttempts:    3,
		IdempotencyKey: "report-2026-10-13",
	})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	info, err := c.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !info.RunAt.Equal(wantRunAt) {
		t.Errorf("RunAt = %s, want %s", info.RunAt, wantRunAt)
	}
	if info.MaxAttempts != 3 {
		t.Errorf("MaxAttempts = %d, want 3", info.MaxAttempts)
	}
	if info.IdempotencyKey != "report-2026-10-13" {
		t.Errorf("IdempotencyKey = %q, want %q", info.IdempotencyKey, "report-2026-10-13")
	}
}

// TestGetReturnsRetryContext 覆盖"任务已经失败重试过"的读取：attempt 与
// last_error 是排查问题时最先要看的两个字段。
func TestGetReturnsRetryContext(t *testing.T) {
	c, db := newTestClientAndDB(t, gorangetask.ClientConfig{})
	ctx := context.Background()

	id, err := c.Enqueue(ctx, gorangetask.Task{Queue: "orders", Type: "noop", Payload: []byte(`{}`)})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// 失败写回要等 Worker 阶段才实现，这里直接造出"已尝试两次并留下错误"的行。
	if _, err := db.ExecContext(ctx,
		"UPDATE "+tasksTable+" SET attempt = 2, last_error = ? WHERE id = ?",
		"charge_card: upstream timeout", int64(id)); err != nil {
		t.Fatalf("seed retry context: %v", err)
	}

	info, err := c.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if info.Attempt != 2 {
		t.Errorf("Attempt = %d, want 2", info.Attempt)
	}
	if info.LastError != "charge_card: upstream timeout" {
		t.Errorf("LastError = %q, want %q", info.LastError, "charge_card: upstream timeout")
	}
}

func TestGetNotFound(t *testing.T) {
	c, _ := newTestClientAndDB(t, gorangetask.ClientConfig{})

	_, err := c.Get(context.Background(), gorangetask.TaskID(999999))
	if !errors.Is(err, gorangetask.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestGetSurfacesDatabaseErrors(t *testing.T) {
	c, db := newTestClientAndDB(t, gorangetask.ClientConfig{})
	ctx := context.Background()

	if _, err := db.ExecContext(ctx, "DROP TABLE "+tasksTable); err != nil {
		t.Fatalf("drop tasks table: %v", err)
	}

	_, err := c.Get(ctx, gorangetask.TaskID(1))
	if err == nil {
		t.Fatal("want an error when the tasks table is missing")
	}
	if errors.Is(err, gorangetask.ErrNotFound) {
		t.Errorf("error %v must not be classified as ErrNotFound", err)
	}
	if !strings.Contains(err.Error(), "get task") {
		t.Errorf("error %v should carry the operation context", err)
	}
}

// TestCancelPendingTask 覆盖成功路径：状态变成 canceled，finished_at 与
// updated_at 由同一条语句写成、因此必须相等，且任务的内容字段不受影响。
func TestCancelPendingTask(t *testing.T) {
	c, db := newTestClientAndDB(t, gorangetask.ClientConfig{})
	ctx := context.Background()

	id, err := c.Enqueue(ctx, gorangetask.Task{
		Queue:   "orders",
		Type:    "send_receipt",
		Payload: []byte(`{"order_id":7}`),
	})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	before := readTask(t, db, id)

	if err := c.Cancel(ctx, id); err != nil {
		t.Fatalf("Cancel: %v", err)
	}

	info, err := c.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get after cancel: %v", err)
	}
	if info.Status != gorangetask.StatusCanceled {
		t.Errorf("Status = %q, want %q", info.Status, gorangetask.StatusCanceled)
	}
	if info.FinishedAt.IsZero() {
		t.Error("FinishedAt is zero, want a cancellation timestamp")
	}
	if !info.FinishedAt.Equal(info.UpdatedAt) {
		t.Errorf("FinishedAt = %s, want to equal UpdatedAt %s (same statement)", info.FinishedAt, info.UpdatedAt)
	}
	if info.UpdatedAt.Before(info.CreatedAt) {
		t.Errorf("UpdatedAt = %s, should not precede CreatedAt %s", info.UpdatedAt, info.CreatedAt)
	}

	after := readTask(t, db, id)
	if after.queueName != before.queueName || after.taskType != before.taskType || after.payload != before.payload {
		t.Errorf("cancel must not change task content: before=%+v after=%+v", before, after)
	}
	if after.attempt != before.attempt || after.maxAttempts != before.maxAttempts {
		t.Errorf("cancel must not change attempt counters: before=%+v after=%+v", before, after)
	}
	if after.leaseOwner.Valid || after.leaseUntil.Valid || after.leaseEpoch != 0 {
		t.Errorf("cancel must not touch lease columns: %+v", after)
	}
}

// TestCancelAllowsPastRunAt 固化"已到期但尚未领取仍可取消"。
func TestCancelAllowsPastRunAt(t *testing.T) {
	c, _ := newTestClientAndDB(t, gorangetask.ClientConfig{})
	ctx := context.Background()

	id, err := c.Enqueue(ctx, gorangetask.Task{
		Queue:   "orders",
		Type:    "noop",
		Payload: []byte(`{}`),
		RunAt:   time.Now().UTC().Add(-time.Hour), // 早就到期了，但仍未被领取
	})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := c.Cancel(ctx, id); err != nil {
		t.Fatalf("Cancel of an overdue pending task should succeed, got %v", err)
	}
}

// TestCancelRejectsNonPendingStates 逐个状态验证：running 与三种终态都返回
// ErrNotPending，且错误信息里带上实际状态便于排查。重复取消同一个任务同样报错。
func TestCancelRejectsNonPendingStates(t *testing.T) {
	states := []gorangetask.Status{
		gorangetask.StatusRunning,
		gorangetask.StatusSucceeded,
		gorangetask.StatusDead,
		gorangetask.StatusCanceled,
	}

	for _, state := range states {
		t.Run(string(state), func(t *testing.T) {
			c, db := newTestClientAndDB(t, gorangetask.ClientConfig{})
			ctx := context.Background()

			id, err := c.Enqueue(ctx, gorangetask.Task{Queue: "orders", Type: "noop", Payload: []byte(`{}`)})
			if err != nil {
				t.Fatalf("Enqueue: %v", err)
			}

			if state == gorangetask.StatusCanceled {
				// 走真实路径把自己取消掉，而不是直接改库。
				if err := c.Cancel(ctx, id); err != nil {
					t.Fatalf("first Cancel: %v", err)
				}
			} else {
				if _, err := db.ExecContext(ctx,
					"UPDATE "+tasksTable+" SET status = ? WHERE id = ?", string(state), int64(id)); err != nil {
					t.Fatalf("set status: %v", err)
				}
			}

			err = c.Cancel(ctx, id)
			if !errors.Is(err, gorangetask.ErrNotPending) {
				t.Fatalf("want ErrNotPending, got %v", err)
			}
			if !strings.Contains(err.Error(), string(state)) {
				t.Errorf("error %v should mention the actual status %q", err, state)
			}

			// 失败时不得改动任务。
			if got := readTask(t, db, id); got.status != string(state) {
				t.Errorf("status = %q, want it unchanged at %q", got.status, state)
			}
		})
	}
}

func TestCancelNotFound(t *testing.T) {
	c, _ := newTestClientAndDB(t, gorangetask.ClientConfig{})

	err := c.Cancel(context.Background(), gorangetask.TaskID(999999))
	if !errors.Is(err, gorangetask.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestCancelLeavesOtherTasksAlone(t *testing.T) {
	c, db := newTestClientAndDB(t, gorangetask.ClientConfig{})
	ctx := context.Background()

	target, err := c.Enqueue(ctx, gorangetask.Task{Queue: "orders", Type: "noop", Payload: []byte(`{}`)})
	if err != nil {
		t.Fatalf("Enqueue target: %v", err)
	}
	other, err := c.Enqueue(ctx, gorangetask.Task{Queue: "orders", Type: "noop", Payload: []byte(`{}`)})
	if err != nil {
		t.Fatalf("Enqueue other: %v", err)
	}

	if err := c.Cancel(ctx, target); err != nil {
		t.Fatalf("Cancel: %v", err)
	}

	if got := readTask(t, db, other); got.status != string(gorangetask.StatusPending) {
		t.Errorf("other task status = %q, want %q", got.status, gorangetask.StatusPending)
	}
}

func TestCancelSurfacesDatabaseErrors(t *testing.T) {
	c, db := newTestClientAndDB(t, gorangetask.ClientConfig{})
	ctx := context.Background()

	if _, err := db.ExecContext(ctx, "DROP TABLE "+tasksTable); err != nil {
		t.Fatalf("drop tasks table: %v", err)
	}

	err := c.Cancel(ctx, gorangetask.TaskID(1))
	if err == nil {
		t.Fatal("want an error when the tasks table is missing")
	}
	if errors.Is(err, gorangetask.ErrNotFound) || errors.Is(err, gorangetask.ErrNotPending) {
		t.Errorf("error %v must not be classified as a domain outcome", err)
	}
	if !strings.Contains(err.Error(), "cancel task") {
		t.Errorf("error %v should carry the operation context", err)
	}
}
