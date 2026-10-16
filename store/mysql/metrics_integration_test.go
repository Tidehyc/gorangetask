package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	gorangetask "github.com/Tidehyc/gorangetask"
)

// recordingMetrics 记录 store 侧的打点，其余事件交给 NopMetrics 兜底。
//
// 这里刻意用**嵌入 NopMetrics**的写法（而不是老老实实实现 12 个方法）：
// 它同时充当"NopMetrics 真的能被嵌入、只覆盖关心的事件"这条用法的实测，
// 而那正是文档推荐给接入方的方式。根包那份 recordingMetrics 全量实现，
// 因为它要断言"某事件没有被报告"。
type recordingMetrics struct {
	gorangetask.NopMetrics

	mu       sync.Mutex
	enqueued []string
	canceled int
}

func (m *recordingMetrics) TaskEnqueued(queue string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.enqueued = append(m.enqueued, queue)
}

func (m *recordingMetrics) TaskCanceled() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.canceled++
}

func (m *recordingMetrics) enqueuedQueues() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.enqueued...)
}

func (m *recordingMetrics) cancelCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.canceled
}

func TestClientReportsEnqueuedTasks(t *testing.T) {
	m := &recordingMetrics{}
	c, db := newTestClientAndDB(t, gorangetask.ClientConfig{Metrics: m, DefaultQueue: "jobs"})
	ctx := context.Background()

	// 显式队列。
	if _, err := c.Enqueue(ctx, gorangetask.Task{
		Queue: "critical", Type: "t", Payload: json.RawMessage(`{}`),
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// 不指定队列：记的必须是**归一化之后**的名字。若实现把调用方传的空串
	// 原样记出去，接入方的指标就会多出一个空标签值。
	if _, err := c.Enqueue(ctx, gorangetask.Task{Type: "t", Payload: json.RawMessage(`{}`)}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// 校验失败（空 payload）不该计数：那条任务根本没写进库。
	if _, err := c.Enqueue(ctx, gorangetask.Task{Type: "t"}); !errors.Is(err, gorangetask.ErrInvalidPayload) {
		t.Fatalf("Enqueue(no payload) error = %v, want ErrInvalidPayload", err)
	}

	if got, want := m.enqueuedQueues(), []string{"critical", "jobs"}; !slices.Equal(got, want) {
		t.Errorf("TaskEnqueued queues = %v, want %v", got, want)
	}

	// EnqueueTx 走的是同一条 insert，因此同样计数——**包括事务回滚的那一次**。
	// 这是已知局限：库无从知道调用方的事务最终是提交还是回滚（Tx 接口刻意不暴露
	// Commit）。写成显式断言而不是含糊带过，是为了让改这里的人知道它不是疏漏。
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	if _, err := c.EnqueueTx(ctx, tx, gorangetask.Task{
		Queue: "rolled-back", Type: "t", Payload: json.RawMessage(`{}`),
	}); err != nil {
		t.Fatalf("EnqueueTx: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	if got, want := m.enqueuedQueues(), []string{"critical", "jobs", "rolled-back"}; !slices.Equal(got, want) {
		t.Errorf("TaskEnqueued queues = %v, want %v (EnqueueTx 计入，含回滚)", got, want)
	}
}

func TestClientReportsCanceledTasks(t *testing.T) {
	m := &recordingMetrics{}
	c, _ := newTestClientAndDB(t, gorangetask.ClientConfig{Metrics: m})
	ctx := context.Background()

	id, err := c.Enqueue(ctx, gorangetask.Task{Type: "t", Payload: json.RawMessage(`{}`)})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := c.Cancel(ctx, id); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if got := m.cancelCount(); got != 1 {
		t.Errorf("TaskCanceled = %d, want 1", got)
	}

	// 重复取消（此时任务已是 canceled 终态）必须返回错误、且**不再计数**。
	// 这一条是判别者：把打点放在方法开头而不是"影响 N 行"之后，这里就会多记一次，
	// 于是"调用了几次 Cancel"被当成"成功取消了几条任务"。
	if err := c.Cancel(ctx, id); !errors.Is(err, gorangetask.ErrNotPending) {
		t.Fatalf("Cancel(again) error = %v, want ErrNotPending", err)
	}
	if got := m.cancelCount(); got != 1 {
		t.Errorf("TaskCanceled = %d after a failed cancel, want still 1", got)
	}

	// 取消一个不存在的任务同样不该计数。
	if err := c.Cancel(ctx, gorangetask.TaskID(999999)); !errors.Is(err, gorangetask.ErrNotFound) {
		t.Fatalf("Cancel(missing) error = %v, want ErrNotFound", err)
	}
	if got := m.cancelCount(); got != 1 {
		t.Errorf("TaskCanceled = %d after cancelling a missing task, want still 1", got)
	}
}

// setTaskAges 把任务的 created_at 与 run_at 分别推到指定的"多久之前"。
//
// 这是**测试自己构造时间**的唯一手段，也因此刻意走数据库时钟（TIMESTAMPADD +
// UTC_TIMESTAMP(6)），与产品侧的取时口径一致——把测试机器的时钟引进来，这条例
// 用就变成了在测两台机器的钟差。
func setTaskAges(t *testing.T, db *sql.DB, id gorangetask.TaskID, createdAgo, runAgo time.Duration) {
	t.Helper()
	_, err := db.ExecContext(context.Background(),
		`UPDATE `+tasksTable+`
		    SET created_at = TIMESTAMPADD(MICROSECOND, ?, UTC_TIMESTAMP(6)),
		        run_at     = TIMESTAMPADD(MICROSECOND, ?, UTC_TIMESTAMP(6))
		  WHERE id = ?`,
		-createdAgo.Microseconds(), -runAgo.Microseconds(), int64(id))
	if err != nil {
		t.Fatalf("backdate task %d: %v", id, err)
	}
}

// TestClaimReportsQueueWaitMeasuredByDatabaseClock 钉住排队等待时长的口径：
// 从 **GREATEST(created_at, run_at)** 到领取时刻，且由数据库时钟计算。
//
// 两条子用例分别针对两个容易写错的地方：取哪一列、以及用谁的时钟。
func TestClaimReportsQueueWaitMeasuredByDatabaseClock(t *testing.T) {
	c, db := newTestClientAndDB(t, gorangetask.ClientConfig{})
	ctx := context.Background()

	claim := func(t *testing.T, queue string) gorangetask.ClaimedTask {
		t.Helper()
		claimed, err := c.Claim(ctx, gorangetask.ClaimSpec{
			Queue: queue, Limit: 1, Owner: "test-owner", LeaseDuration: time.Minute,
		})
		if err != nil {
			t.Fatalf("Claim: %v", err)
		}
		if len(claimed) != 1 {
			t.Fatalf("claimed %d task(s), want 1", len(claimed))
		}
		return claimed[0]
	}

	t.Run("wait is measured from the moment the task became claimable", func(t *testing.T) {
		const queue = "wait-both"
		id, err := c.Enqueue(ctx, gorangetask.Task{Queue: queue, Type: "t", Payload: json.RawMessage(`{}`)})
		if err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
		// 两列一起推到 30 秒前：这是一个"30 秒前就该被领走、却一直没人领"的任务。
		setTaskAges(t, db, id, 30*time.Second, 30*time.Second)

		wait := claim(t, queue).Wait
		// 下界留 1 秒余量：UPDATE 与 SELECT 之间必然过去一点时间，Wait 只会更大。
		if wait < 29*time.Second || wait > 2*time.Minute {
			t.Errorf("Wait = %v, want about 30s", wait)
		}
	})

	t.Run("a future run_at is not counted as queueing", func(t *testing.T) {
		const queue = "wait-runat"
		id, err := c.Enqueue(ctx, gorangetask.Task{Queue: queue, Type: "t", Payload: json.RawMessage(`{}`)})
		if err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
		// created_at 在 30 秒前、run_at 在 10 秒前：这是一条被刻意延后 20 秒的任务，
		// 它真正的排队时间只有 10 秒。若实现取的是 created_at（少了 GREATEST），
		// 这里会读到约 30 秒。
		setTaskAges(t, db, id, 30*time.Second, 10*time.Second)

		wait := claim(t, queue).Wait
		if wait < 9*time.Second || wait > 20*time.Second {
			t.Errorf("Wait = %v, want about 10s（取 GREATEST(created_at, run_at)，而不是 created_at）", wait)
		}
	})

	t.Run("a freshly enqueued task has almost no queueing", func(t *testing.T) {
		const queue = "wait-fresh"
		if _, err := c.Enqueue(ctx, gorangetask.Task{Queue: queue, Type: "t", Payload: json.RawMessage(`{}`)}); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}

		wait := claim(t, queue).Wait
		// 上界要留得比 30 秒那两条紧得多：这才验得出"刚入队的任务几乎没有等待"，
		// 而不是一个恒大于 29 秒的常量。
		if wait < 0 || wait > 10*time.Second {
			t.Errorf("Wait = %v, want nearly 0 for a fresh task", wait)
		}
	})
}
