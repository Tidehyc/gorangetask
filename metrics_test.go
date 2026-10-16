package gorangetask

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// recordingMetrics 是 Metrics 的内存实现，记录每次调用的名字与参数，
// 让用例可以断言"哪些事件在什么时刻被报告过、带了什么值"。
//
// 它把接口的 12 个方法全部实现（而不是嵌入 NopMetrics 只覆盖几个），
// 因为断言"某事件**没有**被报告"与断言"被报告了"同等重要。
type recordingMetrics struct {
	mu     sync.Mutex
	events []metricEvent
	seq    int
}

type metricEvent struct {
	seq  int
	name string
	args []any
}

func (m *recordingMetrics) record(name string, args ...any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.seq++
	m.events = append(m.events, metricEvent{seq: m.seq, name: name, args: append([]any(nil), args...)})
}

func (m *recordingMetrics) TaskEnqueued(queue string) { m.record("TaskEnqueued", queue) }
func (m *recordingMetrics) TaskCanceled()             { m.record("TaskCanceled") }
func (m *recordingMetrics) TaskClaimed(queue, taskType string) {
	m.record("TaskClaimed", queue, taskType)
}
func (m *recordingMetrics) TaskFinished(queue, taskType string, outcome Outcome, d time.Duration) {
	m.record("TaskFinished", queue, taskType, outcome, d)
}
func (m *recordingMetrics) TaskWaited(queue, taskType string, d time.Duration) {
	m.record("TaskWaited", queue, taskType, d)
}
func (m *recordingMetrics) LeaseLost(queue, taskType string) {
	m.record("LeaseLost", queue, taskType)
}
func (m *recordingMetrics) HandlerPanicked(queue, taskType string) {
	m.record("HandlerPanicked", queue, taskType)
}
func (m *recordingMetrics) HandlerTimedOut(queue, taskType string) {
	m.record("HandlerTimedOut", queue, taskType)
}
func (m *recordingMetrics) TasksReaped(toPending, toDead int) {
	m.record("TasksReaped", toPending, toDead)
}
func (m *recordingMetrics) TasksCleaned(status Status, deleted int64) {
	m.record("TasksCleaned", status, deleted)
}
func (m *recordingMetrics) ClaimLatency(d time.Duration) { m.record("ClaimLatency", d) }
func (m *recordingMetrics) ConcurrencyInUse(n int)       { m.record("ConcurrencyInUse", n) }

// snapshot 返回当前所有事件的副本。
func (m *recordingMetrics) snapshot() []metricEvent {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]metricEvent(nil), m.events...)
}

// count 返回名为 name 的事件数。
func (m *recordingMetrics) count(name string) int {
	n := 0
	for _, e := range m.snapshot() {
		if e.name == name {
			n++
		}
	}
	return n
}

// only 返回名为 name 的唯一事件，并断言它恰好出现一次。
func (m *recordingMetrics) only(t *testing.T, name string) metricEvent {
	t.Helper()
	var found []metricEvent
	for _, e := range m.snapshot() {
		if e.name == name {
			found = append(found, e)
		}
	}
	if len(found) != 1 {
		t.Fatalf("event %q occurred %d time(s), want exactly 1; all events: %v", name, len(found), m.names())
	}
	return found[0]
}

// never 断言名为 name 的事件一次都没发生。
func (m *recordingMetrics) never(t *testing.T, name string) {
	t.Helper()
	if n := m.count(name); n != 0 {
		t.Fatalf("event %q occurred %d time(s), want 0; all events: %v", name, n, m.names())
	}
}

// first 返回名为 name 的第一个事件（没有则失败）。用于"会重复发生"的事件：
// 回收器与清理器按毫秒级间隔跑，断言"恰好一次"会随调度抖动而假失败。
func (m *recordingMetrics) first(t *testing.T, name string) metricEvent {
	t.Helper()
	for _, e := range m.snapshot() {
		if e.name == name {
			return e
		}
	}
	t.Fatalf("event %q never occurred; all events: %v", name, m.names())
	return metricEvent{}
}

// names 返回事件名的序列，失败信息里用它给出全貌。
func (m *recordingMetrics) names() []string {
	events := m.snapshot()
	names := make([]string, 0, len(events))
	for _, e := range events {
		names = append(names, e.name)
	}
	return names
}

// indexOf 返回第一个"名字匹配且第 idx 个参数等于 want"的事件的序号；没有则返回 -1。
func (m *recordingMetrics) indexOf(name string, idx int, want any) int {
	for _, e := range m.snapshot() {
		if e.name == name && idx < len(e.args) && e.args[idx] == want {
			return e.seq
		}
	}
	return -1
}

// workerWithMetrics 造一个带 recordingMetrics 的 Worker 与它的记录器。
func workerWithMetrics(t *testing.T, store *fakeStore, reg *Registry, mutate func(*Config)) (*Worker, *recordingMetrics) {
	t.Helper()

	m := &recordingMetrics{}
	cfg := fastConfig()
	cfg.Metrics = m
	if mutate != nil {
		mutate(&cfg)
	}
	w, err := NewWorker(store, reg, cfg)
	if err != nil {
		t.Fatalf("NewWorker() unexpected error: %v", err)
	}
	return w, m
}

func TestMetricsRecordsClaimAndSucceededLifecycle(t *testing.T) {
	task := claimedTask(7, "email.send", 1, 5)
	task.Queue = "critical"
	task.Wait = 42 * time.Millisecond
	store := &fakeStore{batches: [][]ClaimedTask{{task}}}
	reg := newRegistryWith(t, "email.send")
	w, m := workerWithMetrics(t, store, reg, nil)

	if _, err := w.runOnce(context.Background()); err != nil {
		t.Fatalf("runOnce() unexpected error: %v", err)
	}

	// 领取：一次往返耗时、一条领取、一条排队延迟。
	if lat := m.only(t, "ClaimLatency").args[0]; lat.(time.Duration) < 0 {
		t.Errorf("ClaimLatency = %v, want >= 0", lat)
	}
	if got := m.only(t, "TaskClaimed").args; got[0] != "critical" || got[1] != "email.send" {
		t.Errorf("TaskClaimed args = %v, want [critical email.send]", got)
	}
	if got := m.only(t, "TaskWaited").args; got[2] != 42*time.Millisecond {
		t.Errorf("TaskWaited duration = %v, want 42ms", got[2])
	}

	// 结果：一条 TaskFinished，去向是 succeeded，带 Handler 耗时。
	fin := m.only(t, "TaskFinished")
	if fin.args[0] != "critical" || fin.args[1] != "email.send" || fin.args[2] != OutcomeSucceeded {
		t.Errorf("TaskFinished args = %v, want [critical email.send succeeded ...]", fin.args)
	}
	if d, ok := fin.args[3].(time.Duration); !ok || d < 0 {
		t.Errorf("TaskFinished duration = %#v, want a non-negative duration", fin.args[3])
	}

	// 并发水位必须"执行期间抬起、结束后落回"，且两个边沿分别落在结果事件的两侧
	// ——只断言"见过 1 和 0"抓不到"忘记减去"这类实现错误（那样只有 1、没有 0）。
	up := m.indexOf("ConcurrencyInUse", 0, 1)
	down := m.indexOf("ConcurrencyInUse", 0, 0)
	if up == -1 {
		t.Fatalf("ConcurrencyInUse never reported 1; all events: %v", m.names())
	}
	if down == -1 {
		t.Fatalf("ConcurrencyInUse never reported 0 (the decrement is missing?); all events: %v", m.names())
	}
	if !(up < fin.seq && fin.seq < down) {
		t.Errorf("ConcurrencyInUse edges (%d, %d) do not straddle TaskFinished (%d)", up, down, fin.seq)
	}

	m.never(t, "LeaseLost")
	m.never(t, "HandlerPanicked")
	m.never(t, "HandlerTimedOut")
}

func TestMetricsRecordsFailureOutcomes(t *testing.T) {
	boom := func(context.Context, json.RawMessage) error { return errors.New("boom") }

	cases := []struct {
		name    string
		attempt int
		want    Outcome
	}{
		{"retryable failure", 1, OutcomeRetried},
		{"exhausted attempts", 3, OutcomeDead},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			task := claimedTask(11, "t", c.attempt, 3)
			store := &fakeStore{batches: [][]ClaimedTask{{task}}}
			reg := NewRegistry()
			if err := reg.Register("t", boom); err != nil {
				t.Fatalf("Register: %v", err)
			}
			w, m := workerWithMetrics(t, store, reg, nil)

			if _, err := w.runOnce(context.Background()); err != nil {
				t.Fatalf("runOnce() unexpected error: %v", err)
			}

			if got := m.only(t, "TaskFinished").args[2]; got != c.want {
				t.Errorf("outcome = %v, want %v", got, c.want)
			}
		})
	}
}

// TestMetricsPanicIsCountedAsItsOwnEvent 钉住"panic 既算一次普通失败、又有
// 自己的诊断计数"。只断言 TaskFinished 出现分辨不出它是 panic 还是普通错误。
func TestMetricsPanicIsCountedAsItsOwnEvent(t *testing.T) {
	task := claimedTask(13, "t", 1, 3)
	store := &fakeStore{batches: [][]ClaimedTask{{task}}}
	reg := NewRegistry()
	if err := reg.Register("t", func(context.Context, json.RawMessage) error {
		panic("kaboom")
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	w, m := workerWithMetrics(t, store, reg, nil)

	if _, err := w.runOnce(context.Background()); err != nil {
		t.Fatalf("runOnce() unexpected error: %v", err)
	}

	if got := m.only(t, "HandlerPanicked").args; got[0] != DefaultQueue || got[1] != "t" {
		t.Errorf("HandlerPanicked args = %v, want [%s t]", got, DefaultQueue)
	}
	// panic 仍是一次普通失败：它会照常计进 TaskFinished（这里是可重试）。
	if got := m.only(t, "TaskFinished").args[2]; got != OutcomeRetried {
		t.Errorf("outcome = %v, want %v（panic 应走普通失败路径）", got, OutcomeRetried)
	}
}

// TestMetricsTimeoutIsCountedAsItsOwnEvent 同上，针对执行超时。
func TestMetricsTimeoutIsCountedAsItsOwnEvent(t *testing.T) {
	task := claimedTask(14, "t", 1, 3)
	task.Timeout = 20 * time.Millisecond
	store := &fakeStore{batches: [][]ClaimedTask{{task}}}
	reg := NewRegistry()
	if err := reg.Register("t", func(hctx context.Context, _ json.RawMessage) error {
		// 不理会 ctx 的 Handler 会让停机等待满 ShutdownTimeout，所以这里正常
		// 观察取消：超时到了就返回 ctx 的错误。
		<-hctx.Done()
		return hctx.Err()
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	w, m := workerWithMetrics(t, store, reg, nil)

	if _, err := w.runOnce(context.Background()); err != nil {
		t.Fatalf("runOnce() unexpected error: %v", err)
	}

	if got := m.only(t, "HandlerTimedOut").args; got[0] != DefaultQueue || got[1] != "t" {
		t.Errorf("HandlerTimedOut args = %v, want [%s t]", got, DefaultQueue)
	}
	// 超时同样只是一次普通失败。
	if got := m.only(t, "TaskFinished").args[2]; got != OutcomeRetried {
		t.Errorf("outcome = %v, want %v", got, OutcomeRetried)
	}
	// 超时不是 panic，两个计数必须分开——否则"Handler 有 bug"与"超时太短"就
	// 混成了一个信号。
	m.never(t, "HandlerPanicked")
}

// TestMetricsLeaseLostIsCountedOnce 覆盖失租的两个入口，并钉住"一次失租只计一次"。
func TestMetricsLeaseLostIsCountedOnce(t *testing.T) {
	t.Run("renewal rejected", func(t *testing.T) {
		task := claimedTask(15, "t", 1, 3)
		store := &fakeStore{batches: [][]ClaimedTask{{task}}}
		store.renewFn = func(context.Context, int, ClaimedTask) error { return ErrLeaseLost }

		reg := NewRegistry()
		if err := reg.Register("t", func(hctx context.Context, _ json.RawMessage) error {
			<-hctx.Done() // 等续租被拒后取消本任务
			return hctx.Err()
		}); err != nil {
			t.Fatalf("Register: %v", err)
		}
		w, m := workerWithMetrics(t, store, reg, func(c *Config) {
			c.LeaseDuration = time.Hour
			c.HeartbeatInterval = time.Millisecond
		})

		if _, err := w.runOnce(context.Background()); err != nil {
			t.Fatalf("runOnce() unexpected error: %v", err)
		}

		if got := m.only(t, "LeaseLost").args; got[0] != DefaultQueue || got[1] != "t" {
			t.Errorf("LeaseLost args = %v, want [%s t]", got, DefaultQueue)
		}
		// 失租后本次结果不写回 ⇒ 任务没有去向，不该计 TaskFinished。
		m.never(t, "TaskFinished")
	})

	t.Run("write-back rejected", func(t *testing.T) {
		task := claimedTask(16, "t", 1, 3)
		store := &fakeStore{batches: [][]ClaimedTask{{task}}, succeedErr: ErrLeaseLost}
		reg := newRegistryWith(t, "t")
		w, m := workerWithMetrics(t, store, reg, nil)

		if _, err := w.runOnce(context.Background()); err != nil {
			t.Fatalf("runOnce() unexpected error: %v", err)
		}

		// 写回被 fencing 拒掉：结果没有落地，所以有失租、没有 TaskFinished。
		m.only(t, "LeaseLost")
		m.never(t, "TaskFinished")
	})
}

func TestMetricsReapAndCleanupAreReported(t *testing.T) {
	t.Run("reap", func(t *testing.T) {
		store := &fakeStore{reapResult: &ReapResult{ToPending: 2, ToDead: 1}}
		reg := newRegistryWith(t, "t")
		w, m := workerWithMetrics(t, store, reg, func(c *Config) {
			c.ReapInterval = time.Millisecond
		})

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			defer close(done)
			_ = w.Run(ctx)
		}()
		waitUntil(t, 5*time.Second, "the reap to be reported", func() bool {
			return m.count("TasksReaped") > 0
		})
		cancel()
		<-done

		got := m.first(t, "TasksReaped").args
		if got[0] != 2 || got[1] != 1 {
			t.Errorf("TasksReaped args = %v, want [2 1]", got)
		}
	})

	t.Run("cleanup", func(t *testing.T) {
		// 只让 succeeded 有东西可删，其余两个终态返回 0：断言"按状态分别报告"。
		store := &fakeStore{cleanupDeleted: func(spec CleanupSpec, _ int) int64 {
			if spec.Status == StatusSucceeded {
				return 5
			}
			return 0
		}}
		reg := newRegistryWith(t, "t")
		w, m := workerWithMetrics(t, store, reg, func(c *Config) {
			c.CleanupInterval = time.Millisecond
		})

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			defer close(done)
			_ = w.Run(ctx)
		}()
		waitUntil(t, 5*time.Second, "the cleanup to be reported", func() bool {
			return m.count("TasksCleaned") > 0
		})
		cancel()
		<-done

		got := m.first(t, "TasksCleaned").args
		if got[0] != StatusSucceeded || got[1] != int64(5) {
			t.Errorf("TasksCleaned args = %v, want [succeeded 5]", got)
		}
	})
}

// TestNewWorkerReplacesNilMetrics 钉住 nil 指标被换成空实现——库内部遍布打点，
// 靠每个调用点判空是不可靠的（漏一处就是 nil 解引用 panic）。
func TestNewWorkerReplacesNilMetrics(t *testing.T) {
	store := &fakeStore{batches: [][]ClaimedTask{{claimedTask(17, "t", 1, 3)}}}
	reg := newRegistryWith(t, "t")
	w, err := NewWorker(store, reg, fastConfig()) // fastConfig 不设 Metrics
	if err != nil {
		t.Fatalf("NewWorker() unexpected error: %v", err)
	}
	if _, ok := w.cfg.Metrics.(NopMetrics); !ok {
		t.Fatalf("cfg.Metrics = %#v, want NopMetrics", w.cfg.Metrics)
	}

	// 空实现下整条路径照常工作、不 panic。
	if _, err := w.runOnce(context.Background()); err != nil {
		t.Fatalf("runOnce() unexpected error: %v", err)
	}
	if got := len(store.succeededIDs()); got != 1 {
		t.Errorf("succeeded %d task(s), want 1", got)
	}
}

// TestMetricsNeverCarryTaskID 是对接口设计的回归守卫：需求 §8 禁止把 task ID
// 放进指标标签。指标事件里出现 uint64 形态的任务 ID 就意味着有人加了高基数维度。
func TestMetricsNeverCarryTaskID(t *testing.T) {
	task := claimedTask(987654, "t", 1, 3)
	task.Wait = time.Millisecond
	store := &fakeStore{batches: [][]ClaimedTask{{task}}}
	reg := newRegistryWith(t, "t")
	w, m := workerWithMetrics(t, store, reg, nil)

	if _, err := w.runOnce(context.Background()); err != nil {
		t.Fatalf("runOnce() unexpected error: %v", err)
	}

	const taskID = uint64(987654)
	for _, e := range m.snapshot() {
		for i, v := range e.args {
			// TaskID 是 uint64 的命名类型，不匹配 `v.(uint64)`，两个都要查。
			switch value := v.(type) {
			case uint64:
				if value == taskID {
					t.Errorf("metric event %q carries the task ID at args[%d]: %v", e.name, i, v)
				}
			case TaskID:
				if uint64(value) == taskID {
					t.Errorf("metric event %q carries the task ID at args[%d]: %v", e.name, i, v)
				}
			case string:
				if strings.Contains(value, fmt.Sprint(taskID)) {
					t.Errorf("metric event %q carries the task ID at args[%d]: %q", e.name, i, value)
				}
			}
		}
	}
}
