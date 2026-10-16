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

// recordingLogger 是 Logger 的内存实现，让用例可以断言"库在哪些时刻、用哪些
// 级别、带哪些字段记了日志"。它必须并发安全：Run 会并发调用。
type recordingLogger struct {
	mu      sync.Mutex
	records []logRecord
}

type logRecord struct {
	level Level
	msg   string
	kv    []any
}

func (l *recordingLogger) Log(_ context.Context, level Level, msg string, kv ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	// 拷一份 kv：库那边可能复用底层数组，直接存会让断言看到被改写后的内容。
	l.records = append(l.records, logRecord{level: level, msg: msg, kv: append([]any(nil), kv...)})
}

// all 返回当前所有记录的快照。
func (l *recordingLogger) all() []logRecord {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]logRecord(nil), l.records...)
}

// messages 返回所有记录的消息文本，便于断言"某条日志没有出现"。
func (l *recordingLogger) messages() []string {
	records := l.all() // 先取快照：len(l.records) 在锁外读会与并发 Log 打架
	msgs := make([]string, 0, len(records))
	for _, r := range records {
		msgs = append(msgs, r.msg)
	}
	return msgs
}

// find 返回第一条消息等于 msg 的记录。
func (l *recordingLogger) find(t *testing.T, msg string) logRecord {
	t.Helper()
	for _, r := range l.all() {
		if r.msg == msg {
			return r
		}
	}
	t.Fatalf("no log record with message %q; got %v", msg, l.messages())
	return logRecord{}
}

// fields 把一条记录的 kv 展平成 map，并在 kv 不成对或键不是字符串时直接失败
// ——库承诺 kv 是交替的键值对，契约破了就该在这里炸，而不是让下面的断言
// 因为"取不到键"而静默通过。
func (r logRecord) fields(t *testing.T) map[string]any {
	t.Helper()
	if len(r.kv)%2 != 0 {
		t.Fatalf("log record %q has an odd number of kv items: %v", r.msg, r.kv)
	}
	m := make(map[string]any, len(r.kv)/2)
	for i := 0; i < len(r.kv); i += 2 {
		key, ok := r.kv[i].(string)
		if !ok {
			t.Fatalf("log record %q has a non-string key at position %d: %#v", r.msg, i, r.kv[i])
		}
		m[key] = r.kv[i+1]
	}
	return m
}

// workerWithLogger 造一个带 recordingLogger 的 Worker 与它的日志接收器。
func workerWithLogger(t *testing.T, store *fakeStore, reg *Registry, mutate func(*Config)) (*Worker, *recordingLogger) {
	t.Helper()

	logger := &recordingLogger{}
	cfg := fastConfig()
	cfg.Logger = logger
	if mutate != nil {
		mutate(&cfg)
	}
	w, err := NewWorker(store, reg, cfg)
	if err != nil {
		t.Fatalf("NewWorker() unexpected error: %v", err)
	}
	return w, logger
}

func TestLevelString(t *testing.T) {
	cases := []struct {
		level Level
		want  string
	}{
		{LevelDebug, "debug"},
		{LevelInfo, "info"},
		{LevelWarn, "warn"},
		{LevelError, "error"},
		// 未定义的取值不能 panic：日志实现拿到意外的级别不该把 Worker 打挂。
		{Level(42), "unknown"},
		{Level(-1), "unknown"},
	}
	for _, c := range cases {
		if got := c.level.String(); got != c.want {
			t.Errorf("Level(%d).String() = %q, want %q", c.level, got, c.want)
		}
	}
}

func TestWorkerLogsSucceededTaskWithRequiredFields(t *testing.T) {
	task := claimedTask(7, "email.send", 2, 5)
	task.Queue = "critical"
	store := &fakeStore{batches: [][]ClaimedTask{{task}}}
	reg := newRegistryWith(t, "email.send")
	w, logger := workerWithLogger(t, store, reg, nil)

	if _, err := w.runOnce(context.Background()); err != nil {
		t.Fatalf("runOnce() unexpected error: %v", err)
	}

	rec := logger.find(t, "task succeeded")
	if rec.level != LevelInfo {
		t.Errorf("level = %v, want %v", rec.level, LevelInfo)
	}
	fields := rec.fields(t)

	// 需求 §8 点名的字段一个都不能少。
	for key, want := range map[string]any{
		"task_id":      uint64(7),
		"task_type":    "email.send",
		"queue":        "critical",
		"attempt":      2,
		"max_attempts": 5,
		"lease_epoch":  uint64(1),
		"worker_id":    w.owner,
	} {
		got, ok := fields[key]
		if !ok {
			t.Errorf("log field %q is missing; got %v", key, fields)
			continue
		}
		if got != want {
			t.Errorf("log field %q = %#v, want %#v", key, got, want)
		}
	}

	// duration 必须存在且类型正确。"取到了但类型不对"与"根本没取到"要分开报，
	// 否则一个被写成字符串的耗时也能通过。
	d, ok := fields["duration"]
	if !ok {
		t.Errorf("log field %q is missing; got %v", "duration", fields)
	} else if elapsed, isDuration := d.(time.Duration); !isDuration {
		t.Errorf("log field %q = %#v, want a time.Duration", "duration", d)
	} else if elapsed < 0 {
		t.Errorf("log field %q = %v, want >= 0", "duration", elapsed)
	}
}

func TestWorkerLogsRetryAndDeadOutcomes(t *testing.T) {
	t.Run("retryable failure is logged as a scheduled retry", func(t *testing.T) {
		task := claimedTask(11, "t", 1, 5)
		store := &fakeStore{batches: [][]ClaimedTask{{task}}}
		reg := NewRegistry()
		if err := reg.Register("t", func(context.Context, json.RawMessage) error {
			return errors.New("downstream unavailable")
		}); err != nil {
			t.Fatalf("Register: %v", err)
		}
		w, logger := workerWithLogger(t, store, reg, nil)

		if _, err := w.runOnce(context.Background()); err != nil {
			t.Fatalf("runOnce() unexpected error: %v", err)
		}

		rec := logger.find(t, "task failed; retry scheduled")
		if rec.level != LevelWarn {
			t.Errorf("level = %v, want %v", rec.level, LevelWarn)
		}
		fields := rec.fields(t)
		if fields["task_id"] != uint64(11) {
			t.Errorf("task_id = %#v, want 11", fields["task_id"])
		}
		// 退避延迟是运维真正想看的那个数（"还要等多久才会再试"）。
		delay, ok := fields["delay"].(time.Duration)
		if !ok {
			t.Fatalf("delay = %#v, want a time.Duration", fields["delay"])
		}
		if delay <= 0 {
			t.Errorf("delay = %v, want > 0", delay)
		}
		if errText, ok := fields["error"].(error); !ok || !strings.Contains(errText.Error(), "downstream unavailable") {
			t.Errorf("error = %#v, want the handler error", fields["error"])
		}

		// 这条路径不该同时出现"死信"日志——两种去向必须互斥。
		for _, msg := range logger.messages() {
			if msg == "task marked dead" {
				t.Errorf("a retryable failure was also logged as dead: %v", logger.messages())
			}
		}
	})

	t.Run("exhausted attempts are logged as dead", func(t *testing.T) {
		task := claimedTask(12, "t", 3, 3) // attempt == maxAttempts ⇒ 已耗尽
		store := &fakeStore{batches: [][]ClaimedTask{{task}}}
		reg := NewRegistry()
		if err := reg.Register("t", func(context.Context, json.RawMessage) error {
			return errors.New("still broken")
		}); err != nil {
			t.Fatalf("Register: %v", err)
		}
		w, logger := workerWithLogger(t, store, reg, nil)

		if _, err := w.runOnce(context.Background()); err != nil {
			t.Fatalf("runOnce() unexpected error: %v", err)
		}

		rec := logger.find(t, "task marked dead")
		if rec.level != LevelWarn {
			t.Errorf("level = %v, want %v", rec.level, LevelWarn)
		}
		if got := rec.fields(t)["attempt"]; got != 3 {
			t.Errorf("attempt = %#v, want 3", got)
		}

		for _, msg := range logger.messages() {
			if msg == "task failed; retry scheduled" {
				t.Errorf("an exhausted task was also logged as a retry: %v", logger.messages())
			}
		}
	})
}

// TestWorkerLogsNeverCarryPayload 是本文件最重要的一条：需求 §8 明确要求
// "不默认打印完整 payload"。payload 与 Handler 错误都可能含敏感数据，
// 一旦哪天有人"顺手把 payload 加上方便排查"，这条用例必须挡住。
func TestWorkerLogsNeverCarryPayload(t *testing.T) {
	const sentinel = "SENTINEL-PAYLOAD-9f3a1c"
	payload := json.RawMessage(`{"card":"` + sentinel + `"}`)

	// 三条路径都跑：成功、可重试失败、死信。只要有一条把 payload 带进日志，
	// 哨兵就会出现在某个 kv 值里。
	cases := []struct {
		name    string
		handler Handler
		attempt int
	}{
		{"succeeded", noopHandler, 1},
		{"retried", func(context.Context, json.RawMessage) error {
			return errors.New("boom")
		}, 1},
		{"dead", func(context.Context, json.RawMessage) error {
			return errors.New("boom")
		}, 3},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			task := claimedTask(21, "t", c.attempt, 3)
			task.Payload = payload
			store := &fakeStore{batches: [][]ClaimedTask{{task}}}
			reg := NewRegistry()
			if err := reg.Register("t", c.handler); err != nil {
				t.Fatalf("Register: %v", err)
			}
			w, logger := workerWithLogger(t, store, reg, nil)

			if _, err := w.runOnce(context.Background()); err != nil {
				t.Fatalf("runOnce() unexpected error: %v", err)
			}

			records := logger.all()
			if len(records) == 0 {
				t.Fatal("no log records at all; the silencing may have gone too far")
			}
			for _, rec := range records {
				for i, v := range rec.kv {
					if strings.Contains(fmt.Sprint(v), sentinel) {
						t.Errorf("log record %q carries payload data at kv[%d] = %v", rec.msg, i, v)
					}
				}
			}
		})
	}
}

func TestWorkerLogsInfrastructureError(t *testing.T) {
	store := &fakeStore{claimErr: errors.New("mysql: connection refused")}
	reg := newRegistryWith(t, "t")
	w, logger := workerWithLogger(t, store, reg, nil)

	// 走真实路径：领取失败由 loop 上报（runOnce 自己只是把错误返回给 loop，
	// 不负责上报），所以这里必须让 Run 跑起来。
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = w.Run(ctx)
	}()
	waitUntil(t, 5*time.Second, "the claim error to be logged", func() bool {
		for _, msg := range logger.messages() {
			if msg == "worker error" {
				return true
			}
		}
		return false
	})
	cancel()
	<-done

	rec := logger.find(t, "worker error")
	if rec.level != LevelError {
		t.Errorf("level = %v, want %v", rec.level, LevelError)
	}
	fields := rec.fields(t)
	if fields["worker_id"] != w.owner {
		t.Errorf("worker_id = %#v, want %q", fields["worker_id"], w.owner)
	}
	if err, ok := fields["error"].(error); !ok || !strings.Contains(err.Error(), "connection refused") {
		t.Errorf("error = %#v, want the infrastructure error", fields["error"])
	}
}

// TestWorkerLogsReapOnlyWhenSomethingWasReaped 钉住"平时安静、有事才出声"：
// 回收每轮都在跑，每轮都记一条会把"真的捡回了任务"那一次埋在噪音里。
func TestWorkerLogsReapOnlyWhenSomethingWasReaped(t *testing.T) {
	t.Run("nothing reaped stays silent", func(t *testing.T) {
		store := &fakeStore{}
		reg := newRegistryWith(t, "t")
		w, logger := workerWithLogger(t, store, reg, func(c *Config) {
			c.ReapInterval = time.Millisecond
		})

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			defer close(done)
			_ = w.Run(ctx)
		}()
		waitUntil(t, 5*time.Second, "the reaper to run at least once", func() bool {
			return len(store.reaped()) > 0
		})
		cancel()
		<-done

		for _, msg := range logger.messages() {
			if msg == "expired leases reaped" {
				t.Errorf("reaped nothing but logged a reap: %v", logger.messages())
			}
		}
	})

	t.Run("a real reap is logged with both counts", func(t *testing.T) {
		// 让回收的返回计数来源于 store：改用返回值可注入的假实现会多一层，
		// 这里直接让 ReapExpiredLeases 返回非零（fakeStore 的 reaped 记录
		// 只记 limit，计数由 reapResult 提供）。
		store := &fakeStore{reapResult: &ReapResult{ToPending: 2, ToDead: 1}}
		reg := newRegistryWith(t, "t")
		w, logger := workerWithLogger(t, store, reg, func(c *Config) {
			c.ReapInterval = time.Millisecond
		})

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			defer close(done)
			_ = w.Run(ctx)
		}()
		waitUntil(t, 5*time.Second, "the reap result to be logged", func() bool {
			for _, msg := range logger.messages() {
				if msg == "expired leases reaped" {
					return true
				}
			}
			return false
		})
		cancel()
		<-done

		fields := logger.find(t, "expired leases reaped").fields(t)
		if fields["to_pending"] != 2 || fields["to_dead"] != 1 {
			t.Errorf("reap counts = %v/%v, want 2/1", fields["to_pending"], fields["to_dead"])
		}
	})
}

// TestWorkerWithoutLoggerStaysSilent 守住"未配置 Logger 时一切照常"：
// nil 接口不该被调用，也不该让任何路径崩掉。
func TestWorkerWithoutLoggerStaysSilent(t *testing.T) {
	task := claimedTask(31, "t", 1, 3)
	store := &fakeStore{batches: [][]ClaimedTask{{task}}}
	reg := newRegistryWith(t, "t")
	w, err := NewWorker(store, reg, fastConfig()) // fastConfig 不设 Logger
	if err != nil {
		t.Fatalf("NewWorker() unexpected error: %v", err)
	}
	if w.cfg.Logger != nil {
		t.Fatalf("cfg.Logger = %#v, want nil", w.cfg.Logger)
	}

	if _, err := w.runOnce(context.Background()); err != nil {
		t.Fatalf("runOnce() unexpected error: %v", err)
	}
	if got := len(store.succeededIDs()); got != 1 {
		t.Errorf("succeeded %d task(s), want 1", got)
	}
}
