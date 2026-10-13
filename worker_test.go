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

// fakeStore 是 WorkerStore 的内存实现。它让 Worker 的执行/确认逻辑可以在
// 不连数据库的情况下被确定性地验证：断言"某条任务最终走了哪条分支"。
type fakeStore struct {
	mu sync.Mutex

	batches    [][]ClaimedTask
	claimSpecs []ClaimSpec
	claimErr   error

	succeeded []TaskID
	retried   []retryCall
	dead      []deadCall

	succeedErr error
	retryErr   error
	deadErr    error

	// onSucceed 在成功确认后被调用（用于测试里等待异步处理完成）。
	onSucceed func(TaskID)
}

type retryCall struct {
	task      ClaimedTask
	delay     time.Duration
	lastError string
}

type deadCall struct {
	task      ClaimedTask
	lastError string
}

func (f *fakeStore) Claim(_ context.Context, spec ClaimSpec) ([]ClaimedTask, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.claimSpecs = append(f.claimSpecs, spec)
	if f.claimErr != nil {
		return nil, f.claimErr
	}
	if len(f.batches) == 0 {
		return nil, nil
	}
	batch := f.batches[0]
	f.batches = f.batches[1:]
	return batch, nil
}

func (f *fakeStore) Succeed(_ context.Context, task ClaimedTask) error {
	f.mu.Lock()
	f.succeeded = append(f.succeeded, task.ID)
	hook, err := f.onSucceed, f.succeedErr
	f.mu.Unlock()

	if hook != nil {
		hook(task.ID)
	}
	return err
}

func (f *fakeStore) Retry(_ context.Context, task ClaimedTask, delay time.Duration, lastError string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.retried = append(f.retried, retryCall{task: task, delay: delay, lastError: lastError})
	return f.retryErr
}

func (f *fakeStore) MarkDead(_ context.Context, task ClaimedTask, lastError string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.dead = append(f.dead, deadCall{task: task, lastError: lastError})
	return f.deadErr
}

func (f *fakeStore) specs() []ClaimSpec {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]ClaimSpec(nil), f.claimSpecs...)
}

func (f *fakeStore) succeededIDs() []TaskID {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]TaskID(nil), f.succeeded...)
}

func (f *fakeStore) retryCalls() []retryCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]retryCall(nil), f.retried...)
}

func (f *fakeStore) deadCalls() []deadCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]deadCall(nil), f.dead...)
}

// claimedTask 造一条领取结果。
func claimedTask(id TaskID, taskType string, attempt, maxAttempts int) ClaimedTask {
	return ClaimedTask{
		ID:          id,
		Queue:       DefaultQueue,
		Type:        taskType,
		Payload:     json.RawMessage(`{}`),
		Attempt:     attempt,
		MaxAttempts: maxAttempts,
		Owner:       "test-owner",
		Epoch:       1,
	}
}

// newRegistryWith 注册一个 noopHandler 并返回注册表。
func newRegistryWith(t *testing.T, taskTypes ...string) *Registry {
	t.Helper()

	r := NewRegistry()
	for _, taskType := range taskTypes {
		if err := r.Register(taskType, noopHandler); err != nil {
			t.Fatalf("Register(%q): %v", taskType, err)
		}
	}
	return r
}

// fastConfig 返回一个轮询极快的配置，便于测试 Run 的循环而不拖慢用例。
func fastConfig() Config {
	return Config{
		Queues:          []string{DefaultQueue},
		Concurrency:     1,
		PollInterval:    time.Millisecond,
		MaxPollInterval: 5 * time.Millisecond,
	}
}

func TestNewWorkerRejectsInvalidArguments(t *testing.T) {
	store := &fakeStore{}

	tests := []struct {
		name     string
		store    WorkerStore
		registry *Registry
		cfg      Config
	}{
		{"nil store", nil, newRegistryWith(t, "t"), Config{}},
		{"nil registry", store, nil, Config{}},
		{"empty registry", store, NewRegistry(), Config{}},
		{"invalid config", store, newRegistryWith(t, "t"), Config{Concurrency: -1}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewWorker(tt.store, tt.registry, tt.cfg)
			if !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("NewWorker() error = %v, want ErrInvalidConfig", err)
			}
		})
	}
}

func TestNewWorkerFillsDefaultsAndValidates(t *testing.T) {
	w, err := NewWorker(&fakeStore{}, newRegistryWith(t, "t"), Config{})
	if err != nil {
		t.Fatalf("NewWorker() unexpected error: %v", err)
	}
	if got, want := w.cfg.Concurrency, DefaultConcurrency; got != want {
		t.Errorf("Concurrency = %d, want %d", got, want)
	}
	if got, want := w.cfg.BatchSize, DefaultBatchSize; got != want {
		t.Errorf("BatchSize = %d, want %d", got, want)
	}
	if got, want := w.cfg.LeaseDuration, DefaultLeaseDuration; got != want {
		t.Errorf("LeaseDuration = %s, want %s", got, want)
	}
	if len(w.cfg.Queues) != 1 || w.cfg.Queues[0] != DefaultQueue {
		t.Errorf("Queues = %v, want [%s]", w.cfg.Queues, DefaultQueue)
	}
}

func TestNewWorkerOwnerIsUsableAndUnique(t *testing.T) {
	a, err := NewWorker(&fakeStore{}, newRegistryWith(t, "t"), Config{})
	if err != nil {
		t.Fatalf("NewWorker() unexpected error: %v", err)
	}
	b, err := NewWorker(&fakeStore{}, newRegistryWith(t, "t"), Config{})
	if err != nil {
		t.Fatalf("NewWorker() unexpected error: %v", err)
	}

	if a.owner == "" {
		t.Fatal("owner is empty; it becomes lease_owner and must not be blank")
	}
	if a.owner == b.owner {
		t.Errorf("two workers share owner %q; fencing would not distinguish them", a.owner)
	}
	// lease_owner 是 VARCHAR(128)：留足余量，绝不能被方言实现判为超长。
	if n := len([]rune(a.owner)); n > 128 {
		t.Errorf("owner is %d characters, must fit in VARCHAR(128)", n)
	}
}

func TestWorkerClaimSpecMatchesConfig(t *testing.T) {
	store := &fakeStore{}
	reg := newRegistryWith(t, "t")
	cfg := fastConfig()
	cfg.BatchSize = 3
	cfg.LeaseDuration = 42 * time.Second

	w, err := NewWorker(store, reg, cfg)
	if err != nil {
		t.Fatalf("NewWorker() unexpected error: %v", err)
	}
	if _, err := w.runOnce(context.Background()); err != nil {
		t.Fatalf("runOnce() error: %v", err)
	}

	specs := store.specs()
	if len(specs) != 1 {
		t.Fatalf("Claim called %d times, want 1", len(specs))
	}
	got := specs[0]
	if got.Queue != DefaultQueue {
		t.Errorf("Queue = %q, want %q", got.Queue, DefaultQueue)
	}
	if got.Limit != 3 {
		t.Errorf("Limit = %d, want 3 (Config.BatchSize)", got.Limit)
	}
	if got.Owner != w.owner {
		t.Errorf("Owner = %q, want the worker owner %q", got.Owner, w.owner)
	}
	if got.LeaseDuration != 42*time.Second {
		t.Errorf("LeaseDuration = %s, want 42s (Config.LeaseDuration)", got.LeaseDuration)
	}
}

func TestWorkerSucceedsTask(t *testing.T) {
	store := &fakeStore{batches: [][]ClaimedTask{{claimedTask(7, "t", 1, 3)}}}
	var ran int
	reg := NewRegistry()
	if err := reg.Register("t", func(context.Context, json.RawMessage) error { ran++; return nil }); err != nil {
		t.Fatalf("Register: %v", err)
	}

	w, err := NewWorker(store, reg, fastConfig())
	if err != nil {
		t.Fatalf("NewWorker() unexpected error: %v", err)
	}
	n, err := w.runOnce(context.Background())
	if err != nil {
		t.Fatalf("runOnce() error: %v", err)
	}
	if n != 1 {
		t.Errorf("runOnce() claimed %d, want 1", n)
	}
	if ran != 1 {
		t.Errorf("handler ran %d times, want 1", ran)
	}
	if got := store.succeededIDs(); len(got) != 1 || got[0] != 7 {
		t.Errorf("succeeded = %v, want [7]", got)
	}
	if len(store.retryCalls()) != 0 || len(store.deadCalls()) != 0 {
		t.Error("a successful task must not be retried or marked dead")
	}
}

func TestWorkerRetriesFailedHandler(t *testing.T) {
	store := &fakeStore{batches: [][]ClaimedTask{{claimedTask(11, "t", 1, 3)}}}
	reg := NewRegistry()
	if err := reg.Register("t", func(context.Context, json.RawMessage) error {
		return errors.New("smtp unavailable")
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	w, err := NewWorker(store, reg, fastConfig())
	if err != nil {
		t.Fatalf("NewWorker() unexpected error: %v", err)
	}
	if _, err := w.runOnce(context.Background()); err != nil {
		t.Fatalf("runOnce() error: %v", err)
	}

	retries := store.retryCalls()
	if len(retries) != 1 {
		t.Fatalf("retry called %d times, want 1", len(retries))
	}
	if retries[0].task.ID != 11 {
		t.Errorf("retried task %d, want 11", retries[0].task.ID)
	}
	if retries[0].delay != 0 {
		t.Errorf("retry delay = %s, want 0 (立即重试，退避留给 §4)", retries[0].delay)
	}
	if !strings.Contains(retries[0].lastError, "smtp unavailable") {
		t.Errorf("lastError = %q, want the handler error text", retries[0].lastError)
	}
	if len(store.deadCalls()) != 0 {
		t.Error("a retryable failure with attempts left must not go to dead")
	}
}

func TestWorkerMarksDeadWhenAttemptsExhausted(t *testing.T) {
	// Attempt 是"包含本次"的次数，因此 attempt == max 就是最后一次。
	store := &fakeStore{batches: [][]ClaimedTask{{claimedTask(13, "t", 3, 3)}}}
	reg := NewRegistry()
	if err := reg.Register("t", func(context.Context, json.RawMessage) error {
		return errors.New("still broken")
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	w, err := NewWorker(store, reg, fastConfig())
	if err != nil {
		t.Fatalf("NewWorker() unexpected error: %v", err)
	}
	if _, err := w.runOnce(context.Background()); err != nil {
		t.Fatalf("runOnce() error: %v", err)
	}

	dead := store.deadCalls()
	if len(dead) != 1 {
		t.Fatalf("MarkDead called %d times, want 1", len(dead))
	}
	if dead[0].task.ID != 13 {
		t.Errorf("dead task %d, want 13", dead[0].task.ID)
	}
	if !strings.Contains(dead[0].lastError, "still broken") {
		t.Errorf("lastError = %q, want the handler error text", dead[0].lastError)
	}
	if len(store.retryCalls()) != 0 {
		t.Error("an exhausted task must not also be retried")
	}
}

// TestWorkerUnknownTypeIsRetriedForNow 记录本项的既定边界：未知 task type 的
// 错误虽然带永久标记，但"永久错误直接进 dead"属于 §4「重试退避与永久错误」，
// 本项一律按可重试处理，因此它会重试到次数耗尽才进 dead。
func TestWorkerUnknownTypeIsRetriedForNow(t *testing.T) {
	store := &fakeStore{batches: [][]ClaimedTask{{claimedTask(17, "missing", 1, 5)}}}
	w, err := NewWorker(store, newRegistryWith(t, "t"), fastConfig())
	if err != nil {
		t.Fatalf("NewWorker() unexpected error: %v", err)
	}
	if _, err := w.runOnce(context.Background()); err != nil {
		t.Fatalf("runOnce() error: %v", err)
	}

	retries := store.retryCalls()
	if len(retries) != 1 {
		t.Fatalf("retry called %d times, want 1", len(retries))
	}
	if !strings.Contains(retries[0].lastError, "no handler") {
		t.Errorf("lastError = %q, want it to mention the missing handler", retries[0].lastError)
	}
}

func TestWorkerReportsWriteBackFailureWithoutCrashing(t *testing.T) {
	store := &fakeStore{
		batches:    [][]ClaimedTask{{claimedTask(19, "t", 1, 3)}},
		succeedErr: fmt.Errorf("%w: task 19", ErrLeaseLost),
	}
	var reported []error
	w, err := NewWorker(store, newRegistryWith(t, "t"), fastConfig())
	if err != nil {
		t.Fatalf("NewWorker() unexpected error: %v", err)
	}
	w.onError = func(err error) { reported = append(reported, err) }

	// 领取本身成功，因此 runOnce 不该返回 error；写回失败只上报。
	if _, err := w.runOnce(context.Background()); err != nil {
		t.Fatalf("runOnce() error = %v, want nil (写回失败不是领取失败)", err)
	}
	if len(reported) != 1 {
		t.Fatalf("reported %d errors, want 1", len(reported))
	}
	if !errors.Is(reported[0], ErrLeaseLost) {
		t.Errorf("reported %v, want it to wrap ErrLeaseLost", reported[0])
	}
}

func TestWorkerSurfacesClaimError(t *testing.T) {
	sentinel := errors.New("database is down")
	store := &fakeStore{claimErr: sentinel}
	w, err := NewWorker(store, newRegistryWith(t, "t"), fastConfig())
	if err != nil {
		t.Fatalf("NewWorker() unexpected error: %v", err)
	}

	_, err = w.runOnce(context.Background())
	if err == nil {
		t.Fatal("runOnce() error = nil, want the claim error")
	}
	if !errors.Is(err, sentinel) {
		t.Errorf("runOnce() error = %v, want it to wrap the claim error", err)
	}
}

func TestWorkerRetryFailureIsReported(t *testing.T) {
	store := &fakeStore{
		batches:  [][]ClaimedTask{{claimedTask(23, "t", 1, 3)}},
		retryErr: errors.New("write failed"),
	}
	reg := NewRegistry()
	if err := reg.Register("t", func(context.Context, json.RawMessage) error {
		return errors.New("boom")
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	var reported []error
	w, err := NewWorker(store, reg, fastConfig())
	if err != nil {
		t.Fatalf("NewWorker() unexpected error: %v", err)
	}
	w.onError = func(err error) { reported = append(reported, err) }

	if _, err := w.runOnce(context.Background()); err != nil {
		t.Fatalf("runOnce() error: %v", err)
	}
	if len(reported) != 1 || !strings.Contains(reported[0].Error(), "retry task 23") {
		t.Fatalf("reported = %v, want one error mentioning retry task 23", reported)
	}
}

func TestWorkerRunProcessesTasksThenStopsOnCancel(t *testing.T) {
	const total = 3
	store := &fakeStore{batches: [][]ClaimedTask{{
		claimedTask(1, "t", 1, 3),
		claimedTask(2, "t", 1, 3),
		claimedTask(3, "t", 1, 3),
	}}}

	done := make(chan TaskID, total)
	store.onSucceed = func(id TaskID) { done <- id }

	w, err := NewWorker(store, newRegistryWith(t, "t"), fastConfig())
	if err != nil {
		t.Fatalf("NewWorker() unexpected error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- w.Run(ctx) }()

	for i := 0; i < total; i++ {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatalf("only %d/%d tasks were processed", i, total)
		}
	}
	cancel()

	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run() = %v, want nil on cancellation", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run() did not return after ctx was canceled")
	}

	if got := len(store.succeededIDs()); got != total {
		t.Errorf("succeeded %d tasks, want %d", got, total)
	}
}

// TestWorkerRunHonoursConcurrency 用 Handler 内的并发计数证明 Concurrency 个
// goroutine 确实同时在跑：每个 Handler 都阻塞在同一个栅栏上，只有 N 个同时
// 进入才能放行。
func TestWorkerRunHonoursConcurrency(t *testing.T) {
	const concurrency = 4

	batches := make([][]ClaimedTask, 0, concurrency)
	for i := 0; i < concurrency; i++ {
		batches = append(batches, []ClaimedTask{claimedTask(TaskID(i+1), "t", 1, 3)})
	}
	store := &fakeStore{batches: batches}

	var (
		mu         sync.Mutex
		running    int
		maxRunning int
	)
	release := make(chan struct{})
	reg := NewRegistry()
	if err := reg.Register("t", func(context.Context, json.RawMessage) error {
		mu.Lock()
		running++
		if running > maxRunning {
			maxRunning = running
		}
		allArrived := running == concurrency
		mu.Unlock()

		if allArrived {
			close(release) // 只在"最后一个到达者"手里关一次
		}
		select {
		case <-release:
		case <-time.After(3 * time.Second):
			// 没等到全部并发到达：不阻塞用例，让断言去报错。
		}

		mu.Lock()
		running--
		mu.Unlock()
		return nil
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	cfg := fastConfig()
	cfg.Concurrency = concurrency
	w, err := NewWorker(store, reg, cfg)
	if err != nil {
		t.Fatalf("NewWorker() unexpected error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- w.Run(ctx) }()

	deadline := time.After(5 * time.Second)
	for {
		mu.Lock()
		done := maxRunning == concurrency
		mu.Unlock()
		if done {
			break
		}
		select {
		case <-deadline:
			mu.Lock()
			got := maxRunning
			mu.Unlock()
			cancel()
			t.Fatalf("max concurrent handlers = %d, want %d", got, concurrency)
		case <-time.After(time.Millisecond):
		}
	}
	cancel()
	<-runDone

	if got := len(store.succeededIDs()); got != concurrency {
		t.Errorf("succeeded %d tasks, want %d", got, concurrency)
	}
}

func TestWorkerPickQueueRoundRobins(t *testing.T) {
	cfg := fastConfig()
	cfg.Queues = []string{"a", "b", "c"}
	w, err := NewWorker(&fakeStore{}, newRegistryWith(t, "t"), cfg)
	if err != nil {
		t.Fatalf("NewWorker() unexpected error: %v", err)
	}

	want := []string{"a", "b", "c", "a", "b", "c"}
	for i, expected := range want {
		if got := w.pickQueue(); got != expected {
			t.Fatalf("pickQueue() #%d = %q, want %q", i, got, expected)
		}
	}
}

func TestWorkerSleepBacksOffUpToMax(t *testing.T) {
	cfg := fastConfig()
	cfg.PollInterval = 10 * time.Millisecond
	cfg.MaxPollInterval = 40 * time.Millisecond
	w, err := NewWorker(&fakeStore{}, newRegistryWith(t, "t"), cfg)
	if err != nil {
		t.Fatalf("NewWorker() unexpected error: %v", err)
	}

	ctx := context.Background()
	// 从 0 起：sleep 会把低于 PollInterval 的退避值抬到 PollInterval。
	backoff := time.Duration(0)
	for _, want := range []time.Duration{20 * time.Millisecond, 40 * time.Millisecond, 40 * time.Millisecond, 40 * time.Millisecond} {
		backoff = w.sleep(ctx, backoff)
		if backoff != want {
			t.Fatalf("next backoff = %s, want %s", backoff, want)
		}
	}
}

func TestJitterStaysWithinHalfToFull(t *testing.T) {
	const d = 100 * time.Millisecond
	for i := 0; i < 200; i++ {
		got := jitter(d)
		if got < d/2 || got > d {
			t.Fatalf("jitter(%s) = %s, want within [%s, %s]", d, got, d/2, d)
		}
	}
	if got := jitter(0); got != 0 {
		t.Errorf("jitter(0) = %s, want 0", got)
	}
}

func TestSleepContextHonoursCancellation(t *testing.T) {
	if err := sleepContext(context.Background(), 0); err != nil {
		t.Errorf("sleepContext(0) = %v, want nil", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	if err := sleepContext(ctx, time.Minute); !errors.Is(err, context.Canceled) {
		t.Errorf("sleepContext(canceled) = %v, want context.Canceled", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("sleepContext blocked for %s after cancellation", elapsed)
	}
}

func TestTruncateRunes(t *testing.T) {
	tests := []struct {
		in   string
		n    int
		want string
	}{
		{"hello", 10, "hello"},
		{"hello", 5, "hello"},
		{"hello", 3, "hel"},
		{"任务队列", 2, "任务"},
		{"任务队列", 4, "任务队列"},
		{"任务队列", 0, ""},
		{"", 3, ""},
	}
	for _, tt := range tests {
		if got := truncateRunes(tt.in, tt.n); got != tt.want {
			t.Errorf("truncateRunes(%q, %d) = %q, want %q", tt.in, tt.n, got, tt.want)
		}
	}
}

func TestOwnerIDIsBoundedAndFallsBack(t *testing.T) {
	if got := ownerID("", 42, 0xabc); !strings.HasPrefix(got, "worker-42-") {
		t.Errorf("ownerID with an empty host = %q, want the worker- fallback", got)
	}

	longHost := strings.Repeat("h", 500)
	got := ownerID(longHost, 42, 0)
	if n := len([]rune(got)); n > 128 {
		t.Errorf("ownerID length = %d, must fit in VARCHAR(128)", n)
	}
	if !strings.HasPrefix(got, strings.Repeat("h", 64)+"-42-") {
		t.Errorf("ownerID = %q, want the host truncated to 64 characters", got)
	}
}

func TestWorkerMarkDeadFailureIsReported(t *testing.T) {
	store := &fakeStore{
		batches: [][]ClaimedTask{{claimedTask(29, "t", 3, 3)}},
		deadErr: errors.New("write failed"),
	}
	reg := NewRegistry()
	if err := reg.Register("t", func(context.Context, json.RawMessage) error {
		return errors.New("boom")
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	var reported []error
	w, err := NewWorker(store, reg, fastConfig())
	if err != nil {
		t.Fatalf("NewWorker() unexpected error: %v", err)
	}
	w.onError = func(err error) { reported = append(reported, err) }

	if _, err := w.runOnce(context.Background()); err != nil {
		t.Fatalf("runOnce() error: %v", err)
	}
	if len(reported) != 1 || !strings.Contains(reported[0].Error(), "mark task 29 dead") {
		t.Fatalf("reported = %v, want one error mentioning mark task 29 dead", reported)
	}
}

// TestWorkerRunReportsClaimErrorsAndKeepsRunning 固化"领取失败不退出循环"：
// 数据库暂时不可用时 Worker 应当退避重试，而不是整体退出。
func TestWorkerRunReportsClaimErrorsAndKeepsRunning(t *testing.T) {
	store := &fakeStore{claimErr: errors.New("database is down")}

	reported := make(chan error, 8)
	w, err := NewWorker(store, newRegistryWith(t, "t"), fastConfig())
	if err != nil {
		t.Fatalf("NewWorker() unexpected error: %v", err)
	}
	w.onError = func(err error) {
		select {
		case reported <- err:
		default:
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- w.Run(ctx) }()

	select {
	case err := <-reported:
		if !strings.Contains(err.Error(), "claim tasks") {
			t.Errorf("reported error = %v, want it to mention claim tasks", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a claim error was never reported; the loop should surface it and keep running")
	}
	cancel()

	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Run() did not return after ctx was canceled")
	}
}

// TestWorkerRunStopsWhenCancelledDuringBatch 覆盖"处理完一批后被取消"的分支：
// ctx 在批次执行期间被取消时，循环应立即退出，不再进入下一轮。
func TestWorkerRunStopsWhenCancelledDuringBatch(t *testing.T) {
	store := &fakeStore{batches: [][]ClaimedTask{{
		claimedTask(31, "t", 1, 3),
		claimedTask(32, "t", 1, 3),
	}}}

	ctx, cancel := context.WithCancel(context.Background())
	store.onSucceed = func(TaskID) { cancel() }

	w, err := NewWorker(store, newRegistryWith(t, "t"), fastConfig())
	if err != nil {
		t.Fatalf("NewWorker() unexpected error: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run() = %v, want nil on cancellation", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run() did not return after being canceled mid-batch")
	}
}
