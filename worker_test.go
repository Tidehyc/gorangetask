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
	renewals  []ClaimedTask

	succeedErr error
	retryErr   error
	deadErr    error

	// onSucceed 在成功确认后被调用（用于测试里等待异步处理完成）。
	onSucceed func(TaskID)

	// renewFn 决定第 call 次（从 1 开始）续租的返回；为 nil 时一律成功。
	// 用例用它来制造失租、数据库错误，或把"已经续够了"当作同步点。它拿到的是
	// 这次续租自己的 ctx，因此也能模拟"语句执行到一半被取消"的真驱动行为。
	renewFn func(ctx context.Context, call int, task ClaimedTask) error

	// renewsInFlight 是当前正在进行的续租数，writeBackWhileRenewing 记录
	// "写回时仍有续租在途"这一越界行为。process 必须先停止看护再写回，
	// 否则续租会在任务写成终态之后到达并凭空报出 ErrLeaseLost。
	renewsInFlight         int
	writeBackWhileRenewing bool

	// honourCtx 为 true 时，写回方法像真实驱动那样在 ctx 已取消时直接失败。
	// 用于证明"停机期间的成功确认确实换用了脱离取消的 context"——否则这个
	// 开关一开，停机中的确认就会（错误地）失败。
	honourCtx bool
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

func (f *fakeStore) Succeed(ctx context.Context, task ClaimedTask) error {
	f.noteWriteBack()
	if err := f.ctxErr(ctx); err != nil {
		return err
	}

	f.mu.Lock()
	f.succeeded = append(f.succeeded, task.ID)
	hook, err := f.onSucceed, f.succeedErr
	f.mu.Unlock()

	if hook != nil {
		hook(task.ID)
	}
	return err
}

func (f *fakeStore) Retry(ctx context.Context, task ClaimedTask, delay time.Duration, lastError string) error {
	f.noteWriteBack()
	if err := f.ctxErr(ctx); err != nil {
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	f.retried = append(f.retried, retryCall{task: task, delay: delay, lastError: lastError})
	return f.retryErr
}

func (f *fakeStore) MarkDead(ctx context.Context, task ClaimedTask, lastError string) error {
	f.noteWriteBack()
	if err := f.ctxErr(ctx); err != nil {
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	f.dead = append(f.dead, deadCall{task: task, lastError: lastError})
	return f.deadErr
}

// RenewLease 记录一次续租，并调用 renewFn 决定结果。
func (f *fakeStore) RenewLease(ctx context.Context, task ClaimedTask, _ time.Duration) error {
	if err := f.ctxErr(ctx); err != nil {
		return err
	}

	f.mu.Lock()
	f.renewals = append(f.renewals, task)
	call := len(f.renewals)
	f.renewsInFlight++
	fn := f.renewFn
	f.mu.Unlock()

	var err error
	if fn != nil {
		err = fn(ctx, call, task)
	}

	f.mu.Lock()
	f.renewsInFlight--
	f.mu.Unlock()
	return err
}

// noteWriteBack 记录一次状态写回，并检查它是否与续租重叠：
// process 必须"先停看护、再写回"，重叠就说明这个顺序被破坏了。
func (f *fakeStore) noteWriteBack() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.renewsInFlight > 0 {
		f.writeBackWhileRenewing = true
	}
}

// ctxErr 在开了 honourCtx 时把 ctx 的取消状态当作驱动错误返回。
func (f *fakeStore) ctxErr(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.honourCtx {
		return ctx.Err()
	}
	return nil
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

func (f *fakeStore) renewCalls() []ClaimedTask {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]ClaimedTask(nil), f.renewals...)
}

func (f *fakeStore) wroteBackWhileRenewing() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.writeBackWhileRenewing
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

// TestWorkerShutdownCancelsInFlightHandlerAndDoesNotWriteBack 固化停机的核心约定：
// ctx 取消会传到在途 Handler 手里；它因停机返回的错误**不写回存储**——把这种
// "被取消"当成任务失败写回 pending，会让另一个 Worker 在旧副作用可能仍在途时
// 立刻重跑（需求文档 §4.6）。任务保持 running，等租约过期由回收器恢复。
func TestWorkerShutdownCancelsInFlightHandlerAndDoesNotWriteBack(t *testing.T) {
	store := &fakeStore{batches: [][]ClaimedTask{{claimedTask(41, "t", 1, 3)}}}

	started := make(chan struct{})
	sawCancel := make(chan error, 1)
	sawCause := make(chan error, 1)
	reg := NewRegistry()
	if err := reg.Register("t", func(ctx context.Context, _ json.RawMessage) error {
		close(started)
		<-ctx.Done() // 只观察取消，不做业务
		sawCancel <- ctx.Err()
		sawCause <- context.Cause(ctx)
		return ctx.Err()
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	w, err := NewWorker(store, reg, fastConfig())
	if err != nil {
		t.Fatalf("NewWorker() unexpected error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- w.Run(ctx) }()

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("handler never started")
	}
	// 先取消，再等 Handler 观察取消：这样"取消传到了 Handler 手里"与
	// "Run 在超时内返回"两件事都能被观察到。
	cancel()

	select {
	case err := <-sawCancel:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("handler saw context error %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("handler was never cancelled by shutdown")
	}

	// 停机的取消原因必须与"失去租约"区分开：失租会让 Worker 连成功都不写回，
	// 而停机允许把已经成功的结果确认掉。把两者混成一个原因就会改错行为。
	select {
	case cause := <-sawCause:
		if !errors.Is(cause, context.Canceled) || errors.Is(cause, ErrLeaseLost) {
			t.Errorf("context.Cause = %v, want context.Canceled (not a lease loss)", cause)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("handler never reported the cancellation cause")
	}

	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run() = %v, want nil (handler exited within ShutdownTimeout)", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run() did not return after shutdown")
	}

	if n := len(store.retryCalls()); n != 0 {
		t.Errorf("retried %d time(s), want 0: a shutdown-cancelled task must not be released early", n)
	}
	if n := len(store.deadCalls()); n != 0 {
		t.Errorf("marked %d dead, want 0: a shutdown-cancelled task is not a real failure", n)
	}
	if n := len(store.succeededIDs()); n != 0 {
		t.Errorf("succeeded %d, want 0: the handler never completed", n)
	}
}

// TestWorkerShutdownTimeoutAbandonsStuckHandler 覆盖"Handler 不理会 ctx"：
// Run 必须在 ShutdownTimeout 之后带着 ErrShutdownTimeout 返回，而不是无限等待。
func TestWorkerShutdownTimeoutAbandonsStuckHandler(t *testing.T) {
	store := &fakeStore{batches: [][]ClaimedTask{{claimedTask(43, "t", 1, 3)}}}

	started := make(chan struct{})
	release := make(chan struct{})
	// 用例结束后放走被放弃的 goroutine，避免它一直挂着。
	t.Cleanup(func() { close(release) })

	reg := NewRegistry()
	if err := reg.Register("t", func(context.Context, json.RawMessage) error {
		close(started)
		<-release // 故意无视 ctx 的 Handler：这类只能靠超时放弃
		return errors.New("released after the worker gave up")
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	cfg := fastConfig()
	cfg.ShutdownTimeout = 50 * time.Millisecond
	w, err := NewWorker(store, reg, cfg)
	if err != nil {
		t.Fatalf("NewWorker() unexpected error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- w.Run(ctx) }()

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("handler never started")
	}

	begin := time.Now()
	cancel()

	select {
	case err := <-runDone:
		if !errors.Is(err, ErrShutdownTimeout) {
			t.Fatalf("Run() = %v, want ErrShutdownTimeout", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run() did not return; it must give up after ShutdownTimeout")
	}

	// Handler 一直不退出，因此 Run 只能等满整个预算才放弃——这也顺带证明
	// 它没有提前返回。
	if elapsed := time.Since(begin); elapsed < cfg.ShutdownTimeout {
		t.Errorf("Run() gave up after %s, want it to wait at least ShutdownTimeout (%s)", elapsed, cfg.ShutdownTimeout)
	}
}

// TestWorkerShutdownAcksHandlerThatFinishedInTime 覆盖停机期间的"已完成"路径：
// Handler 在停机开始后仍然成功返回时，结果必须被确认——它已明确结束，确认不会
// 造成并行执行。store 开了 honourCtx（像真驱动一样拒绝已取消的 ctx），因此这条
// 断言只有在确认确实换用了脱离取消的 context 时才能通过。
func TestWorkerShutdownAcksHandlerThatFinishedInTime(t *testing.T) {
	store := &fakeStore{
		batches:   [][]ClaimedTask{{claimedTask(47, "t", 1, 3)}},
		honourCtx: true,
	}

	started := make(chan struct{})
	release := make(chan struct{})
	reg := NewRegistry()
	if err := reg.Register("t", func(context.Context, json.RawMessage) error {
		close(started)
		<-release // 先挂住，等停机开始之后才成功返回
		return nil
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	w, err := NewWorker(store, reg, fastConfig())
	if err != nil {
		t.Fatalf("NewWorker() unexpected error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- w.Run(ctx) }()

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("handler never started")
	}
	cancel()       // 先停机……
	close(release) // ……再让已经干完活的 Handler 返回

	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run() = %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run() did not return after shutdown")
	}

	if got := store.succeededIDs(); len(got) != 1 || got[0] != 47 {
		t.Errorf("succeeded = %v, want [47]: a handler that completed must still be acked", got)
	}
}

// TestWorkerStopsLaunchingHandlersAfterShutdown 覆盖批内停机：ctx 在批次执行
// 途中被取消时，批内剩下的任务不再启动 Handler（保持 running 交给回收器），
// 整批都不产生任何写回。
func TestWorkerStopsLaunchingHandlersAfterShutdown(t *testing.T) {
	store := &fakeStore{batches: [][]ClaimedTask{{
		claimedTask(51, "t", 1, 3),
		claimedTask(52, "t", 1, 3),
		claimedTask(53, "t", 1, 3),
	}}}

	var (
		mu  sync.Mutex
		ran int
	)
	started := make(chan struct{})
	reg := NewRegistry()
	if err := reg.Register("t", func(ctx context.Context, _ json.RawMessage) error {
		mu.Lock()
		ran++
		first := ran == 1
		mu.Unlock()
		if first {
			close(started)
		}
		<-ctx.Done()
		return ctx.Err()
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	cfg := fastConfig()
	cfg.BatchSize = 3 // 让三条任务落在同一批里
	w, err := NewWorker(store, reg, cfg)
	if err != nil {
		t.Fatalf("NewWorker() unexpected error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- w.Run(ctx) }()

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("handler never started")
	}
	cancel()

	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run() = %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run() did not return after shutdown")
	}

	mu.Lock()
	got := ran
	mu.Unlock()
	if got != 1 {
		t.Errorf("handler ran %d time(s), want 1: shutdown must stop launching more handlers", got)
	}
	if r, d, s := len(store.retryCalls()), len(store.deadCalls()), len(store.succeededIDs()); r != 0 || d != 0 || s != 0 {
		t.Errorf("write-backs during shutdown: retried=%d dead=%d succeeded=%d, want all 0", r, d, s)
	}
}

// TestWorkerUsesPerTaskTimeout 固定逐任务超时的两条路径：
// task.Timeout > 0 时覆盖 Worker 默认值，为 0 时回落到 cfg.HandlerTimeout。
//
// 断言方式是"Handler 内等 ctx.Done，把观察到的错误与耗时回传"，不靠墙钟睡眠：
// 超时没生效时 Handler 会一直等到安全定时器（5s）才返回，断言随即报错，
// 用例本身不会被挂死。
func TestWorkerUsesPerTaskTimeout(t *testing.T) {
	const cutoff = 60 * time.Millisecond

	tests := []struct {
		name           string
		taskTimeout    time.Duration
		handlerTimeout time.Duration
	}{
		{
			// 默认值设成 10s：若逐任务超时没生效，Handler 会一直等到安全定时器。
			name:           "task timeout overrides the worker default",
			taskTimeout:    cutoff,
			handlerTimeout: 10 * time.Second,
		},
		{
			// 任务没设：必须回落到配置里的 60ms。
			name:           "zero task timeout falls back to the worker default",
			taskTimeout:    0,
			handlerTimeout: cutoff,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			task := claimedTask(61, "t", 1, 3)
			task.Timeout = tt.taskTimeout
			store := &fakeStore{batches: [][]ClaimedTask{{task}}}

			type observation struct {
				err     error
				cause   error
				elapsed time.Duration
			}
			seen := make(chan observation, 1)

			reg := NewRegistry()
			if err := reg.Register("t", func(hctx context.Context, _ json.RawMessage) error {
				start := time.Now()
				select {
				case <-hctx.Done():
				case <-time.After(5 * time.Second):
					// 超时没生效；不阻塞用例，交给断言报错。
				}
				seen <- observation{err: hctx.Err(), cause: context.Cause(hctx), elapsed: time.Since(start)}
				return hctx.Err()
			}); err != nil {
				t.Fatalf("Register: %v", err)
			}

			cfg := fastConfig()
			cfg.HandlerTimeout = tt.handlerTimeout
			w, err := NewWorker(store, reg, cfg)
			if err != nil {
				t.Fatalf("NewWorker() unexpected error: %v", err)
			}

			if _, err := w.runOnce(context.Background()); err != nil {
				t.Fatalf("runOnce() error: %v", err)
			}

			got := <-seen
			if !errors.Is(got.err, context.DeadlineExceeded) {
				t.Fatalf("handler context error = %v, want context.DeadlineExceeded", got.err)
			}
			// 取消原因必须能被分辨出来：超时就是超时，不能与停机、失租混为一谈。
			if !errors.Is(got.cause, context.DeadlineExceeded) {
				t.Errorf("context.Cause = %v, want context.DeadlineExceeded", got.cause)
			}
			// 截断只能来自 deadline，不能来自别处：太短说明用错了更小的值，
			// 接近 5s 说明超时根本没生效（安全定时器兜的底）。
			if got.elapsed < cutoff/2 || got.elapsed > 2*time.Second {
				t.Errorf("handler ran for %s before cancellation, want roughly %s", got.elapsed, cutoff)
			}
			// 超时是普通的任务失败，应当走重试路径，而不是被当成停机。
			if retries := store.retryCalls(); len(retries) != 1 {
				t.Errorf("retry called %d time(s), want 1: a handler timeout is a normal task failure", len(retries))
			}
		})
	}
}

// --- 租约续期与失租 ---------------------------------------------------------
//
// 这几条用例都直接调 runOnce 而不是 Run：心跳看护是在 process 里起的，
// 与 Run 的领取循环无关，单独跑一轮更确定。

// TestWorkerRenewsLeaseWhileHandlerRuns 固定"在途任务会被持续续租"，并顺带固定
// 续租用的是**本次领取**的 owner/epoch——续租与三条写回一样是"凭租约说话"的操作，
// 用错凭据续租等于给别人的任务续命。
func TestWorkerRenewsLeaseWhileHandlerRuns(t *testing.T) {
	const renewals = 3

	store := &fakeStore{batches: [][]ClaimedTask{{claimedTask(7, "t", 1, 3)}}}
	enough := make(chan struct{})
	var once sync.Once
	store.renewFn = func(_ context.Context, call int, _ ClaimedTask) error {
		if call >= renewals {
			once.Do(func() { close(enough) })
		}
		return nil
	}

	reg := NewRegistry()
	// Handler 一直跑到"续租确实发生过 N 次"为止：等一个可观测的事实，
	// 而不是睡固定时长——宿主负载高时它只会变慢，不会误报。
	if err := reg.Register("t", func(hctx context.Context, _ json.RawMessage) error {
		select {
		case <-enough:
			return nil
		case <-hctx.Done():
			return context.Cause(hctx)
		case <-time.After(10 * time.Second):
			return errors.New("handler waited too long for lease renewals")
		}
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	cfg := fastConfig()
	cfg.LeaseDuration = time.Hour // 租约远长于用例时长，续租只可能来自看护
	cfg.HeartbeatInterval = time.Millisecond
	w, err := NewWorker(store, reg, cfg)
	if err != nil {
		t.Fatalf("NewWorker() unexpected error: %v", err)
	}

	if _, err := w.runOnce(context.Background()); err != nil {
		t.Fatalf("runOnce() error: %v", err)
	}

	renews := store.renewCalls()
	if len(renews) < renewals {
		t.Fatalf("renewed the lease %d time(s), want at least %d", len(renews), renewals)
	}
	want := claimedTask(7, "t", 1, 3)
	for i, got := range renews {
		if got.ID != want.ID || got.Owner != want.Owner || got.Epoch != want.Epoch {
			t.Errorf("renewal %d used id=%d owner=%q epoch=%d, want id=%d owner=%q epoch=%d",
				i+1, got.ID, got.Owner, got.Epoch, want.ID, want.Owner, want.Epoch)
		}
	}
	// 续租只是延长租约，不能改变任务的结果。
	if got := store.succeededIDs(); len(got) != 1 || got[0] != 7 {
		t.Errorf("succeeded = %v, want [7]", got)
	}
	if n := len(store.retryCalls()); n != 0 {
		t.Errorf("retried %d time(s), want 0", n)
	}
	if n := len(store.deadCalls()); n != 0 {
		t.Errorf("marked %d dead, want 0", n)
	}
}

// TestWorkerCancelsHandlerWhenLeaseIsLost 固定失租后的完整反应（需求文档 §4.4）：
// 续租被拒 ⇒ 取消 Handler 的 context，且 Handler 能看到原因是 ErrLeaseLost；
// 本次执行的任何结果都**不写回**——任务已经归新的持有者，写也只会被 fencing 拒绝。
func TestWorkerCancelsHandlerWhenLeaseIsLost(t *testing.T) {
	store := &fakeStore{batches: [][]ClaimedTask{{claimedTask(21, "t", 1, 3)}}}
	store.renewFn = func(context.Context, int, ClaimedTask) error {
		return fmt.Errorf("%w: task 21 is held by another worker now", ErrLeaseLost)
	}

	// Handler 与 process 跑在同一个 goroutine 上，因此这里用普通变量回传即可。
	var handlerCause error
	reg := NewRegistry()
	if err := reg.Register("t", func(hctx context.Context, _ json.RawMessage) error {
		select {
		case <-hctx.Done():
			handlerCause = context.Cause(hctx)
		case <-time.After(5 * time.Second):
			handlerCause = errors.New("handler was never cancelled after losing the lease")
		}
		return hctx.Err()
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	cfg := fastConfig()
	cfg.LeaseDuration = time.Hour
	cfg.HeartbeatInterval = time.Millisecond
	w, err := NewWorker(store, reg, cfg)
	if err != nil {
		t.Fatalf("NewWorker() unexpected error: %v", err)
	}

	var reported []error
	w.onError = func(err error) { reported = append(reported, err) }

	if _, err := w.runOnce(context.Background()); err != nil {
		t.Fatalf("runOnce() error: %v", err)
	}

	if !errors.Is(handlerCause, ErrLeaseLost) {
		t.Errorf("context.Cause seen by the handler = %v, want it to wrap ErrLeaseLost", handlerCause)
	}
	if n := len(store.succeededIDs()); n != 0 {
		t.Errorf("succeeded %d, want 0: a lease-holding worker cannot write results any more", n)
	}
	if n := len(store.retryCalls()); n != 0 {
		t.Errorf("retried %d time(s), want 0: 失租不是任务失败，任务已归别人", n)
	}
	if n := len(store.deadCalls()); n != 0 {
		t.Errorf("marked %d dead, want 0", n)
	}
	if len(reported) == 0 {
		t.Error("losing the lease must be reported; it is invisible otherwise")
	}
}

// TestWorkerKeepsRunningWhenRenewFailsWithInfrastructureError 固化"续租失败 ≠ 失租"
// （需求文档 §9）：连接抖动只说明这次没问成，租约可能仍然有效，绝不能据此取消
// Handler。若把驱动错误误判成失租，一次数据库抖动就会把所有在途任务半路掐掉。
func TestWorkerKeepsRunningWhenRenewFailsWithInfrastructureError(t *testing.T) {
	store := &fakeStore{batches: [][]ClaimedTask{{claimedTask(31, "t", 1, 3)}}}

	const transientFailures = 2
	recovered := make(chan struct{})
	var once sync.Once
	store.renewFn = func(_ context.Context, call int, _ ClaimedTask) error {
		if call > transientFailures {
			once.Do(func() { close(recovered) })
			return nil
		}
		return errors.New("driver: connection reset by peer")
	}

	reg := NewRegistry()
	if err := reg.Register("t", func(hctx context.Context, _ json.RawMessage) error {
		select {
		case <-recovered:
			return nil
		case <-hctx.Done():
			// 被取消就意味着有人把基础设施错误当成了失租。
			return fmt.Errorf("handler was cancelled even though the lease was never lost: %w", context.Cause(hctx))
		case <-time.After(10 * time.Second):
			return errors.New("lease renewals never recovered")
		}
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	cfg := fastConfig()
	cfg.LeaseDuration = time.Hour
	cfg.HeartbeatInterval = time.Millisecond
	w, err := NewWorker(store, reg, cfg)
	if err != nil {
		t.Fatalf("NewWorker() unexpected error: %v", err)
	}

	var reported []error
	w.onError = func(err error) { reported = append(reported, err) }

	if _, err := w.runOnce(context.Background()); err != nil {
		t.Fatalf("runOnce() error: %v", err)
	}

	// 被取消的 Handler 会返回错误并走重试路径，因此"任务成功"本身就证明它没被取消。
	if got := store.succeededIDs(); len(got) != 1 || got[0] != 31 {
		t.Errorf("succeeded = %v, want [31]: 续租的基础设施错误不该打断执行", got)
	}
	if n := len(store.retryCalls()); n != 0 {
		t.Errorf("retried %d time(s), want 0", n)
	}
	if len(store.renewCalls()) <= transientFailures {
		t.Errorf("renewed %d time(s), want more than %d: 失败之后必须继续重试",
			len(store.renewCalls()), transientFailures)
	}
	if len(reported) < transientFailures {
		t.Errorf("reported %d renew error(s), want at least %d: 续租失败必须被上报",
			len(reported), transientFailures)
	}
}

// TestWorkerStopsLeaseWatchBeforeWritingBack 固定 process 里的顺序：**先停看护、
// 等它退出，再写回**。
//
// 顺序反过来的后果不是"少一次续租"，而是凭空多出一类错误：任务写成终态之后到达的
// 续租会因为 status 不再是 running 而返回 ErrLeaseLost，把一个已经正常结束的任务
// 报成"失租"。这条用例把第一次续租挂在半途，制造出一个确定的"续租在途"窗口——
// 只要写回发生在窗口内，store 就会记下越界。
func TestWorkerStopsLeaseWatchBeforeWritingBack(t *testing.T) {
	store := &fakeStore{batches: [][]ClaimedTask{{claimedTask(51, "t", 1, 3)}}}

	renewStarted := make(chan struct{})
	releaseRenew := make(chan struct{})
	var once sync.Once
	store.renewFn = func(context.Context, int, ClaimedTask) error {
		once.Do(func() { close(renewStarted) })
		<-releaseRenew // 把这次续租挂在半途
		return nil
	}

	handlerReturned := make(chan struct{})
	reg := NewRegistry()
	if err := reg.Register("t", func(hctx context.Context, _ json.RawMessage) error {
		defer close(handlerReturned)
		select {
		case <-renewStarted:
			return nil
		case <-time.After(5 * time.Second):
			return errors.New("the lease watch never renewed")
		}
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	cfg := fastConfig()
	cfg.LeaseDuration = time.Hour
	cfg.HeartbeatInterval = time.Millisecond
	w, err := NewWorker(store, reg, cfg)
	if err != nil {
		t.Fatalf("NewWorker() unexpected error: %v", err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = w.runOnce(context.Background())
	}()

	// Handler 返回时那次续租必然仍在途（releaseRenew 还没关），因此接下来
	// 放行它——正确的实现此刻还停在"等看护退出"，错误的实现已经写回完了。
	select {
	case <-handlerReturned:
	case <-time.After(5 * time.Second):
		t.Fatal("handler never returned")
	}
	close(releaseRenew)

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("runOnce did not return")
	}

	if store.wroteBackWhileRenewing() {
		t.Error("a write-back overlapped an in-flight lease renewal: 写回前必须已经停掉租约看护")
	}
	if got := store.succeededIDs(); len(got) != 1 || got[0] != 51 {
		t.Errorf("succeeded = %v, want [51]", got)
	}
	if n := len(store.renewCalls()); n < 1 {
		t.Errorf("renewed %d time(s), want at least 1", n)
	}
}

// TestWorkerDoesNotReportRenewalsCutShortByShutdown 固定看护里一个容易搞反的细节：
// 停机打断的续租**不是失租**，也不该被上报成续租失败。
//
// 真驱动的续租语句会因为 ctx 已取消而直接失败，那看上去像一条基础设施错误，
// 实际只是停机的附带结果。把它当成失租会去取消一个本就在停机中的 Handler；
// 当成基础设施错误则会为每次停机刷出一条噪音。
func TestWorkerDoesNotReportRenewalsCutShortByShutdown(t *testing.T) {
	store := &fakeStore{batches: [][]ClaimedTask{{claimedTask(81, "t", 1, 3)}}}

	renewStarted := make(chan struct{})
	var once sync.Once
	store.renewFn = func(rctx context.Context, _ int, _ ClaimedTask) error {
		once.Do(func() { close(renewStarted) })
		// 像真驱动一样：语句卡在执行中，ctx 一取消就失败。
		<-rctx.Done()
		return rctx.Err()
	}

	reg := NewRegistry()
	if err := reg.Register("t", func(hctx context.Context, _ json.RawMessage) error {
		<-renewStarted // 先等续租确实卡住，再等停机取消
		<-hctx.Done()
		return hctx.Err()
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	cfg := fastConfig()
	cfg.LeaseDuration = time.Hour
	cfg.HeartbeatInterval = time.Millisecond
	w, err := NewWorker(store, reg, cfg)
	if err != nil {
		t.Fatalf("NewWorker() unexpected error: %v", err)
	}

	var mu sync.Mutex
	var reported []error
	w.onError = func(err error) {
		mu.Lock()
		defer mu.Unlock()
		reported = append(reported, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = w.runOnce(ctx)
	}()

	select {
	case <-renewStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("the lease watch never renewed")
	}
	cancel() // 停机：在途的那次续租会被取消

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("runOnce did not return after shutdown")
	}

	mu.Lock()
	defer mu.Unlock()

	shutdownReports := 0
	for _, err := range reported {
		if strings.Contains(err.Error(), "renew lease") {
			t.Errorf("a renewal cut short by shutdown was reported as a renewal failure: %v", err)
		}
		if strings.Contains(err.Error(), "shutdown in progress") {
			shutdownReports++
		}
	}
	// 反向确认这条用例不是空转：停机路径本身仍然必须上报。
	if shutdownReports == 0 {
		t.Errorf("expected the shutdown report to still happen, got %v", reported)
	}
	if n := len(store.succeededIDs()); n != 0 {
		t.Errorf("succeeded %d, want 0: 停机取消的任务不能写回", n)
	}
	if n := len(store.retryCalls()); n != 0 {
		t.Errorf("retried %d time(s), want 0", n)
	}
	if n := len(store.deadCalls()); n != 0 {
		t.Errorf("marked %d dead, want 0", n)
	}
}
