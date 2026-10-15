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

	// reapLimits 记录回收器的每次调用（存 limit）。回收的语义验证放在
	// store/mysql 的真库用例里；根包这一层只需要一个能观察"回收协程有没有按
	// 节奏跑起来、有没有被配置关掉、出错后还在不在"的缝隙。
	reapLimits []int
	reapErr    error

	// onReap 在记录完一次回收调用后执行，供用例做同步点，或制造"回收恰好在
	// 停机瞬间失败"这类只在特定时序下出现的路径。
	onReap func(ctx context.Context, call int, limit int)

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

// ReapExpiredLeases 记录一次回收调用，并调用 onReap 供用例同步。
// 回收的返回计数在这里恒为零值：根包并不解释它，只是把它交给调用方。
func (f *fakeStore) ReapExpiredLeases(ctx context.Context, limit int) (ReapResult, error) {
	if err := f.ctxErr(ctx); err != nil {
		return ReapResult{}, err
	}

	f.mu.Lock()
	f.reapLimits = append(f.reapLimits, limit)
	call := len(f.reapLimits)
	hook, err := f.onReap, f.reapErr
	f.mu.Unlock()

	if hook != nil {
		hook(ctx, call, limit)
	}
	return ReapResult{}, err
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

// reaped 返回回收器每次调用时传下去的 limit。
func (f *fakeStore) reaped() []int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]int(nil), f.reapLimits...)
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

	cfg := fastConfig()
	cfg.RetryBaseDelay = 40 * time.Millisecond
	cfg.RetryMaxDelay = 640 * time.Millisecond

	w, err := NewWorker(store, reg, cfg)
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
	// attempt 1 的上界就是 base。断言区间而不是具体值：延迟带抖动，但
	// 两端由 retryDelay 的定义决定，因此这条断言是确定性的。
	cap := retryBackoffCap(1, cfg.RetryBaseDelay, cfg.RetryMaxDelay)
	if retries[0].delay < cap/2 || retries[0].delay > cap {
		t.Errorf("retry delay = %s, want within [%s, %s] (cap = base×2^0)",
			retries[0].delay, cap/2, cap)
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

// TestWorkerUnknownTypeGoesStraightToDead 固化需求文档 §4.2 的一条硬要求：
// "找不到 Handler 或无法解码 payload 的任务不能无限快速重试"。
//
// Registry.Resolve 返回的错误**带永久标记**（Lookup 刻意不带），因此这里不需要
// Worker 单独识别任务类型——它只是尊重标记。用例同时断言 Retry **零调用**：
// 只看 dead 终态是不够的，"先重试一次再 dead"和"直接 dead"在终态上无法区分。
func TestWorkerUnknownTypeGoesStraightToDead(t *testing.T) {
	store := &fakeStore{batches: [][]ClaimedTask{{claimedTask(17, "missing", 1, 5)}}}
	w, err := NewWorker(store, newRegistryWith(t, "t"), fastConfig())
	if err != nil {
		t.Fatalf("NewWorker() unexpected error: %v", err)
	}
	if _, err := w.runOnce(context.Background()); err != nil {
		t.Fatalf("runOnce() error: %v", err)
	}

	if retries := store.retryCalls(); len(retries) != 0 {
		t.Fatalf("Retry called %d times, want 0: 未知 task type 是永久失败，不该消耗剩余尝试次数", len(retries))
	}
	dead := store.deadCalls()
	if len(dead) != 1 {
		t.Fatalf("MarkDead called %d times, want 1", len(dead))
	}
	if !strings.Contains(dead[0].lastError, "no handler") {
		t.Errorf("lastError = %q, want it to mention the missing handler", dead[0].lastError)
	}
	// 永久失败的摘要带前缀，运维在库里能分辨"不可能成功"与"次数用完了"。
	if !strings.HasPrefix(dead[0].lastError, "permanent failure:") {
		t.Errorf("lastError = %q, want it to start with the permanent marker", dead[0].lastError)
	}
}

// TestWorkerInvalidPayloadGoesStraightToDead 覆盖另一条永久失败来源：
// TypedHandler 的 payload 解码失败（返回永久 ErrInvalidPayload）。
//
// 它和第 1 次尝试就 dead 是同一件事，但走的是完全不同的代码路径
// （handler.go 而不是 registry），所以分开断言。
func TestWorkerInvalidPayloadGoesStraightToDead(t *testing.T) {
	task := claimedTask(21, "typed", 1, 5)
	// TypedHandler 期望一个对象，这里给一个解不开的 payload。
	task.Payload = json.RawMessage(`"not an object"`)
	store := &fakeStore{batches: [][]ClaimedTask{{task}}}
	reg := NewRegistry()
	var ran int
	if err := reg.Register("typed", TypedHandler(func(context.Context, struct{ N int }) error {
		ran++
		return nil
	})); err != nil {
		t.Fatalf("Register: %v", err)
	}

	w, err := NewWorker(store, reg, fastConfig())
	if err != nil {
		t.Fatalf("NewWorker() unexpected error: %v", err)
	}
	if _, err := w.runOnce(context.Background()); err != nil {
		t.Fatalf("runOnce() error: %v", err)
	}

	if ran != 0 {
		t.Errorf("typed handler body ran %d times, want 0: 解码失败时不该进入函数体", ran)
	}
	if retries := store.retryCalls(); len(retries) != 0 {
		t.Fatalf("Retry called %d times, want 0: payload 解码失败是永久失败", len(retries))
	}
	dead := store.deadCalls()
	if len(dead) != 1 {
		t.Fatalf("MarkDead called %d times, want 1", len(dead))
	}
	if !strings.Contains(dead[0].lastError, "invalid payload") {
		t.Errorf("lastError = %q, want it to mention the invalid payload", dead[0].lastError)
	}
}

// TestWorkerFailureOutcomeMatrix 把 fail 的**判定规则**一次钉完：去向只由两件事
// 决定——错误有没有永久标记，以及尝试次数还剩不剩；摘要文本只由错误自己决定。
//
// 之所以做成一张表：这四条本来就该互相印证。写这条用例时先按"先判永久、后判耗尽，
// 顺序会影响摘要"的假设写过一版，**证伪检查当场否掉了它**——把永久分支短路后那条
// 用例照样通过，因为 "permanent failure: " 前缀来自 Permanent 包装器的 Error()，
// 与走哪条分支无关。顺序在结果上不可观察，所以这里只断言可观察的东西。
func TestWorkerFailureOutcomeMatrix(t *testing.T) {
	tests := []struct {
		name        string
		permanent   bool
		attempt     int
		maxAttempts int
		wantRetry   bool
		wantDead    bool
		wantPrefix  bool
	}{
		{"可重试且还有次数 → 重试", false, 1, 3, true, false, false},
		{"可重试但次数耗尽 → dead", false, 3, 3, false, true, false},
		{"永久失败且还有次数 → dead", true, 1, 9, false, true, true},
		{"永久失败且正好最后一次 → dead", true, 3, 3, false, true, true},
	}

	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			id := TaskID(30 + i)
			store := &fakeStore{batches: [][]ClaimedTask{{claimedTask(id, "t", tc.attempt, tc.maxAttempts)}}}

			reg := NewRegistry()
			if err := reg.Register("t", func(context.Context, json.RawMessage) error {
				err := errors.New("boom")
				if tc.permanent {
					return Permanent(err)
				}
				return err
			}); err != nil {
				t.Fatalf("Register: %v", err)
			}

			cfg := fastConfig()
			cfg.RetryBaseDelay = 40 * time.Millisecond
			cfg.RetryMaxDelay = 640 * time.Millisecond

			w, err := NewWorker(store, reg, cfg)
			if err != nil {
				t.Fatalf("NewWorker() unexpected error: %v", err)
			}
			if _, err := w.runOnce(context.Background()); err != nil {
				t.Fatalf("runOnce() error: %v", err)
			}

			retries, dead := store.retryCalls(), store.deadCalls()
			if gotRetry := len(retries) == 1; gotRetry != tc.wantRetry {
				t.Errorf("Retry called = %v (%d 次), want %v", gotRetry, len(retries), tc.wantRetry)
			}
			if gotDead := len(dead) == 1; gotDead != tc.wantDead {
				t.Errorf("MarkDead called = %v (%d 次), want %v", gotDead, len(dead), tc.wantDead)
			}
			// 同一条失败不该既重试又进死信。
			if len(retries) > 0 && len(dead) > 0 {
				t.Error("同一次失败同时出现了 Retry 与 MarkDead")
			}

			summary := ""
			switch {
			case len(retries) == 1:
				summary = retries[0].lastError
				// 重试必须带退避：上界是 base×2^(attempt-1)。
				cap := retryBackoffCap(tc.attempt, cfg.RetryBaseDelay, cfg.RetryMaxDelay)
				if retries[0].delay < cap/2 || retries[0].delay > cap {
					t.Errorf("retry delay = %s, want within [%s, %s]",
						retries[0].delay, cap/2, cap)
				}
			case len(dead) == 1:
				summary = dead[0].lastError
			default:
				t.Fatal("既没有 Retry 也没有 MarkDead")
			}

			if !strings.Contains(summary, "boom") {
				t.Errorf("lastError = %q, want the original error text", summary)
			}
			if gotPrefix := strings.HasPrefix(summary, "permanent failure:"); gotPrefix != tc.wantPrefix {
				t.Errorf("lastError = %q, permanent 前缀 = %v, want %v", summary, gotPrefix, tc.wantPrefix)
			}
		})
	}
}

// TestWorkerRetrybackoffCapIsExponentialAndCapped 固定退避上界的**纯函数**。
//
// 之所以把它抽出来单独测：延迟本身带抖动，只能断言区间；而"按 2 的幂增长"和
// "封顶"这两件事一旦写错（例如溢出成负数，等价于没有退避）在区间断言里看不出来。
func TestWorkerRetrybackoffCapIsExponentialAndCapped(t *testing.T) {
	const (
		base = 100 * time.Millisecond
		max  = 1600 * time.Millisecond
	)

	tests := []struct {
		name    string
		attempt int
		base    time.Duration
		max     time.Duration
		want    time.Duration
	}{
		{"attempt 0 按第 1 次算", 0, base, max, base},
		{"attempt 负数按第 1 次算", -3, base, max, base},
		{"第 1 次 = base", 1, base, max, base},
		{"第 2 次翻倍", 2, base, max, 2 * base},
		{"第 3 次再翻倍", 3, base, max, 4 * base},
		{"第 4 次到上界", 4, base, max, 8 * base},
		{"第 5 次封顶", 5, base, max, max},
		{"远超上界也不溢出", 1 << 20, base, max, max},
		{"base 已超过 max 时直接封顶", 1, 4 * time.Second, max, max},
		{"max 为 0 时不做无意义放大", 3, base, 0, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := retryBackoffCap(tc.attempt, tc.base, tc.max); got != tc.want {
				t.Errorf("retryBackoffCap(%d, %s, %s) = %s, want %s",
					tc.attempt, tc.base, tc.max, got, tc.want)
			}
		})
	}
}

// TestWorkerRetryDelayStaysWithinItsBackoffCap 断言抖动后的延迟落在
// [cap/2, cap] 里，且这个区间随 attempt 逐级抬高、最终被 RetryMaxDelay 卡住。
//
// 区间两端都由定义决定，因此多次抽样不会 flaky；而"每次抽样都在区间内"同时
// 排除了两类错误实现：没有退避（整个区间会贴着 0）与没有上限（上端会越过 max）。
func TestWorkerRetryDelayStaysWithinItsBackoffCap(t *testing.T) {
	const (
		base = 20 * time.Millisecond
		max  = 160 * time.Millisecond
	)
	w := &Worker{cfg: Config{RetryBaseDelay: base, RetryMaxDelay: max}}

	for _, attempt := range []int{1, 2, 3, 4, 5, 6} {
		cap := retryBackoffCap(attempt, base, max)
		for i := 0; i < 100; i++ {
			got := w.retryDelay(attempt)
			if got < cap/2 || got > cap {
				t.Fatalf("retryDelay(%d) = %s, want within [%s, %s]", attempt, got, cap/2, cap)
			}
		}
	}

	// 区间确实在往上走，而不是常数。
	if retryBackoffCap(1, base, max) >= retryBackoffCap(2, base, max) {
		t.Error("退避上界没有随尝试次数增长")
	}
	// 并且确实被封住。
	if got := retryBackoffCap(50, base, max); got != max {
		t.Errorf("retryBackoffCap(50) = %s, want capped at %s", got, max)
	}
	if got := w.retryDelay(1 << 20); got > max {
		t.Errorf("retryDelay(超出上界的次数) = %s, want <= %s", got, max)
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

// waitUntil 轮询 cond 直到成立或超时。
//
// 回收器由定时器驱动，断言"它已经跑过至少 n 轮"时不能赌某一次调度的时序，
// 轮询可以让用例在快机器上立刻通过、在慢机器上也不假失败。
func waitUntil(t *testing.T, timeout time.Duration, msg string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal(msg)
}

// waitRun 等待 Run 返回。超时即判失败，避免用例挂死（挂死只能靠
// go test -timeout 兜底，排查成本高得多）。
func waitRun(t *testing.T, runDone <-chan error) error {
	t.Helper()
	select {
	case err := <-runDone:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("Run() did not return")
		return nil
	}
}

// TestWorkerReapsOnConfiguredInterval 证明回收协程真的按 ReapInterval 跑起来了，
// 且以 reapBatchSize 为批量上限。"起了个协程"如果只写在注释里，是没法被检验的。
func TestWorkerReapsOnConfiguredInterval(t *testing.T) {
	store := &fakeStore{}
	cfg := fastConfig()
	cfg.ReapInterval = 5 * time.Millisecond

	w, err := NewWorker(store, newRegistryWith(t, "t"), cfg)
	if err != nil {
		t.Fatalf("NewWorker() unexpected error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runDone := make(chan error, 1)
	go func() { runDone <- w.Run(ctx) }()

	// 等到第二轮：只等到一轮无法区分"按周期跑"和"启动时跑了一次"。
	waitUntil(t, 5*time.Second, "the reaper never ran", func() bool {
		return len(store.reaped()) >= 2
	})

	cancel()
	if err := waitRun(t, runDone); err != nil {
		t.Fatalf("Run() = %v, want nil after a clean shutdown", err)
	}

	for _, limit := range store.reaped() {
		if limit != reapBatchSize {
			t.Errorf("reap limit = %d, want reapBatchSize (%d)", limit, reapBatchSize)
		}
	}
}

// TestWorkerReaperKeepsGoingAfterInfrastructureErrors 固定"回收能力不能被一次
// 数据库抖动关掉"这条约定——它正是停机遗留任务唯一的下家。
func TestWorkerReaperKeepsGoingAfterInfrastructureErrors(t *testing.T) {
	store := &fakeStore{reapErr: errors.New("reap exploded")}
	cfg := fastConfig()
	cfg.ReapInterval = 5 * time.Millisecond

	w, err := NewWorker(store, newRegistryWith(t, "t"), cfg)
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
	defer cancel()
	runDone := make(chan error, 1)
	go func() { runDone <- w.Run(ctx) }()

	// 等到第二次：失败之后还能来第二次，才说明循环没被这次错误终结。
	waitUntil(t, 5*time.Second, "the reaper stopped after an infrastructure error", func() bool {
		return len(store.reaped()) >= 2
	})

	cancel()
	if err := waitRun(t, runDone); err != nil {
		t.Fatalf("Run() = %v, want nil", err)
	}

	mu.Lock()
	defer mu.Unlock()
	for _, err := range reported {
		if strings.Contains(err.Error(), "reap expired leases") {
			return
		}
	}
	t.Errorf("the reap failure was never reported, got %v", reported)
}

// TestWorkerReaperCanBeDisabled 对照验证 ReapInterval 的负值确实关掉了回收。
//
// 只断言"没跑"是不够的——那也可能是协程起来了但还没来得及跑。所以两个子用例共用
// 同一个时间窗，让"正间隔至少跑了两轮"给"负间隔一轮都没跑"提供参照。
func TestWorkerReaperCanBeDisabled(t *testing.T) {
	const (
		interval = 5 * time.Millisecond
		window   = 60 * time.Millisecond
	)

	countReaps := func(t *testing.T, reapInterval time.Duration) int {
		t.Helper()
		store := &fakeStore{}
		cfg := fastConfig()
		cfg.ReapInterval = reapInterval

		w, err := NewWorker(store, newRegistryWith(t, "t"), cfg)
		if err != nil {
			t.Fatalf("NewWorker() unexpected error: %v", err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		runDone := make(chan error, 1)
		go func() { runDone <- w.Run(ctx) }()

		time.Sleep(window)
		cancel()
		if err := waitRun(t, runDone); err != nil {
			t.Fatalf("Run() = %v, want nil", err)
		}
		return len(store.reaped())
	}

	if enabled := countReaps(t, interval); enabled < 2 {
		t.Fatalf("reaped %d time(s) in %s with a %s interval; the reaper is not running",
			enabled, window, interval)
	}
	if disabled := countReaps(t, -1); disabled != 0 {
		t.Errorf("reaped %d time(s) with ReapInterval = -1, want 0", disabled)
	}
}

// TestWorkerShutdownTimeoutBlamesTheReaper 覆盖"Handler 都退了、只有回收器没停"。
// 超时消息必须说清是哪一边没停，否则排查时会被误导到 Handler 上。
func TestWorkerShutdownTimeoutBlamesTheReaper(t *testing.T) {
	reapEntered := make(chan struct{}, 1)
	releaseReap := make(chan struct{})
	t.Cleanup(func() { close(releaseReap) })

	store := &fakeStore{}
	store.onReap = func(context.Context, int, int) {
		select {
		case reapEntered <- struct{}{}:
		default:
		}
		<-releaseReap // 故意无视 ctx 的回收调用
	}

	cfg := fastConfig()
	cfg.ReapInterval = 5 * time.Millisecond
	cfg.ShutdownTimeout = 50 * time.Millisecond

	w, err := NewWorker(store, newRegistryWith(t, "t"), cfg)
	if err != nil {
		t.Fatalf("NewWorker() unexpected error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- w.Run(ctx) }()

	select {
	case <-reapEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("the reaper never entered a reap call")
	}
	cancel()

	select {
	case err := <-runDone:
		if !errors.Is(err, ErrShutdownTimeout) {
			t.Fatalf("Run() = %v, want ErrShutdownTimeout", err)
		}
		if !strings.Contains(err.Error(), "the reaper not stopped") {
			t.Errorf("Run() error %q should blame the reaper, not the handlers", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run() did not return")
	}
}

// TestWorkerShutdownTimeoutBlamesBothSides 覆盖"Handler 与回收器都没停"：
// 这是停机超时里最难查的一种，消息必须把两边都点出来。
func TestWorkerShutdownTimeoutBlamesBothSides(t *testing.T) {
	handlerEntered := make(chan struct{}, 1)
	reapEntered := make(chan struct{}, 1)
	releaseHandler := make(chan struct{})
	releaseReap := make(chan struct{})
	t.Cleanup(func() { close(releaseHandler); close(releaseReap) })

	store := &fakeStore{batches: [][]ClaimedTask{{claimedTask(61, "t", 1, 3)}}}
	store.onReap = func(context.Context, int, int) {
		select {
		case reapEntered <- struct{}{}:
		default:
		}
		<-releaseReap
	}

	reg := NewRegistry()
	if err := reg.Register("t", func(context.Context, json.RawMessage) error {
		select {
		case handlerEntered <- struct{}{}:
		default:
		}
		<-releaseHandler // 无视 ctx 的 Handler
		return errors.New("released after the worker gave up")
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	cfg := fastConfig()
	cfg.ReapInterval = 5 * time.Millisecond
	cfg.ShutdownTimeout = 50 * time.Millisecond

	w, err := NewWorker(store, reg, cfg)
	if err != nil {
		t.Fatalf("NewWorker() unexpected error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- w.Run(ctx) }()

	for name, ch := range map[string]<-chan struct{}{"handler": handlerEntered, "reaper": reapEntered} {
		select {
		case <-ch:
		case <-time.After(5 * time.Second):
			t.Fatalf("%s never started", name)
		}
	}
	cancel()

	select {
	case err := <-runDone:
		if !errors.Is(err, ErrShutdownTimeout) {
			t.Fatalf("Run() = %v, want ErrShutdownTimeout", err)
		}
		if !strings.Contains(err.Error(), "handlers still running and the reaper not stopped") {
			t.Errorf("Run() error %q should blame both sides", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run() did not return")
	}
}

// TestWorkerDoesNotReportReapFailuresCausedByShutdown 覆盖"回收恰好在停机瞬间
// 失败"：驱动会因为 ctx 已取消而报错，那看上去像一条基础设施故障，实际只是停机
// 的附带结果，不该被上报（与 watchLease 对续租的处理一致）。
func TestWorkerDoesNotReportReapFailuresCausedByShutdown(t *testing.T) {
	store := &fakeStore{reapErr: errors.New("reap exploded")}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// 让回收在"ctx 刚好被取消"的瞬间失败，这正是停机打断基础设施调用的样子。
	store.onReap = func(context.Context, int, int) { cancel() }

	cfg := fastConfig()
	cfg.ReapInterval = 5 * time.Millisecond

	w, err := NewWorker(store, newRegistryWith(t, "t"), cfg)
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

	runDone := make(chan error, 1)
	go func() { runDone <- w.Run(ctx) }()

	if err := waitRun(t, runDone); err != nil {
		t.Fatalf("Run() = %v, want nil", err)
	}

	// 反向确认这条用例不是空转：回收确实被调用过。
	if n := len(store.reaped()); n == 0 {
		t.Fatal("the reaper never ran; this case would pass vacuously")
	}

	mu.Lock()
	defer mu.Unlock()
	for _, err := range reported {
		if strings.Contains(err.Error(), "reap expired leases") {
			t.Errorf("a reap failure caused by shutdown was reported as infrastructure trouble: %v", err)
		}
	}
}

// --- Handler panic 兜底（§4 第 5 项） ---------------------------------------

// TestRunHandlerConvertsPanicToError 钉住 panic 边界本身：正常返回不动、
// 普通错误原样穿透、panic 转成错误且带上 panic 值与堆栈。
func TestRunHandlerConvertsPanicToError(t *testing.T) {
	t.Run("正常返回不改写", func(t *testing.T) {
		err := runHandler(func(context.Context, json.RawMessage) error { return nil },
			context.Background(), nil)
		if err != nil {
			t.Errorf("runHandler() = %v, want nil", err)
		}
	})

	t.Run("普通错误原样穿透", func(t *testing.T) {
		sentinel := errors.New("plain failure")
		err := runHandler(func(context.Context, json.RawMessage) error { return sentinel },
			context.Background(), nil)
		if !errors.Is(err, sentinel) {
			t.Errorf("runHandler() = %v, want the handler error to pass through unchanged", err)
		}
		if strings.Contains(err.Error(), "panicked") {
			t.Errorf("runHandler() = %v, want no panic wording on an ordinary error", err)
		}
	})

	t.Run("panic 转成带值与堆栈的错误", func(t *testing.T) {
		err := runHandler(func(context.Context, json.RawMessage) error {
			panic("boom: nil map write")
		}, context.Background(), nil)

		if err == nil {
			t.Fatal("runHandler() = nil, want an error: 否则 panic 会一路冒到 Run 把进程带走")
		}
		if !strings.Contains(err.Error(), "handler panicked") {
			t.Errorf("error = %q, want it to say the handler panicked", err)
		}
		if !strings.Contains(err.Error(), "boom: nil map write") {
			t.Errorf("error = %q, want the recovered value kept", err)
		}
		// 堆栈必须能指回发起 panic 的那一帧，否则这段堆栈等于没写。
		if !strings.Contains(err.Error(), "worker_test.go") {
			t.Errorf("error = %q, want the stack to point back at the panicking frame", err)
		}
		if IsPermanent(err) {
			t.Error("panic 不该带永久标记：需求只说「视为一次失败」，去留交给 max_attempts")
		}
	})
}

// TestWorkerRetriesHandlerPanic 把 panic 兜底接到失败路径上：它必须与普通失败
// 走**完全一样**的分流（还有次数就退避重试），既不单独处理，也不让 Worker 崩掉。
//
// "没崩掉"这件事无法直接断言——真让 panic 冒出去，测试进程会在 `runOnce` 那行
// 之前就终止、连失败都打不出来。所以这条用例**跑完本身就是证据**，另外再用
// Retry 的调用记录把"走的是重试"钉住。
func TestWorkerRetriesHandlerPanic(t *testing.T) {
	store := &fakeStore{batches: [][]ClaimedTask{{claimedTask(41, "t", 1, 3)}}}

	reg := NewRegistry()
	if err := reg.Register("t", func(context.Context, json.RawMessage) error {
		panic("handler exploded")
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	cfg := fastConfig()
	cfg.RetryBaseDelay = 40 * time.Millisecond
	cfg.RetryMaxDelay = 640 * time.Millisecond

	w, err := NewWorker(store, reg, cfg)
	if err != nil {
		t.Fatalf("NewWorker() unexpected error: %v", err)
	}
	if _, err := w.runOnce(context.Background()); err != nil {
		t.Fatalf("runOnce() error: %v", err)
	}

	retries := store.retryCalls()
	if len(retries) != 1 {
		t.Fatalf("Retry called %d times, want 1: panic 应视为一次失败", len(retries))
	}
	if len(store.deadCalls()) != 0 {
		t.Error("panic 不该直接进死信：还有剩余尝试次数")
	}
	if !strings.Contains(retries[0].lastError, "handler panicked") {
		t.Errorf("lastError = %q, want the panic wording", retries[0].lastError)
	}
	if !strings.Contains(retries[0].lastError, "handler exploded") {
		t.Errorf("lastError = %q, want the recovered value kept", retries[0].lastError)
	}
	// 走的是同一套退避，不是"panic 专属"的另一条路径。
	cap := retryBackoffCap(1, cfg.RetryBaseDelay, cfg.RetryMaxDelay)
	if retries[0].delay < cap/2 || retries[0].delay > cap {
		t.Errorf("retry delay = %s, want within [%s, %s]", retries[0].delay, cap/2, cap)
	}
}

// TestWorkerHandlerPanicGoesDeadWhenAttemptsExhausted 是 panic 的另一端：
// 次数耗尽时和普通失败一样进死信，摘要仍保留 panic 值与堆栈。
func TestWorkerHandlerPanicGoesDeadWhenAttemptsExhausted(t *testing.T) {
	store := &fakeStore{batches: [][]ClaimedTask{{claimedTask(42, "t", 3, 3)}}}

	reg := NewRegistry()
	if err := reg.Register("t", func(context.Context, json.RawMessage) error {
		panic("no luck on the last attempt")
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

	if retries := store.retryCalls(); len(retries) != 0 {
		t.Fatalf("Retry called %d times, want 0: 次数已耗尽", len(retries))
	}
	dead := store.deadCalls()
	if len(dead) != 1 {
		t.Fatalf("MarkDead called %d times, want 1", len(dead))
	}
	if !strings.Contains(dead[0].lastError, "handler panicked") {
		t.Errorf("lastError = %q, want the panic wording", dead[0].lastError)
	}
	if !strings.Contains(dead[0].lastError, "no luck on the last attempt") {
		t.Errorf("lastError = %q, want the recovered value kept", dead[0].lastError)
	}
	// panic 与"永久失败"是两码事：库里必须能分辨，否则运维会误判成不可重试的配置问题。
	if strings.HasPrefix(dead[0].lastError, "permanent failure:") {
		t.Errorf("lastError = %q, want no permanent marker on a panic", dead[0].lastError)
	}
}

// TestWorkerKeepsProcessingBatchAfterHandlerPanic 守住"兜底是单任务边界"这句话：
// panic 只终结发生它的那一条任务，批里后面的任务照常执行。
func TestWorkerKeepsProcessingBatchAfterHandlerPanic(t *testing.T) {
	store := &fakeStore{batches: [][]ClaimedTask{{
		claimedTask(51, "boom", 1, 3),
		claimedTask(52, "ok", 1, 3),
	}}}

	reg := NewRegistry()
	if err := reg.Register("boom", func(context.Context, json.RawMessage) error {
		panic("first task exploded")
	}); err != nil {
		t.Fatalf("Register(boom): %v", err)
	}
	var okRan int
	if err := reg.Register("ok", func(context.Context, json.RawMessage) error {
		okRan++
		return nil
	}); err != nil {
		t.Fatalf("Register(ok): %v", err)
	}

	w, err := NewWorker(store, reg, fastConfig())
	if err != nil {
		t.Fatalf("NewWorker() unexpected error: %v", err)
	}
	if _, err := w.runOnce(context.Background()); err != nil {
		t.Fatalf("runOnce() error: %v", err)
	}

	if okRan != 1 {
		t.Errorf("second handler ran %d times, want 1: 一条任务 panic 不该中断整批", okRan)
	}
	if got := store.succeededIDs(); len(got) != 1 || got[0] != 52 {
		t.Errorf("succeeded = %v, want [52]", got)
	}
	retries := store.retryCalls()
	if len(retries) != 1 || retries[0].task.ID != 51 {
		t.Errorf("retries = %v, want exactly task 51", retries)
	}
}
