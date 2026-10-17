package mysql

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	gomysql "github.com/go-sql-driver/mysql"

	gorangetask "github.com/Tidehyc/gorangetask"
)

// 本文件覆盖 §6「故障注入：旧 worker 复苏」。
//
// 要验证的是：一个**仍然活着、但曾经被冻结过**的旧 Worker，在租约被回收、任务
// 已经被别人跑完之后醒过来，不能把它的结果覆盖回去。
//
// 为什么必须用子进程：进程被冻结时，它自己那条续租看护也一起停了——这正是
// "租约为什么会过期"的原因。在同一个进程里没法冻结一半的 goroutine，所以用
// `SIGSTOP` 冻结子进程，父进程继续跑。
//
// 为什么冻结点要可控：真实的冻结可以发生在任意时刻，但只有落在
// 「Handler 已成功返回、成功写回还没落到库里」这个窗口里，才能验证"旧执行者
// 尝试写回却被拒"。因此子进程的 store 在 Succeed 之前报到并阻塞，父进程看到
// 报到就 `SIGSTOP`；这等于把这个窗口人为固定在那一刻——注入的是**时间点**，
// 不是行为：阻塞放行之后执行的是真实的 `Succeed`，走真实 SQL 与真实的
// (owner, epoch, 租约未过期) 条件更新。

const (
	// revivalChildDSNEnv 是传给子进程的派生库 DSN。父进程用 -test.run 精确启动
	// 子进程，所以这个变量也是"我现在是子进程"的标记。
	revivalChildDSNEnv = "GOTASK_REVIVAL_CHILD_DSN"

	revivalQueue    = "q-revival"
	revivalTaskType = "revival.probe"
)

// TestRevivalHelperProcess 是上面那条用例的子进程入口，不是独立用例。
//
// 没有 DSN 环境变量时它**直接返回**而不是 Skip——正常 `go test ./...` 会把它跑成
// 一条空用例，而本项目把任何 `--- SKIP` 判为失败（跳过意味着"没验证却看起来像
// 通过"）。子进程真的失败时由 t.Fatalf 让退出码非零，父进程据此报错。
func TestRevivalHelperProcess(t *testing.T) {
	dsn := os.Getenv(revivalChildDSNEnv)
	if dsn == "" {
		return
	}
	runRevivalChild(t, dsn)
}

// runRevivalChild 在子进程里起一个 Worker，等它把任务做完并卡在写回之前。
func runRevivalChild(t *testing.T, dsn string) {
	t.Helper()

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("child: open database: %v", err)
	}
	defer db.Close()

	client, err := New(db, gorangetask.ClientConfig{})
	if err != nil {
		t.Fatalf("child: New: %v", err)
	}

	reg := gorangetask.NewRegistry()
	// Handler 立刻成功：父进程要的是"尽快到达写回"，事件时间线越短越可控。
	mustRegister(t, reg, revivalTaskType, func(context.Context, json.RawMessage) error {
		return nil
	})

	w, err := gorangetask.NewWorker(&gatedWriteBack{WorkerStore: client}, reg, gorangetask.Config{
		Queues:            []string{revivalQueue},
		Concurrency:       1,
		PollInterval:      5 * time.Millisecond,
		MaxPollInterval:   20 * time.Millisecond,
		BatchSize:         1,
		LeaseDuration:     2 * time.Second,
		HeartbeatInterval: 600 * time.Millisecond,
		// 冻结中的进程不该承担回收与清理职责：这个用例里"别人来接手"由父进程
		// 那个 Worker 负责，职责分开才能断言是它做的。
		ReapInterval:    -1,
		CleanupInterval: -1,
		HandlerTimeout:  5 * time.Second,
		// 必须大于"冻结时长 + 接手耗时"：成功写回的 context 带着这个预算，
		// 超了会变成 DeadlineExceeded，那样就测不出 fencing 了。
		ShutdownTimeout: 15 * time.Second,
		Logger:          childLineLogger{},
	})
	if err != nil {
		t.Fatalf("child: NewWorker: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()

	if err := w.Run(ctx); err != nil {
		t.Fatalf("child: worker Run: %v", err)
	}
}

// gatedWriteBack 只在 Succeed 上开一个观察点与一个门闸，其余方法原样转发。
//
// 这不是一个假 store：Claim / RenewLease / Retry / MarkDead / 回收 / 清理 全部
// 落到真实的 *Client，Succeed 最终也调用同一个实现。门闸只决定"什么时候执行
// 那条 UPDATE"，不改变它执行什么。
type gatedWriteBack struct {
	gorangetask.WorkerStore
}

// Succeed 报到、等父进程放行，再执行真实的写回。
//
// 行内打印的是本次执行的 fencing 凭据：父进程要拿它证明"被拒的确实是旧代次"。
func (g *gatedWriteBack) Succeed(ctx context.Context, task gorangetask.ClaimedTask) error {
	fmt.Printf("REVIVAL_ACK_ATTEMPTED epoch=%d owner=%s\n", task.Epoch, task.Owner)
	fmt.Println("REVIVAL_GATE_WAITING")

	// 阻塞在 stdin 上：进程被 SIGSTOP 期间这条读不会返回，父进程解冻前会先把
	// 一个字节写进管道，因此放行时刻完全由父进程决定。
	var one [1]byte
	if _, err := os.Stdin.Read(one[:]); err != nil {
		return fmt.Errorf("read resume gate: %w", err)
	}

	err := g.WorkerStore.Succeed(ctx, task)
	if err != nil {
		fmt.Printf("REVIVAL_ACK_REJECTED %v\n", err)
		return err
	}
	fmt.Println("REVIVAL_ACK_APPLIED")
	return nil
}

// childLineLogger 把日志逐行打到 stdout，父进程按行扫描。
//
// 刻意**不过滤级别**：父进程要断言"旧 Worker 从未记过 task succeeded"，把
// info 级滤掉这条断言就成了空断言。
type childLineLogger struct{}

func (childLineLogger) Log(_ context.Context, level gorangetask.Level, msg string, kv ...any) {
	var b strings.Builder
	fmt.Fprintf(&b, "REVIVAL_LOG level=%s msg=%s", level, msg)
	for i := 0; i+1 < len(kv); i += 2 {
		fmt.Fprintf(&b, " %v=%v", kv[i], kv[i+1])
	}
	fmt.Println(b.String())
}

// --- 父进程一侧 -------------------------------------------------------------

// TestRevivedWorkerCannotWriteBackAfterTakeover 是 §6「故障注入：旧 worker 复苏」
// 的端到端验收。
//
// 时间线：
//  1. 子进程 Worker A 领取任务，Handler 成功返回，卡在成功写回之前；
//  2. 父进程 SIGSTOP 冻结 A —— A 的续租看护随进程一起停摆，租约不再被延长；
//  3. 租约按配置自然过期（不手工改库），父进程里的 Worker B 的回收器把任务交还
//     队列，B 领取并**开始执行**，于是任务重新变成 running、attempt 与 lease_epoch
//     都前进了一代；
//  4. 父进程放行并 SIGCONT：A 用它手里那个**已经过期的 epoch** 执行成功写回；
//  5. 断言这条 UPDATE 被条件更新拒绝（ErrLeaseLost），且整行一个字节都没变。
//
// **判别点在于第 4 步执行时任务仍是 running**（B 还停在 Handler 里）。如果先让 B
// 把任务跑完，`WHERE status = 'running'` 这一条就足以拒掉 A 的写回，用例证明不了
// owner/epoch 这两个谓词有任何作用——那样它测的其实是"不能改写已完成的任务"。
// 这里刻意让新旧执行者**同时存在**，才逼出真正的问题：谁有权写。
func TestRevivedWorkerCannotWriteBackAfterTakeover(t *testing.T) {
	client, db := newTestClientAndDB(t, gorangetask.ClientConfig{})
	ctx := context.Background()

	// 子进程要连同一个派生库（夹具已经建好并迁移过）。
	childDSN := revivalChildDSN(t)

	id, err := client.Enqueue(ctx, gorangetask.Task{
		Queue:       revivalQueue,
		Type:        revivalTaskType,
		Payload:     json.RawMessage(`{"probe":true}`),
		MaxAttempts: 5,
	})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	child := startRevivalChild(t, childDSN)

	// --- 1) 等 A 走完 Handler，停在写回之前 --------------------------------
	attempted := child.waitForLine(t, "REVIVAL_ACK_ATTEMPTED", 30*time.Second)
	staleEpoch := mustIntField(t, attempted, "epoch")
	staleOwner := mustStringField(t, attempted, "owner")
	t.Logf("旧 Worker 停在写回之前：epoch=%d owner=%s", staleEpoch, staleOwner)

	// 此刻任务确实处于 running 且租约属于 A —— 否则后面的"易主"没有意义。
	if row := readTask(t, db, id); row.status != "running" {
		t.Fatalf("task status = %q before freeze, want running", row.status)
	}

	// --- 2) 冻结进程 ------------------------------------------------------
	child.freeze(t)

	// --- 3) 另一个 Worker 接手，并且**停在 Handler 里** --------------------
	holder := startReapingWorker(t, client, revivalQueue)
	holder.waitEntered(t, 30*time.Second)

	row := readTask(t, db, id)
	if row.status != "running" {
		t.Fatalf("task status = %q after takeover, want running（B 应当正持有它）", row.status)
	}
	if row.attempt != 2 {
		t.Fatalf("attempt = %d after takeover, want 2（旧 Worker 领过 1 次、接手者领过 1 次）", row.attempt)
	}
	wantEpoch := staleEpoch + 1
	if row.leaseEpoch != wantEpoch {
		t.Fatalf("lease_epoch = %d after takeover, want %d（每次领取递增，易主必须换代次）",
			row.leaseEpoch, wantEpoch)
	}
	if row.leaseOwner.String == staleOwner {
		t.Fatalf("lease_owner = %q after takeover, 仍是被冻结的旧 Worker", row.leaseOwner.String)
	}
	t.Logf("任务已被 %s 接手（epoch=%d），旧 Worker 手里是过期的 epoch=%d",
		row.leaseOwner.String, row.leaseEpoch, staleEpoch)

	// 接手者持有中的一整行，是"旧 Worker 绝不能改动"的基准。
	before := readTask(t, db, id)

	// --- 4) 解冻并放行那条陈旧的写回 --------------------------------------
	child.resume(t)

	rejected := child.waitForLine(t, "REVIVAL_ACK_REJECTED", 30*time.Second)
	if !strings.Contains(rejected, gorangetask.ErrLeaseLost.Error()) {
		t.Fatalf("旧 Worker 的写回失败原因 = %q, 期望包含 %q", rejected, gorangetask.ErrLeaseLost)
	}
	t.Logf("旧 Worker 的写回被拒：%s", rejected)

	// --- 5) 库里一行都没变，任务仍在接手者名下 ----------------------------
	after := readTask(t, db, id)
	if got, want := viewOf(after), viewOf(before); got != want {
		t.Fatalf("旧 Worker 醒来后改动任务行：\n got: %+v\nwant: %+v", got, want)
	}
	if after.status != "running" || after.leaseEpoch != wantEpoch {
		t.Fatalf("任务状态被旧 Worker 改成了 %q/epoch=%d, want running/epoch=%d",
			after.status, after.leaseEpoch, wantEpoch)
	}

	// --- 6) 旧 Worker 从未记过成功 ----------------------------------------
	// 与上一条互补：上一条看持久层终态，这条看调用方视角的行为。只看终态不够——
	// 终态也可能因为"错误的原因"而正确。
	if out := child.output(); strings.Contains(out, "msg=task succeeded") {
		t.Fatalf("旧 Worker 记了一条成功日志，说明写回被当成成功：\n%s", out)
	}
	// 反过来，它必须留下一条失租证据，否则"没写成功"可能只是因为它什么都没做。
	if out := child.output(); !strings.Contains(out, "msg=worker error") {
		t.Fatalf("旧 Worker 没有上报写回失败：\n%s", out)
	}

	// --- 7) 接手者照常收尾，旧 Worker 能干净退出 --------------------------
	holder.releaseHandler()
	final := waitForStatus(t, db, id, "succeeded", 30*time.Second)
	if final.attempt != 2 || final.leaseEpoch != wantEpoch {
		t.Fatalf("收尾后 attempt/epoch = %d/%d, want 2/%d", final.attempt, final.leaseEpoch, wantEpoch)
	}

	if err := child.terminate(10 * time.Second); err != nil {
		t.Fatalf("旧 Worker 退出异常: %v", err)
	}
}

// revivalChildDSN 复算子进程要连的派生库 DSN。
//
// 只换库名、只补 parseTime，其余（超时、loc=UTC）照抄环境变量——子进程连的必须
// 是同一个库，否则整条用例就退化成"两个不相干的 Worker"。
func revivalChildDSN(t *testing.T) string {
	t.Helper()

	raw := os.Getenv(testDSNEnv)
	if raw == "" {
		t.Skipf("%s not set; skipping MySQL integration test", testDSNEnv)
	}
	parsed, err := gomysql.ParseDSN(raw)
	if err != nil {
		t.Fatalf("parse %s: %v", testDSNEnv, err)
	}
	if parsed.DBName == "" {
		t.Fatalf("%s must include a database name", testDSNEnv)
	}
	parsed.DBName += testDBPrefix
	parsed.ParseTime = true
	return parsed.FormatDSN()
}

// holderWorker 是"接手者"：它在旧 Worker 被冻结期间领取任务，并停在 Handler 里
// 一直持有它——这样任务在旧 Worker 尝试写回时仍然是 running。
type holderWorker struct {
	entered chan struct{}
	release chan struct{}

	releaseOnce sync.Once
}

// waitEntered 等接手者的 Handler 真的开始执行。
//
// 这里的"真的开始"必须由 Handler 自己发信号，不能用"库里 status 变成 running"代替：
// claim 提交与 Handler 启动之间有窗口，用库状态判断会在窗口里误判。
func (h *holderWorker) waitEntered(t *testing.T, timeout time.Duration) {
	t.Helper()
	select {
	case <-h.entered:
	case <-time.After(timeout):
		t.Fatal("接手者的 Handler 始终没有开始执行")
	}
}

// releaseHandler 让接手者的 Handler 返回，任务随后正常成功。
func (h *holderWorker) releaseHandler() {
	h.releaseOnce.Do(func() { close(h.release) })
}

// startReapingWorker 起一个带回收器的 Worker，负责接手被冻结的旧 Worker 的任务。
func startReapingWorker(t *testing.T, c *Client, queue string) *holderWorker {
	t.Helper()

	h := &holderWorker{
		entered: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
	// 用例中途失败也要放行，否则 B 会一直阻塞到 t.Cleanup 的停机预算耗尽。
	t.Cleanup(h.releaseHandler)

	reg := gorangetask.NewRegistry()
	mustRegister(t, reg, revivalTaskType, func(ctx context.Context, _ json.RawMessage) error {
		select {
		case h.entered <- struct{}{}:
		default:
		}
		select {
		case <-h.release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	cfg := workerConfig(queue)
	cfg.LeaseDuration = 2 * time.Second
	cfg.HeartbeatInterval = 600 * time.Millisecond
	cfg.ReapInterval = 200 * time.Millisecond
	// 清理器与本用例无关；显式关掉，免得它也在同一张表上排队加锁。
	cfg.CleanupInterval = -1

	w, err := gorangetask.NewWorker(c, reg, cfg)
	if err != nil {
		t.Fatalf("NewWorker(B): %v", err)
	}

	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(runCtx) }()

	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Errorf("Worker B 未在停机预算内退出")
		}
	})
	return h
}

// --- 子进程句柄 -------------------------------------------------------------

// childProc 管一个子进程 + 它的 stdout 行缓冲。
type childProc struct {
	cmd   *exec.Cmd
	stdin io.WriteCloser

	mu    sync.Mutex
	lines []string

	stdoutDone chan struct{}
	waitOnce   sync.Once
	waitErr    error
}

func startRevivalChild(t *testing.T, dsn string) *childProc {
	t.Helper()

	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("locate test binary: %v", err)
	}
	// -test.run 精确匹配：子进程只跑入口函数，不会把父进程的其他用例再跑一遍。
	// -test.timeout 是兜底：子进程若卡住，它自己会带着堆栈退出，而不是拖死父进程。
	cmd := exec.Command(exe, "-test.run=^TestRevivalHelperProcess$", "-test.timeout=120s")
	cmd.Env = append(os.Environ(), revivalChildDSNEnv+"="+dsn)
	// 子进程的 t.Fatalf 输出直接透到父进程的测试输出里，失败原因不必二次转述。
	cmd.Stderr = os.Stderr

	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatalf("child stdin: %v", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("child stdout: %v", err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start child: %v", err)
	}

	c := &childProc{cmd: cmd, stdin: stdin, stdoutDone: make(chan struct{})}
	go func() {
		defer close(c.stdoutDone)
		sc := bufio.NewScanner(stdout)
		sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
		for sc.Scan() {
			c.mu.Lock()
			c.lines = append(c.lines, sc.Text())
			c.mu.Unlock()
		}
	}()

	// 无论用例怎么结束都不留一个被冻结的孤儿进程：SIGKILL 对已停止的进程同样有效。
	t.Cleanup(func() { c.kill() })
	return c
}

// freeze 用 SIGSTOP 冻结子进程：调度停止、续租看护停摆、租约不再被延长。
func (c *childProc) freeze(t *testing.T) {
	t.Helper()
	if err := c.cmd.Process.Signal(syscall.SIGSTOP); err != nil {
		t.Fatalf("SIGSTOP child: %v", err)
	}
}

// resume 先把放行字节写进管道（进程还在停着，字节先缓冲），再 SIGCONT。
//
// 顺序不能反：先 SIGCONT 的话，子进程可能在被写之前就返回去执行写回了——那样
// 冻结点就不在写回之前，测的又是别的东西。
func (c *childProc) resume(t *testing.T) {
	t.Helper()
	if _, err := c.stdin.Write([]byte{'\n'}); err != nil {
		t.Fatalf("write resume gate: %v", err)
	}
	if err := c.cmd.Process.Signal(syscall.SIGCONT); err != nil {
		t.Fatalf("SIGCONT child: %v", err)
	}
}

// waitForLine 等到出现包含 want 的一行并返回它。
func (c *childProc) waitForLine(t *testing.T, want string, timeout time.Duration) string {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for {
		c.mu.Lock()
		for _, line := range c.lines {
			if strings.Contains(line, want) {
				c.mu.Unlock()
				return line
			}
		}
		tail := strings.Join(c.lines, "\n")
		c.mu.Unlock()

		select {
		case <-c.stdoutDone:
			t.Fatalf("子进程在出现 %q 之前就结束了；输出：\n%s", want, tail)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("等待 %q 超时（%s）；已收到的输出：\n%s", want, timeout, tail)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// output 返回子进程到目前为止打印的全部内容。
func (c *childProc) output() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return strings.Join(c.lines, "\n")
}

// terminate 让子进程优雅停机（SIGTERM），返回它的退出错误。
//
// 已冻结的进程会把 SIGTERM 挂起直到被继续，所以这里先 SIGCONT 再等。
func (c *childProc) terminate(timeout time.Duration) error {
	if err := c.cmd.Process.Signal(syscall.SIGCONT); err != nil {
		return fmt.Errorf("SIGCONT: %w", err)
	}
	if err := c.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		return fmt.Errorf("SIGTERM: %w", err)
	}
	select {
	case <-c.stdoutDone:
	case <-time.After(timeout):
		_ = c.cmd.Process.Kill()
		<-c.stdoutDone
		return fmt.Errorf("子进程未在 %s 内响应 SIGTERM，已强杀", timeout)
	}
	return c.wait()
}

// kill 强杀并回收，供 Cleanup 使用（可重复调用）。
func (c *childProc) kill() {
	if c.cmd.ProcessState != nil {
		return
	}
	_ = c.cmd.Process.Kill()
	<-c.stdoutDone
	_ = c.wait()
}

func (c *childProc) wait() error {
	c.waitOnce.Do(func() { c.waitErr = c.cmd.Wait() })
	return c.waitErr
}

// mustIntField 从 "REVIVAL_ACK_ATTEMPTED epoch=3 owner=x" 这样的行里取出某个
// 整数 key 的值。
func mustIntField(t *testing.T, line, key string) int64 {
	t.Helper()

	raw := mustStringField(t, line, key)
	var v int64
	if _, err := fmt.Sscan(raw, &v); err != nil {
		t.Fatalf("parse %s=%q in %q: %v", key, raw, line, err)
	}
	return v
}

// mustStringField 取出某个 key 的原始字符串值。
func mustStringField(t *testing.T, line, key string) string {
	t.Helper()

	for _, f := range strings.Fields(line) {
		if raw, ok := strings.CutPrefix(f, key+"="); ok {
			return raw
		}
	}
	t.Fatalf("no %s= field in %q", key, line)
	return ""
}

// taskStateView 是"整行快照"的可比较投影。
//
// 用逐字段投影而不是直接比结构体：taskRow 里有 time.Time 与 sql.Null*，
// 直接比较要么依赖内部字段（DeepEqual 会被 Location 指针之类绊住），要么漏掉
// 可空字段的 Valid 状态。这里把每个字段都显式归一成一个字符串。
type taskStateView struct {
	status     string
	payload    string
	attempt    int64
	leaseEpoch int64
	leaseOwner string
	leaseUntil string
	lastError  string
	runAt      string
	createdAt  string
	updatedAt  string
	finishedAt string
}

func viewOf(r taskRow) taskStateView {
	nullString := func(s sql.NullString) string {
		if !s.Valid {
			return "<NULL>"
		}
		return s.String
	}
	nullTime := func(v sql.NullTime) string {
		if !v.Valid {
			return "<NULL>"
		}
		return v.Time.UTC().Format(time.RFC3339Nano)
	}
	return taskStateView{
		status:     r.status,
		payload:    r.payload,
		attempt:    r.attempt,
		leaseEpoch: r.leaseEpoch,
		leaseOwner: nullString(r.leaseOwner),
		leaseUntil: nullTime(r.leaseUntil),
		lastError:  nullString(r.lastError),
		runAt:      r.runAt.UTC().Format(time.RFC3339Nano),
		createdAt:  r.createdAt.UTC().Format(time.RFC3339Nano),
		updatedAt:  r.updatedAt.UTC().Format(time.RFC3339Nano),
		finishedAt: nullTime(r.finishedAt),
	}
}
