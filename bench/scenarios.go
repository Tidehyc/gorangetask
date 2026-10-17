package main

// 本文件放三个"回答一个具体问题"的场景，与 main.go 的吞吐/延迟矩阵分开：
//
//   - latency：低积压下的稳态排队延迟。drain 场景里的 queue_wait 被积压支配
//     （1000 条任务排队，p95 自然等于排空时长的 95%），它说明的是"排队有多久"，
//     不是"调度有多快"。要看后者，必须在队列不被灌满的条件下测。
//   - txcheck：事务完整率。需求 §10.4 的 10,000 笔「提交/回滚混合」事务。
//   - recovery：Worker 停止续租后，任务被另一个 Worker 接手的墙钟耗时。
//
// 三个场景都**不进 -scenario all**：前两个各自改变数据形状（txcheck 会留下
// 一万行），后一个要让一个 Handler 永远卡住；混进吞吐矩阵会让那些数字失去可比性。

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	gorangetask "github.com/Tidehyc/gorangetask"
	mysqlstore "github.com/Tidehyc/gorangetask/store/mysql"
)

// --- 稳态排队延迟 -----------------------------------------------------------

type latencyResult struct {
	Workers         int              `json:"workers"`
	ArrivalInterval string           `json:"arrival_interval"`
	WindowSeconds   float64          `json:"window_seconds"`
	Tasks           int              `json:"tasks"`
	ElapsedSeconds  float64          `json:"elapsed_seconds"`
	Throughput      float64          `json:"throughput_per_s"`
	QueueWait       summary          `json:"queue_wait"`
	Processing      summary          `json:"processing"`
	ClaimLatency    summary          `json:"claim_latency"`
	DB              dbCounters       `json:"db_delta"`
	FinalStatuses   map[string]int64 `json:"final_status_counts"`
}

// runLatencyMatrix 在若干到达间隔下测排队延迟，每个间隔配一组 Worker 并发数。
//
// 任务数由到达间隔推出来（见 tasksForWindow），保证不同到达率下的观测**窗口长度
// 一致**：否则"每 100ms 一条跑 20 秒"和"每 500ms 一条跑 20 秒"的样本量差 5 倍，
// 分位数的尾部完全不可比。
func runLatencyMatrix(ctx context.Context, db *sql.DB, client *mysqlstore.Client,
	workers []int, arrivals []time.Duration, window, timeout time.Duration) ([]latencyResult, error) {
	out := make([]latencyResult, 0, len(workers)*len(arrivals))
	for _, arrival := range arrivals {
		for _, w := range workers {
			res, err := runLatency(ctx, db, client, w, arrival, window, timeout)
			if err != nil {
				return nil, fmt.Errorf("latency workers=%d arrival=%s: %w", w, arrival, err)
			}
			out = append(out, res)
		}
	}
	return out, nil
}

// tasksForWindow 把"观测窗口"换算成任务数，并夹在 [40, 200] 之间。
//
// 下界 40 是为了 p95 不至于退化成一两个样本；上界 200 是为了 100ms 这种快到达
// 不会把窗口拉长到几分钟（窗口本身是分钟级的话，机器负载的变化会混进来）。
func tasksForWindow(arrival, window time.Duration) int {
	n := int(window / arrival)
	if n < 40 {
		n = 40
	}
	if n > 200 {
		n = 200
	}
	return n
}

func runLatency(ctx context.Context, db *sql.DB, client *mysqlstore.Client,
	workers int, arrival, window, timeout time.Duration) (latencyResult, error) {
	if err := truncateAll(ctx, db); err != nil {
		return latencyResult{}, err
	}
	tasks := tasksForWindow(arrival, window)
	payload := payloadOfSize(256)

	rec := &recorder{}
	cfg := gorangetask.Config{
		Queues:            []string{benchQueue},
		Concurrency:       workers,
		PollInterval:      250 * time.Millisecond,
		MaxPollInterval:   2 * time.Second,
		BatchSize:         1,
		LeaseDuration:     30 * time.Second,
		HeartbeatInterval: 10 * time.Second,
		ReapInterval:      -1,
		CleanupInterval:   -1,
		ShutdownTimeout:   30 * time.Second,
		HandlerTimeout:    30 * time.Second,
		Metrics:           rec,
	}
	reg := gorangetask.NewRegistry()
	if err := reg.Register(benchTaskType, func(context.Context, json.RawMessage) error { return nil }); err != nil {
		return latencyResult{}, fmt.Errorf("register handler: %w", err)
	}
	w, err := gorangetask.NewWorker(client, reg, cfg)
	if err != nil {
		return latencyResult{}, fmt.Errorf("NewWorker: %w", err)
	}

	before, err := readCounters(ctx, db)
	if err != nil {
		return latencyResult{}, err
	}

	runCtx, cancel := context.WithCancel(ctx)
	runDone := make(chan error, 1)
	go func() { runDone <- w.Run(runCtx) }()

	start := time.Now()
	ticker := time.NewTicker(arrival)
	defer ticker.Stop()
	for i := 0; i < tasks; i++ {
		if _, err := client.Enqueue(ctx, gorangetask.Task{
			Queue:   benchQueue,
			Type:    benchTaskType,
			Payload: payload,
		}); err != nil {
			cancel()
			<-runDone
			return latencyResult{}, fmt.Errorf("enqueue #%d: %w", i, err)
		}
		if i < tasks-1 {
			select {
			case <-ctx.Done():
				cancel()
				<-runDone
				return latencyResult{}, ctx.Err()
			case <-ticker.C:
			}
		}
	}
	enqueueTook := time.Since(start)

	if err := waitDrained(ctx, rec, tasks, timeout); err != nil {
		cancel()
		<-runDone
		return latencyResult{}, err
	}
	totalTook := time.Since(start)

	cancel()
	select {
	case err := <-runDone:
		if err != nil {
			return latencyResult{}, fmt.Errorf("worker Run: %w", err)
		}
	case <-time.After(30 * time.Second):
		return latencyResult{}, errors.New("worker 未在停机预算内退出")
	}

	after, err := readCounters(ctx, db)
	if err != nil {
		return latencyResult{}, err
	}
	claim, waited, processing := rec.samples()

	statuses, err := finalStatuses(ctx, db)
	if err != nil {
		return latencyResult{}, err
	}
	if statuses["succeeded"] != int64(tasks) {
		return latencyResult{}, fmt.Errorf("库里有 %d 条 succeeded, 期望 %d", statuses["succeeded"], tasks)
	}

	return latencyResult{
		Workers:         workers,
		ArrivalInterval: arrival.String(),
		WindowSeconds:   enqueueTook.Seconds(),
		Tasks:           tasks,
		ElapsedSeconds:  totalTook.Seconds(),
		Throughput:      float64(tasks) / totalTook.Seconds(),
		QueueWait:       summarize(waited),
		Processing:      summarize(processing),
		ClaimLatency:    summarize(claim),
		DB:              after.minus(before),
		FinalStatuses:   statuses,
	}, nil
}

// --- 事务完整率 -------------------------------------------------------------

type txIntegrityResult struct {
	TotalTransactions         int     `json:"total_transactions"`
	Committed                 int     `json:"committed"`
	RolledBack                int     `json:"rolled_back"`
	Seconds                   float64 `json:"seconds"`
	OrdersCommitted           int64   `json:"orders_committed"`
	OrdersCommittedWithTask   int64   `json:"orders_committed_with_task"`
	OrdersRolledBackRemaining int64   `json:"orders_rolled_back_remaining"`
	TasksTotal                int64   `json:"tasks_total"`
	OrphanTasks               int64   `json:"orphan_tasks"`
	IntegrityPercent          float64 `json:"integrity_percent"`
}

// runTxCheck 跑 total 笔「业务行 + 任务行同一事务」的事务，偶数笔提交、奇数笔回滚。
//
// 口径（与需求文档 §10.3 一致）：
//
//	事务完整率 = 已提交业务记录中存在对应任务的记录数 ÷ 已提交业务记录总数
//
// 同时核对两个方向上都不该出现的东西：回滚后残留的业务行、以及没有业务行对应
// 的孤儿任务。只报一个"100%"而不同时报这两个 0，等于没有验证回滚方向。
//
// **并发不是偷懒**：串行跑时每一笔都要等一次 fsync 落盘（本机 100 ms 上下），
// 一万笔要跑十几分钟；而且真实的"调用方事务"本来就有并发。producers 个并发调用方
// 各跑一部分，提交/回滚仍然严格对半。
func runTxCheck(ctx context.Context, db *sql.DB, client *mysqlstore.Client,
	total, producers int) (txIntegrityResult, error) {
	if err := truncateAll(ctx, db); err != nil {
		return txIntegrityResult{}, err
	}
	if producers < 1 {
		producers = 1
	}
	payload := payloadOfSize(256)
	res := txIntegrityResult{TotalTransactions: total}

	var (
		mu        sync.Mutex
		firstErr  error
		commits   int
		rollbacks int
		wg        sync.WaitGroup
	)
	fail := func(err error) {
		mu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		mu.Unlock()
	}

	start := time.Now()
	for g := 0; g < producers; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := g; i < total; i += producers {
				commit := i%2 == 0
				note := "txcheck-c-" + fmt.Sprint(i)
				if !commit {
					note = "txcheck-r-" + fmt.Sprint(i)
				}

				tx, err := db.BeginTx(ctx, nil)
				if err != nil {
					fail(fmt.Errorf("tx #%d begin: %w", i, err))
					return
				}
				result, err := tx.ExecContext(ctx,
					"INSERT INTO "+ordersTable+" (note, created_at) VALUES (?, UTC_TIMESTAMP(6))", note)
				if err != nil {
					_ = tx.Rollback()
					fail(fmt.Errorf("tx #%d insert order: %w", i, err))
					return
				}
				orderID, err := result.LastInsertId()
				if err != nil {
					_ = tx.Rollback()
					fail(fmt.Errorf("tx #%d last insert id: %w", i, err))
					return
				}
				if _, err := client.EnqueueTx(ctx, tx, gorangetask.Task{
					Queue:   benchQueue,
					Type:    benchTaskType,
					Payload: payload,
					// 幂等键带上业务行主键，事后才能把两侧的每一行一一对上，而不是只比总数。
					IdempotencyKey: fmt.Sprintf("txcheck-%d", orderID),
				}); err != nil {
					_ = tx.Rollback()
					fail(fmt.Errorf("tx #%d enqueue: %w", i, err))
					return
				}

				if commit {
					if err := tx.Commit(); err != nil {
						fail(fmt.Errorf("tx #%d commit: %w", i, err))
						return
					}
					mu.Lock()
					commits++
					mu.Unlock()
				} else {
					if err := tx.Rollback(); err != nil {
						fail(fmt.Errorf("tx #%d rollback: %w", i, err))
						return
					}
					mu.Lock()
					rollbacks++
					mu.Unlock()
				}
			}
		}(g)
	}
	wg.Wait()
	res.Seconds = time.Since(start).Seconds()
	if firstErr != nil {
		return res, firstErr
	}
	res.Committed, res.RolledBack = commits, rollbacks

	count := func(query string, args ...any) (int64, error) {
		var n int64
		err := db.QueryRowContext(ctx, query, args...).Scan(&n)
		return n, err
	}
	var err error
	if res.OrdersCommitted, err = count(
		"SELECT COUNT(*) FROM " + ordersTable + " WHERE note LIKE 'txcheck-c-%'"); err != nil {
		return res, fmt.Errorf("count committed orders: %w", err)
	}
	if res.OrdersRolledBackRemaining, err = count(
		"SELECT COUNT(*) FROM " + ordersTable + " WHERE note LIKE 'txcheck-r-%'"); err != nil {
		return res, fmt.Errorf("count rolled back orders: %w", err)
	}
	if res.OrdersCommittedWithTask, err = count(
		"SELECT COUNT(*) FROM "+ordersTable+" o JOIN gotask_tasks t"+
			" ON t.queue_name = ? AND t.idempotency_key = CONCAT('txcheck-', o.id)"+
			" WHERE o.note LIKE 'txcheck-c-%'", benchQueue); err != nil {
		return res, fmt.Errorf("count orders with task: %w", err)
	}
	if res.TasksTotal, err = count(
		"SELECT COUNT(*) FROM gotask_tasks WHERE queue_name = ? AND idempotency_key LIKE 'txcheck-%'",
		benchQueue); err != nil {
		return res, fmt.Errorf("count tasks: %w", err)
	}
	res.OrphanTasks = res.TasksTotal - res.OrdersCommittedWithTask

	if res.OrdersCommitted > 0 {
		res.IntegrityPercent = float64(res.OrdersCommittedWithTask) /
			float64(res.OrdersCommitted) * 100
	}

	// 三项一致性检查，任一不成立就直接失败——不要留一个"大体正确"的数字。
	if res.OrdersCommitted != int64(res.Committed) {
		return res, fmt.Errorf("已提交事务 %d 笔，库里只有 %d 行业务记录", res.Committed, res.OrdersCommitted)
	}
	if res.OrdersRolledBackRemaining != 0 {
		return res, fmt.Errorf("回滚后仍残留 %d 行业务记录", res.OrdersRolledBackRemaining)
	}
	if res.OrphanTasks != 0 {
		return res, fmt.Errorf("存在 %d 条没有业务行对应的任务", res.OrphanTasks)
	}
	if res.OrdersCommittedWithTask != res.OrdersCommitted {
		return res, fmt.Errorf("事务完整率 %.2f%%：%d/%d 笔已提交业务记录没有对应任务",
			res.IntegrityPercent, res.OrdersCommitted-res.OrdersCommittedWithTask, res.OrdersCommitted)
	}
	return res, nil
}

// --- 崩溃恢复耗时 -----------------------------------------------------------

type recoveryResult struct {
	LeaseDuration           string  `json:"lease_duration"`
	HeartbeatInterval       string  `json:"heartbeat_interval"`
	ReapInterval            string  `json:"reap_interval"`
	ShutdownTimeout         string  `json:"shutdown_timeout"`
	LeaseRemainingAtStallMs float64 `json:"lease_remaining_at_stall_ms"`
	RecoverySeconds         float64 `json:"recovery_seconds"`
	DecompositionSeconds    float64 `json:"decomposition_ceiling_seconds"`
	FirstWorkerHandlerRuns  int64   `json:"first_worker_handler_runs"`
	SecondWorkerHandlerRuns int64   `json:"second_worker_handler_runs"`
	FinalStatus             string  `json:"final_status"`
	FinalAttempt            int64   `json:"final_attempt"`
	StaleWriteBackRejected  bool    `json:"stale_write_back_rejected"`
}

const (
	recoveryLease     = 2 * time.Second
	recoveryHeartbeat = 666 * time.Millisecond
	recoveryReap      = 250 * time.Millisecond
	recoveryShutdown  = 500 * time.Millisecond
)

// runRecovery 测「Worker 停止续租 → 另一个 Worker 把任务接手跑完」的墙钟耗时。
//
// 崩溃怎么造出来的：让第一个 Worker 的 Handler **无视 ctx 永远阻塞**，再取消它的
// Run。按产品的停机语义，Run 在 ShutdownTimeout 后放弃等待并返回 ErrShutdownTimeout，
// 而这条任务的续租看护**随停机一起停掉**——从这一刻起，库里的状态与"进程被 kill -9"
// 完全一致：任务停在 running、租约不再被延长、也不会有人写回。为此不需要真的杀进程，
// 因此这个数字是确定性可复现的；代价是 Handler 的 goroutine 还活着（它在测量结束时
// 被放行，随即被 fencing 拒掉写回——这一条本身也是断言）。
//
// 恢复耗时按定义拆得开：剩余租约 + 一个回收周期 + 一次轮询 + 领取与执行。
// 报告里同时给出实测值与这个上界，两者差得远就说明有别的东西在拖时间。
func runRecovery(ctx context.Context, db *sql.DB, client *mysqlstore.Client) (recoveryResult, error) {
	if err := truncateAll(ctx, db); err != nil {
		return recoveryResult{}, err
	}
	res := recoveryResult{
		LeaseDuration:     recoveryLease.String(),
		HeartbeatInterval: recoveryHeartbeat.String(),
		ReapInterval:      recoveryReap.String(),
		ShutdownTimeout:   recoveryShutdown.String(),
	}

	taskID, err := client.Enqueue(ctx, gorangetask.Task{
		Queue:   benchQueue,
		Type:    benchTaskType,
		Payload: payloadOfSize(256),
	})
	if err != nil {
		return res, fmt.Errorf("enqueue: %w", err)
	}

	var aRuns, bRuns atomic.Int64
	entered := make(chan struct{}, 1)
	releaseA := make(chan struct{})
	bFinished := make(chan struct{}, 1)
	aRec := &recorder{}
	bRec := &recorder{}

	regA := gorangetask.NewRegistry()
	if err := regA.Register(benchTaskType, func(context.Context, json.RawMessage) error {
		aRuns.Add(1)
		select {
		case entered <- struct{}{}:
		default:
		}
		<-releaseA // 刻意不监听 ctx：模拟"Handler 卡在外部调用里，停机叫不停它"
		return nil
	}); err != nil {
		return res, fmt.Errorf("register A handler: %w", err)
	}
	wA, err := gorangetask.NewWorker(client, regA, gorangetask.Config{
		Queues:            []string{benchQueue},
		Concurrency:       1,
		PollInterval:      50 * time.Millisecond,
		MaxPollInterval:   200 * time.Millisecond,
		BatchSize:         1,
		LeaseDuration:     recoveryLease,
		HeartbeatInterval: recoveryHeartbeat,
		ReapInterval:      -1,
		CleanupInterval:   -1,
		ShutdownTimeout:   recoveryShutdown,
		HandlerTimeout:    5 * time.Minute, // 远大于观测窗口，确保超时不插手
		Metrics:           aRec,
	})
	if err != nil {
		return res, fmt.Errorf("NewWorker A: %w", err)
	}

	runA, cancelA := context.WithCancel(ctx)
	doneA := make(chan error, 1)
	go func() { doneA <- wA.Run(runA) }()

	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		cancelA()
		<-doneA
		return res, errors.New("第一个 Worker 未在 10s 内领到任务")
	}

	// 停止续租那一刻还剩多少租约：用数据库时钟算，别用本地钟去减库里的时间。
	var remainingUs int64
	if err := db.QueryRowContext(ctx,
		"SELECT TIMESTAMPDIFF(MICROSECOND, UTC_TIMESTAMP(6), lease_until) FROM gotask_tasks WHERE id = ?",
		uint64(taskID)).Scan(&remainingUs); err != nil {
		cancelA()
		return res, fmt.Errorf("read remaining lease: %w", err)
	}
	res.LeaseRemainingAtStallMs = round3(float64(remainingUs) / 1000)

	stallAt := time.Now()
	cancelA()
	select {
	case err := <-doneA:
		if !errors.Is(err, gorangetask.ErrShutdownTimeout) {
			return res, fmt.Errorf("第一个 Worker 应当以 ErrShutdownTimeout 退出，实际 %v", err)
		}
	case <-time.After(10 * time.Second):
		return res, errors.New("第一个 Worker 未在停机预算内返回")
	}

	regB := gorangetask.NewRegistry()
	if err := regB.Register(benchTaskType, func(context.Context, json.RawMessage) error {
		bRuns.Add(1)
		select {
		case bFinished <- struct{}{}:
		default:
		}
		return nil
	}); err != nil {
		return res, fmt.Errorf("register B handler: %w", err)
	}
	wB, err := gorangetask.NewWorker(client, regB, gorangetask.Config{
		Queues:            []string{benchQueue},
		Concurrency:       1,
		PollInterval:      250 * time.Millisecond,
		MaxPollInterval:   2 * time.Second,
		BatchSize:         1,
		LeaseDuration:     30 * time.Second,
		HeartbeatInterval: 10 * time.Second,
		ReapInterval:      recoveryReap,
		CleanupInterval:   -1,
		ShutdownTimeout:   5 * time.Second,
		HandlerTimeout:    30 * time.Second,
		Metrics:           bRec,
	})
	if err != nil {
		return res, fmt.Errorf("NewWorker B: %w", err)
	}

	runB, cancelB := context.WithCancel(ctx)
	doneB := make(chan error, 1)
	go func() { doneB <- wB.Run(runB) }()

	select {
	case <-bFinished:
	case <-time.After(60 * time.Second):
		cancelB()
		<-doneB
		return res, errors.New("第二个 Worker 未在 60s 内接手并完成（租约可能没有被回收）")
	}
	res.RecoverySeconds = round3(time.Since(stallAt).Seconds())
	res.DecompositionSeconds = round3(
		res.LeaseRemainingAtStallMs/1000 + recoveryReap.Seconds() + 0.25 + 0.05)

	cancelB()
	select {
	case err := <-doneB:
		if err != nil {
			return res, fmt.Errorf("worker B Run: %w", err)
		}
	case <-time.After(30 * time.Second):
		return res, errors.New("第二个 Worker 未在停机预算内退出")
	}

	// 放行第一个 Worker 的 Handler：它会拿着陈旧 epoch 去写回，必须被 fencing 拒掉。
	//
	// 先确认放行之前一次失租都没记过：失租指标有两条来源（续租被拒、写回被拒），
	// 若续租先撞上，这里看到的 1 就不是"写回被拒"的证据了。
	leaseLostBefore := aRec.snapshot().LeaseLost
	if leaseLostBefore != 0 {
		return res, fmt.Errorf("放行之前已记了 %d 次失租：这是续租先撞上的，不是写回被拒", leaseLostBefore)
	}
	close(releaseA)
	deadline := time.Now().Add(10 * time.Second)
	for {
		if aRec.snapshot().LeaseLost > leaseLostBefore {
			res.StaleWriteBackRejected = true
			break
		}
		if time.Now().After(deadline) {
			return res, errors.New("陈旧 Worker 的写回既没有被拒，也没有被记为失租")
		}
		time.Sleep(20 * time.Millisecond)
	}

	if err := db.QueryRowContext(ctx,
		"SELECT status, attempt FROM gotask_tasks WHERE id = ?", uint64(taskID)).
		Scan(&res.FinalStatus, &res.FinalAttempt); err != nil {
		return res, fmt.Errorf("read final task row: %w", err)
	}
	res.FirstWorkerHandlerRuns = aRuns.Load()
	res.SecondWorkerHandlerRuns = bRuns.Load()

	if res.FinalStatus != string(gorangetask.StatusSucceeded) {
		return res, fmt.Errorf("最终状态是 %s，期望 succeeded", res.FinalStatus)
	}
	if res.FinalAttempt != 2 {
		return res, fmt.Errorf("最终 attempt = %d，期望 2（说明任务被领取过两次）", res.FinalAttempt)
	}
	if res.FirstWorkerHandlerRuns != 1 || res.SecondWorkerHandlerRuns != 1 {
		return res, fmt.Errorf("Handler 执行次数 A=%d B=%d，期望各 1 次",
			res.FirstWorkerHandlerRuns, res.SecondWorkerHandlerRuns)
	}
	return res, nil
}
