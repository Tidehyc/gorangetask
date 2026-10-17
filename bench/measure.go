package main

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	gorangetask "github.com/Tidehyc/gorangetask"
)

// 本文件只放"怎么测"：分位数摘要、数据库计数器、MySQL 进程 CPU 采样、连接池
// 采样，以及一个把库的 Metrics 事件记成原始样本的 recorder。场景编排在 main.go。

// --- 分位数 ----------------------------------------------------------------

// summary 是一组耗时样本的摘要，单位统一换成毫秒，便于直接写进文档。
//
// 分位数用**最近秩法**（nearest rank：ceil(p·N)-1），不做插值。样本量在几百到
// 几千时它与插值法的差别远小于测量噪声，而它有一个好处：每个数都是真实观测到的
// 一次调用，不是算出来的。
type summary struct {
	N    int     `json:"n"`
	P50  float64 `json:"p50_ms"`
	P95  float64 `json:"p95_ms"`
	P99  float64 `json:"p99_ms"`
	Max  float64 `json:"max_ms"`
	Mean float64 `json:"mean_ms"`
}

func summarize(ds []time.Duration) summary {
	if len(ds) == 0 {
		return summary{}
	}
	sorted := make([]time.Duration, len(ds))
	copy(sorted, ds)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	pick := func(p float64) time.Duration {
		idx := int(math.Ceil(p*float64(len(sorted)))) - 1
		if idx < 0 {
			idx = 0
		}
		if idx >= len(sorted) {
			idx = len(sorted) - 1
		}
		return sorted[idx]
	}

	var total time.Duration
	for _, d := range sorted {
		total += d
	}
	return summary{
		N:    len(sorted),
		P50:  toMillis(pick(0.50)),
		P95:  toMillis(pick(0.95)),
		P99:  toMillis(pick(0.99)),
		Max:  toMillis(sorted[len(sorted)-1]),
		Mean: toMillis(total / time.Duration(len(sorted))),
	}
}

func toMillis(d time.Duration) float64 { return float64(d.Microseconds()) / 1000.0 }

// --- 数据库全局计数器 ------------------------------------------------------

// dbCounters 是 MySQL 的全局状态计数器。三项都是**自实例启动起单调累加**的，
// 因此只能在测量窗口两端各取一次、用差值表述"这段时间里发生了什么"。
type dbCounters struct {
	// Questions 是客户端发出的语句数：它直接回答了"空队列轮询带来多少 QPS"。
	Questions int64 `json:"questions"`
	// RowLockWaits 是 InnoDB 行锁等待次数。
	RowLockWaits int64 `json:"innodb_row_lock_waits"`
	// RowLockTimeMs 是行锁等待累计毫秒数。
	RowLockTimeMs int64 `json:"innodb_row_lock_time_ms"`
}

func readCounters(ctx context.Context, db *sql.DB) (dbCounters, error) {
	var c dbCounters
	rows, err := db.QueryContext(ctx, `SHOW GLOBAL STATUS WHERE Variable_name IN
        ('Questions', 'Innodb_row_lock_waits', 'Innodb_row_lock_time')`)
	if err != nil {
		return c, fmt.Errorf("read global status: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var name, value string
		if err := rows.Scan(&name, &value); err != nil {
			return c, fmt.Errorf("scan global status: %w", err)
		}
		n, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			// 有些版本把非数值状态也放在 SHOW STATUS 里；本函数只取这三个数字键。
			continue
		}
		switch name {
		case "Questions":
			c.Questions = n
		case "Innodb_row_lock_waits":
			c.RowLockWaits = n
		case "Innodb_row_lock_time":
			c.RowLockTimeMs = n
		}
	}
	return c, rows.Err()
}

// minus 返回两个采样点之间的增量。
func (a dbCounters) minus(b dbCounters) dbCounters {
	return dbCounters{
		Questions:     a.Questions - b.Questions,
		RowLockWaits:  a.RowLockWaits - b.RowLockWaits,
		RowLockTimeMs: a.RowLockTimeMs - b.RowLockTimeMs,
	}
}

// --- MySQL 进程 CPU --------------------------------------------------------

// cpuStats 是测量窗口内 MySQL 进程 CPU 占用的采样摘要，单位为"一个核的百分比"
// （100% = 跑满一个核，机器有 4 个核时满负载是 400%）。
type cpuStats struct {
	Samples int     `json:"samples"`
	Mean    float64 `json:"mean_percent_of_one_core"`
	Max     float64 `json:"max_percent_of_one_core"`
	Note    string  `json:"note,omitempty"`
}

// cpuProbe 周期性地问一次 `docker stats`，把 MySQL 容器的瞬时 CPU 占用记下来。
//
// 为什么不用 /proc/<pid>/stat：命令沙箱有独立的 cgroup 与 PID 视图，拿不到宿主上
// 容器进程的 /proc 条目（实测不可读），宿主根 cgroup 也不可见。`docker stats` 是
// 这个环境里唯一能拿到的、指向**MySQL 自己**的 CPU 读数。
//
// 每次 `docker stats --no-stream` 自身要花约 1 秒（内部取两次样本算速率），因此
// 采样节奏约 1.1 秒一次：测量窗口越长越可信，几秒级的场景只能看个量级。
type cpuProbe struct {
	container string

	mu      sync.Mutex
	samples []float64
	failure string

	stopCh chan struct{}
	doneCh chan struct{}
}

func startCPUProbe(container string) *cpuProbe {
	p := &cpuProbe{container: container, stopCh: make(chan struct{}), doneCh: make(chan struct{})}
	if container == "" {
		p.failure = "未指定 -container"
		close(p.doneCh)
		return p
	}
	go func() {
		defer close(p.doneCh)
		for {
			select {
			case <-p.stopCh:
				return
			default:
			}

			out, err := exec.Command("docker", "stats", p.container,
				"--no-stream", "--format", "{{.CPUPerc}}").Output()
			if err != nil {
				p.mu.Lock()
				if p.failure == "" {
					p.failure = err.Error()
				}
				p.mu.Unlock()
				return
			}
			v, ok := parsePercent(string(out))
			if ok {
				p.mu.Lock()
				p.samples = append(p.samples, v)
				p.mu.Unlock()
			}

			select {
			case <-p.stopCh:
				return
			case <-time.After(100 * time.Millisecond):
			}
		}
	}()
	return p
}

func (p *cpuProbe) stop() cpuStats {
	close(p.stopCh)
	<-p.doneCh

	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.samples) == 0 {
		note := "无 CPU 采样"
		if p.failure != "" {
			note = "docker stats 不可用：" + p.failure
		}
		return cpuStats{Note: note}
	}
	var sum, max float64
	for _, v := range p.samples {
		sum += v
		if v > max {
			max = v
		}
	}
	return cpuStats{
		Samples: len(p.samples),
		Mean:    round2(sum / float64(len(p.samples))),
		Max:     round2(max),
	}
}

// parsePercent 从 "12.34%" 里取出 12.34。
func parsePercent(s string) (float64, bool) {
	s = strings.TrimSpace(s)
	s = strings.TrimSuffix(s, "%")
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, false
	}
	return v, true
}

// --- 连接池 ----------------------------------------------------------------

// poolStats 是测量窗口结束时的连接池状态，外加窗口内的最高水位。
type poolStats struct {
	MaxOpenConns   int     `json:"max_open_conns"`
	MaxInUse       int     `json:"max_in_use"`
	MaxIdle        int     `json:"max_idle"`
	WaitCount      int64   `json:"wait_count"`
	WaitDurationMs float64 `json:"wait_duration_ms"`
}

// poolProbe 每 20ms 读一次 db.Stats()，记住 InUse 的最高水位。
//
// db.Stats() 读的是 database/sql 内部的原子量，代价可以忽略，所以可以采得密一点；
// WaitCount 是累计值，只需在结束时取一次。
type poolProbe struct {
	db *sql.DB

	mu       sync.Mutex
	maxInUse int
	maxIdle  int

	stopCh chan struct{}
	doneCh chan struct{}
}

func startPoolProbe(db *sql.DB) *poolProbe {
	p := &poolProbe{db: db, stopCh: make(chan struct{}), doneCh: make(chan struct{})}
	go func() {
		defer close(p.doneCh)
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-p.stopCh:
				return
			case <-ticker.C:
				s := p.db.Stats()
				p.mu.Lock()
				if s.InUse > p.maxInUse {
					p.maxInUse = s.InUse
				}
				if s.Idle > p.maxIdle {
					p.maxIdle = s.Idle
				}
				p.mu.Unlock()
			}
		}
	}()
	return p
}

func (p *poolProbe) stop() poolStats {
	close(p.stopCh)
	<-p.doneCh

	final := p.db.Stats()
	p.mu.Lock()
	defer p.mu.Unlock()
	return poolStats{
		MaxOpenConns:   final.MaxOpenConnections,
		MaxInUse:       p.maxInUse,
		MaxIdle:        p.maxIdle,
		WaitCount:      final.WaitCount,
		WaitDurationMs: toMillis(final.WaitDuration),
	}
}

// --- Metrics 记录器 --------------------------------------------------------

// recorder 把库发出的指标事件原样存成样本。它实现 gorangetask.Metrics。
//
// 这里刻意不做任何聚合：分位数必须由原始样本算，边跑边折叠成均值会把长尾抹掉，
// 而长尾正是这类队列最该看的指标。
type recorder struct {
	gorangetask.NopMetrics

	mu             sync.Mutex
	claimLatency   []time.Duration
	waited         []time.Duration
	processing     []time.Duration
	claimed        int64
	succeeded      int64
	retried        int64
	dead           int64
	leaseLost      int64
	panicked       int64
	timedOut       int64
	enqueued       int64
	canceled       int64
	maxConcurrency int
}

func (r *recorder) TaskEnqueued(string) { r.bump(&r.enqueued) }
func (r *recorder) TaskCanceled()       { r.bump(&r.canceled) }

func (r *recorder) TaskClaimed(string, string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.claimed++
}

func (r *recorder) TaskFinished(_, _ string, o gorangetask.Outcome, d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	switch o {
	case gorangetask.OutcomeSucceeded:
		r.succeeded++
	case gorangetask.OutcomeRetried:
		r.retried++
	case gorangetask.OutcomeDead:
		r.dead++
	}
	r.processing = append(r.processing, d)
}

func (r *recorder) TaskWaited(_, _ string, d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.waited = append(r.waited, d)
}

func (r *recorder) LeaseLost(string, string)       { r.bump(&r.leaseLost) }
func (r *recorder) HandlerPanicked(string, string) { r.bump(&r.panicked) }
func (r *recorder) HandlerTimedOut(string, string) { r.bump(&r.timedOut) }

func (r *recorder) TasksReaped(int, int)                   {}
func (r *recorder) TasksCleaned(gorangetask.Status, int64) {}

func (r *recorder) ClaimLatency(d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.claimLatency = append(r.claimLatency, d)
}

func (r *recorder) ConcurrencyInUse(n int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if n > r.maxConcurrency {
		r.maxConcurrency = n
	}
}

func (r *recorder) bump(p *int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	*p++
}

// counts 是一次快照。
type counts struct {
	Claimed        int64 `json:"claimed"`
	Succeeded      int64 `json:"succeeded"`
	Retried        int64 `json:"retried"`
	Dead           int64 `json:"dead"`
	LeaseLost      int64 `json:"lease_lost"`
	Panicked       int64 `json:"handler_panicked"`
	TimedOut       int64 `json:"handler_timed_out"`
	Enqueued       int64 `json:"enqueued"`
	Canceled       int64 `json:"canceled"`
	MaxConcurrency int   `json:"max_concurrency"`
}

func (r *recorder) snapshot() counts {
	r.mu.Lock()
	defer r.mu.Unlock()
	return counts{
		Claimed:        r.claimed,
		Succeeded:      r.succeeded,
		Retried:        r.retried,
		Dead:           r.dead,
		LeaseLost:      r.leaseLost,
		Panicked:       r.panicked,
		TimedOut:       r.timedOut,
		Enqueued:       r.enqueued,
		Canceled:       r.canceled,
		MaxConcurrency: r.maxConcurrency,
	}
}

// samples 取一份原始样本的拷贝，供分位数计算（计算本身在锁外做）。
func (r *recorder) samples() (claim, waited, processing []time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	claim = append(claim, r.claimLatency...)
	waited = append(waited, r.waited...)
	processing = append(processing, r.processing...)
	return claim, waited, processing
}

func round2(v float64) float64 { return math.Round(v*100) / 100 }
