// Command bench 是 GoRangeTask 的性能基线程序。
//
// 它回答需求 §10.4 要求的几件事：固定数据库与配置下，入队延迟（含示例 Producer
// 那种"业务行 + 任务行同一事务"的写法）的分位数、不同并发下的吞吐、人为积压后的
// 排空时间、空队列轮询带来的数据库 QPS，以及这期间的锁等待、连接池占用与 MySQL
// 进程 CPU。
//
// 用法（本机开发环境）：
//
//	export GOTASK_BENCH_DSN='root:rootpw@tcp(127.0.0.1:3306)/gotask_bench?parseTime=true&loc=UTC&timeout=5s&readTimeout=30s&writeTimeout=30s'
//	go run ./bench -out bench/results/$(date +%Y%m%dT%H%M%S).json
//
// 单场景：
//
//	go run ./bench -scenario enqueue
//	go run ./bench -scenario drain -workers 1,4,16 -sizes 256,4096,32768
//	go run ./bench -scenario idle -workers 1,4,16
//	go run ./bench -scenario latency -workers 1,4 -arrivals 100ms,500ms
//	go run ./bench -scenario txcheck -txcount 10000
//	go run ./bench -scenario recovery
//
// 后三个场景（latency / txcheck / recovery）刻意**不包含在 -scenario all 里**，
// 原因见 scenarios.go 开头。
//
// 设计取舍见 docs/GoRangeTask-性能基线.md。三条最要紧的：
//
//  1. **不预设结论**。程序只报观测值，不做"比 X 快/慢"的判断。
//  2. **原始样本不折叠**。分位数由每次调用的耗时算出来，不是从均值推的。
//  3. **该失败就失败**。任何一个场景出现任务未完成、Handler 报错或计数对不上，
//     都以非零退出码结束——宁可没有数字，也不要一个"看起来跑完了"的数字。
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	gomysql "github.com/go-sql-driver/mysql"

	gorangetask "github.com/Tidehyc/gorangetask"
	mysqlmigrations "github.com/Tidehyc/gorangetask/migrations/mysql"
	mysqlstore "github.com/Tidehyc/gorangetask/store/mysql"
)

const (
	benchQueue    = "bench"
	benchTaskType = "bench.noop"
	ordersTable   = "bench_orders"
)

func main() {
	var (
		dsn       = flag.String("dsn", os.Getenv("GOTASK_BENCH_DSN"), "MySQL DSN（库会被自动创建；默认取 GOTASK_BENCH_DSN）")
		out       = flag.String("out", "", "把结果写成 JSON 的路径（目录不存在会自动创建）")
		container = flag.String("container", "gotask-mysql", "用于采 MySQL 进程 CPU 的容器名；传空则跳过 CPU 采集")
		scenario  = flag.String("scenario", "all", "all | enqueue | drain | idle | latency | txcheck | recovery")
		tasks     = flag.Int("tasks", 1000, "每个 drain 场景预置的任务数")
		txcount   = flag.Int("txcount", 10000, "txcheck 场景的事务数（偶数笔提交、奇数笔回滚）")
		workers   = flag.String("workers", "1,4,16", "drain/idle 的并发数列表（逗号分隔）")
		arrivals  = flag.String("arrivals", "100ms,500ms", "latency 场景的入队间隔列表（逗号分隔）")
		window    = flag.Duration("window", 30*time.Second, "latency 场景每个到达间隔的观测窗口")
		sizes     = flag.String("sizes", "256,4096,32768", "drain 的 payload 字节数列表（逗号分隔）")
		producers = flag.Int("producers", 8, "入队场景的并发 producer 数")
		enqueues  = flag.Int("enqueues", 400, "每个入队场景的总调用次数")
		idleFor   = flag.Duration("idle", 20*time.Second, "空队列轮询的观测时长")
		timeout   = flag.Duration("timeout", 5*time.Minute, "单个 drain 场景的排空超时")
	)
	flag.Parse()

	arrivalList, err := splitDurations(*arrivals)
	if err != nil {
		fmt.Fprintf(os.Stderr, "bench: %v\n", err)
		os.Exit(1)
	}
	o := options{
		dsn:       *dsn,
		out:       *out,
		container: *container,
		scenario:  *scenario,
		tasks:     *tasks,
		txcount:   *txcount,
		workers:   splitInts(*workers),
		arrivals:  arrivalList,
		window:    *window,
		sizes:     splitInts(*sizes),
		producers: *producers,
		enqueues:  *enqueues,
		idleFor:   *idleFor,
		timeout:   *timeout,
	}
	if err := run(o); err != nil {
		fmt.Fprintf(os.Stderr, "bench: %v\n", err)
		os.Exit(1)
	}
}

// options 是一次运行的输入。参数攒够十个以后用结构体比继续加位置参数清楚——
// 尤其 txcount / arrivals / window 只服务于一个场景，混在位置参数里极易传错。
type options struct {
	dsn       string
	out       string
	container string
	scenario  string
	tasks     int
	txcount   int
	workers   []int
	arrivals  []time.Duration
	window    time.Duration
	sizes     []int
	producers int
	enqueues  int
	idleFor   time.Duration
	timeout   time.Duration
}

// report 是整份结果的顶层结构：环境一次，场景若干。
type report struct {
	Meta            meta               `json:"meta"`
	Enqueue         []enqueueResult    `json:"enqueue,omitempty"`
	EnqueueOverhead *overhead          `json:"enqueue_overhead,omitempty"`
	Drain           []drainResult      `json:"drain,omitempty"`
	Idle            []idleResult       `json:"idle,omitempty"`
	Latency         []latencyResult    `json:"latency,omitempty"`
	TxCheck         *txIntegrityResult `json:"tx_integrity,omitempty"`
	Recovery        *recoveryResult    `json:"recovery,omitempty"`
}

func run(o options) error {
	dsn, scenario := o.dsn, o.scenario
	if dsn == "" {
		return errors.New("缺少 DSN：用 -dsn 或 GOTASK_BENCH_DSN 指定，例如 " +
			"'root:rootpw@tcp(127.0.0.1:3306)/gotask_bench?parseTime=true&loc=UTC&timeout=5s&readTimeout=30s&writeTimeout=30s'")
	}
	if o.tasks <= 0 || o.producers <= 0 || o.enqueues <= 0 || o.txcount <= 0 {
		return errors.New("-tasks / -producers / -enqueues / -txcount 必须大于 0")
	}
	if len(o.arrivals) == 0 {
		return errors.New("-arrivals 至少要有一个间隔")
	}
	// 空队列场景要先花 5 秒等退避长到稳态，观测窗口必须比它长。
	if o.idleFor <= 6*time.Second {
		return errors.New("-idle 必须大于 6s（前 5 秒用于等轮询退避进入稳态）")
	}

	ctx := context.Background()

	db, dbName, err := openBenchDB(ctx, dsn)
	if err != nil {
		return err
	}
	defer db.Close()

	client, err := mysqlstore.New(db, gorangetask.ClientConfig{})
	if err != nil {
		return fmt.Errorf("mysqlstore.New: %w", err)
	}

	if err := prepareSchema(ctx, db); err != nil {
		return err
	}

	r := &report{}
	if err := r.Meta.collect(ctx, db, dbName, dsn); err != nil {
		return err
	}

	switch scenario {
	case "all", "enqueue":
		results, err := runEnqueueMatrix(ctx, db, client, o.producers, o.enqueues)
		if err != nil {
			return err
		}
		r.Enqueue = results
		r.EnqueueOverhead = enqueueOverhead(results)
	case "drain", "idle", "latency", "txcheck", "recovery":
	default:
		return fmt.Errorf("未知 -scenario %q（可选 all | enqueue | drain | idle | latency | txcheck | recovery）", scenario)
	}

	if scenario == "all" || scenario == "drain" {
		results, err := runDrainMatrix(ctx, db, client, o.workers, o.sizes, o.tasks, o.container, o.timeout)
		if err != nil {
			return err
		}
		r.Drain = results
	}
	if scenario == "all" || scenario == "idle" {
		results, err := runIdleMatrix(ctx, db, client, o.workers, o.idleFor, o.container)
		if err != nil {
			return err
		}
		r.Idle = results
	}
	if scenario == "latency" {
		results, err := runLatencyMatrix(ctx, db, client, o.workers, o.arrivals, o.window, o.timeout)
		if err != nil {
			return err
		}
		r.Latency = results
	}
	if scenario == "txcheck" {
		res, err := runTxCheck(ctx, db, client, o.txcount, o.producers)
		if err != nil {
			return err
		}
		r.TxCheck = &res
	}
	if scenario == "recovery" {
		res, err := runRecovery(ctx, db, client)
		if err != nil {
			return err
		}
		r.Recovery = &res
	}

	if err := r.Meta.collectLoadAfter(); err != nil {
		return err
	}
	r.Meta.print()
	r.print()
	return r.write(o.out)
}

// --- 环境 -------------------------------------------------------------------

type meta struct {
	GeneratedAt   string            `json:"generated_at"`
	GoVersion     string            `json:"go_version"`
	NumCPU        int               `json:"num_cpu"`
	CPUModel      string            `json:"cpu_model"`
	MemTotalKB    int64             `json:"mem_total_kb"`
	LoadAvgBefore string            `json:"load_avg_before"`
	LoadAvgAfter  string            `json:"load_avg_after,omitempty"`
	MySQLVersion  string            `json:"mysql_version"`
	MySQLVars     map[string]string `json:"mysql_variables"`
	DBHost        string            `json:"db_host"`
	DBName        string            `json:"db_name"`
	Pool          string            `json:"pool_settings"`
	WorkerConfig  map[string]string `json:"worker_config"`
	Notes         []string          `json:"notes"`
}

func (m *meta) collect(ctx context.Context, db *sql.DB, dbName, dsn string) error {
	m.GeneratedAt = time.Now().Format(time.RFC3339)
	m.GoVersion = runtime.Version()
	m.NumCPU = runtime.NumCPU()
	m.CPUModel = firstLineValue("/proc/cpuinfo", "model name")
	m.MemTotalKB = memTotalKB()
	m.LoadAvgBefore = loadAvg()
	m.DBName = dbName
	m.DBHost = hostOf(dsn)
	m.Pool = "MaxOpenConns=32 MaxIdleConns=32 ConnMaxLifetime=0（bench 固定值）"
	m.WorkerConfig = map[string]string{
		"queues":             benchQueue,
		"batch_size":         "1",
		"poll_interval":      "250ms",
		"max_poll_interval":  "2s",
		"lease_duration":     "30s",
		"heartbeat_interval": "10s",
		"shutdown_timeout":   "30s",
		"handler_timeout":    "30s",
		"reaper":             "关闭（ReapInterval=-1，隔离出干净的吞吐数字）",
		"cleanup":            "关闭（CleanupInterval=-1，避免删掉要统计的终态行）",
		"concurrency":        "见各场景（并发池大小；Run 按它起同等数量的领取 goroutine）",
	}
	m.MySQLVars = map[string]string{}
	row := db.QueryRowContext(ctx, `SELECT VERSION(),
        @@innodb_buffer_pool_size, @@innodb_flush_log_at_trx_commit, @@sync_binlog,
        @@max_connections, @@transaction_isolation, @@binlog_format, @@innodb_flush_method`)
	var version, isolation, binlog, flushMethod string
	var bufferPool, flushLog, syncBinlog, maxConns int64
	if err := row.Scan(&version, &bufferPool, &flushLog, &syncBinlog,
		&maxConns, &isolation, &binlog, &flushMethod); err != nil {
		return fmt.Errorf("read MySQL variables: %w", err)
	}
	m.MySQLVersion = version
	m.MySQLVars["innodb_buffer_pool_size"] = strconv.FormatInt(bufferPool, 10)
	m.MySQLVars["innodb_flush_log_at_trx_commit"] = strconv.FormatInt(flushLog, 10)
	m.MySQLVars["sync_binlog"] = strconv.FormatInt(syncBinlog, 10)
	m.MySQLVars["max_connections"] = strconv.FormatInt(maxConns, 10)
	m.MySQLVars["transaction_isolation"] = isolation
	m.MySQLVars["binlog_format"] = binlog
	m.MySQLVars["innodb_flush_method"] = flushMethod

	m.Notes = []string{
		"压测进程与 MySQL 跑在同一台机器上，两者抢同一批 CPU 核：吞吐偏悲观，延迟偏乐观（进程内计时不含网络）。",
		"没有网络跳数：客户端与数据库之间是 loopback，所以入队延迟只反映 SQL 与 fsync 的成本。",
		"innodb_flush_log_at_trx_commit=1 且 sync_binlog=1 时，每次提交都要落盘，提交类延迟的下限由磁盘 fsync 决定。",
	}
	return nil
}

func (m *meta) collectLoadAfter() error {
	m.LoadAvgAfter = loadAvg()
	return nil
}

func (m *meta) print() {
	fmt.Printf("## 环境\n\n")
	fmt.Printf("- 时间：%s\n", m.GeneratedAt)
	fmt.Printf("- Go：%s，CPU：%s × %d\n", m.GoVersion, m.CPUModel, m.NumCPU)
	fmt.Printf("- 内存：%d MiB；load average 开始/结束：%s / %s\n", m.MemTotalKB/1024, m.LoadAvgBefore, m.LoadAvgAfter)
	fmt.Printf("- MySQL：%s @ %s，库 %s\n", m.MySQLVersion, m.DBHost, m.DBName)
	vars := make([]string, 0, len(m.MySQLVars))
	for k, v := range m.MySQLVars {
		vars = append(vars, k+"="+v)
	}
	fmt.Printf("- MySQL 参数：%s\n", strings.Join(vars, ", "))
	fmt.Printf("- 连接池：%s\n", m.Pool)
	fmt.Println()
}

func (r *report) print() {
	if len(r.Enqueue) > 0 {
		fmt.Println("## 入队延迟")
		fmt.Println()
		fmt.Printf("| 模式 | producer | 调用数 | 吞吐 (次/s) | p50 | p95 | p99 | max |\n")
		fmt.Printf("|---|---|---|---|---|---|---|---|\n")
		for _, e := range r.Enqueue {
			fmt.Printf("| %s | %d | %d | %.1f | %.3f ms | %.3f ms | %.3f ms | %.3f ms |\n",
				e.Mode, e.Producers, e.Calls, e.Throughput,
				e.Latency.P50, e.Latency.P95, e.Latency.P99, e.Latency.Max)
		}
		if over := enqueueOverhead(r.Enqueue); over != nil {
			fmt.Printf("\n同一事务形状下多写一条任务行的代价（producer_tx 对比 producer_tx_baseline）：\n\n")
			fmt.Printf("- 每笔事务平均耗时：%.2f ms → %.2f ms（+%.2f ms）\n",
				over.MeanBaseMs, over.MeanTxMs, over.MeanDeltaMs)
			fmt.Printf("- 吞吐：%.1f → %.1f 次/s（%+.1f%%）\n",
				over.ThroughputBase, over.ThroughputTx, over.ThroughputDrop)
			fmt.Printf("\n注意：分位点不能相减（两个独立样本的 p95 之差没有统计意义），\n")
			fmt.Printf("因此这里只报均值与吞吐；要看尾部长什么样，请分别看两种模式自己的 p99。\n")
		}
		fmt.Println()
	}

	if len(r.Drain) > 0 {
		fmt.Println("## 积压排空")
		fmt.Println()
		fmt.Printf("| 并发 | payload | 任务数 | 预置 (s) | 排空 (s) | 吞吐 (task/s) |")
		fmt.Printf(" claim p50/p95/p99 | 排队 p50/p95/p99 |\n")
		fmt.Printf("|---|---|---|---|---|---|---|---|\n")
		for _, d := range r.Drain {
			fmt.Printf("| %d | %d B | %d | %.2f | %.2f | %.1f | %.2f / %.2f / %.2f ms | %.1f / %.1f / %.1f ms |\n",
				d.Workers, d.PayloadBytes, d.Tasks, d.PrefillSeconds, d.DrainSeconds, d.Throughput,
				d.ClaimLatency.P50, d.ClaimLatency.P95, d.ClaimLatency.P99,
				d.QueueWait.P50, d.QueueWait.P95, d.QueueWait.P99)
		}
		fmt.Println()
		fmt.Printf("| 并发 | payload | 锁等待次数 | 锁等待 (ms) | DB 语句数 | 池最高占用 | 池等待 | MySQL CPU 均值/峰值 (%% of 1 core) |\n")
		fmt.Printf("|---|---|---|---|---|---|---|---|\n")
		for _, d := range r.Drain {
			fmt.Printf("| %d | %d B | %d | %d | %d | %d | %d 次 / %.1f ms | %.1f%% / %.1f%% (n=%d) |\n",
				d.Workers, d.PayloadBytes, d.DB.RowLockWaits, d.DB.RowLockTimeMs, d.DB.Questions,
				d.Pool.MaxInUse, d.Pool.WaitCount, d.Pool.WaitDurationMs,
				d.CPU.Mean, d.CPU.Max, d.CPU.Samples)
		}
		fmt.Println()
	}

	if len(r.Idle) > 0 {
		fmt.Println("## 空队列轮询")
		fmt.Println()
		fmt.Printf("| 并发 | 时长 (s) | DB 语句数 | DB QPS | 每 worker QPS | MySQL CPU 均值 (%% of 1 core) |\n")
		fmt.Printf("|---|---|---|---|---|---|\n")
		for _, i := range r.Idle {
			fmt.Printf("| %d | %.1f | %d | %.2f | %.3f | %.1f%% |\n",
				i.Workers, i.Seconds, i.Questions, i.QPS, i.QPSPerWorker, i.CPU.Mean)
		}
		fmt.Println()
	}

	if len(r.Latency) > 0 {
		fmt.Println("## 稳态排队延迟（队列不被灌满）")
		fmt.Println()
		fmt.Printf("| 并发 | 到达间隔 | 任务数 | 入队窗口 (s) | 排队 p50/p95/p99/max | 处理 p95 | claim p95 |\n")
		fmt.Printf("|---|---|---|---|---|---|---|\n")
		for _, l := range r.Latency {
			fmt.Printf("| %d | %s | %d | %.1f | %.1f / %.1f / %.1f / %.1f ms | %.2f ms | %.2f ms |\n",
				l.Workers, l.ArrivalInterval, l.Tasks, l.WindowSeconds,
				l.QueueWait.P50, l.QueueWait.P95, l.QueueWait.P99, l.QueueWait.Max,
				l.Processing.P95, l.ClaimLatency.P95)
		}
		fmt.Println()
	}

	if r.TxCheck != nil {
		t := r.TxCheck
		fmt.Println("## 事务完整率")
		fmt.Println()
		fmt.Printf("- 事务总数 %d（提交 %d / 回滚 %d），耗时 %.1f s\n",
			t.TotalTransactions, t.Committed, t.RolledBack, t.Seconds)
		fmt.Printf("- 已提交业务行 %d，其中有对应任务的 %d ⇒ **事务完整率 %.2f%%**\n",
			t.OrdersCommitted, t.OrdersCommittedWithTask, t.IntegrityPercent)
		fmt.Printf("- 回滚后残留业务行 %d（期望 0），孤儿任务 %d（期望 0）\n",
			t.OrdersRolledBackRemaining, t.OrphanTasks)
		fmt.Println()
	}

	if r.Recovery != nil {
		v := r.Recovery
		fmt.Println("## 崩溃恢复耗时")
		fmt.Println()
		fmt.Printf("- 配置：租约 %s / 心跳 %s / 回收周期 %s / 停机预算 %s\n",
			v.LeaseDuration, v.HeartbeatInterval, v.ReapInterval, v.ShutdownTimeout)
		fmt.Printf("- 停止续租时剩余租约 %.0f ms\n", v.LeaseRemainingAtStallMs)
		fmt.Printf("- **从停止续租到另一个 Worker 跑完：%.2f s**\n", v.RecoverySeconds)
		fmt.Printf("- 可解释的上界（剩余租约 + 回收周期 + 一次轮询 + 领取执行）：%.2f s\n",
			v.DecompositionSeconds)
		fmt.Printf("- 陈旧 Worker 的写回被拒：%v（最终 status=%s，attempt=%d，Handler 执行 A=%d B=%d）\n",
			v.StaleWriteBackRejected, v.FinalStatus, v.FinalAttempt,
			v.FirstWorkerHandlerRuns, v.SecondWorkerHandlerRuns)
		fmt.Println()
	}
}

func (r *report) write(path string) error {
	if path == "" {
		return nil
	}
	data, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal report: %w", err)
	}
	data = append(data, '\n')
	if dir := filepath.Dir(path); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("create %s: %w", dir, err)
		}
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	fmt.Printf("结果已写入 %s\n", path)
	return nil
}

// --- 数据库准备 -------------------------------------------------------------

// openBenchDB 连上 DSN 指定库，库里没有就先建。
//
// 建库要连"不指定库"的连接：CREATE DATABASE 需要服务器级权限，在目标库自己的
// 连接里执行会报错。
func openBenchDB(ctx context.Context, dsn string) (*sql.DB, string, error) {
	parsed, err := gomysql.ParseDSN(dsn)
	if err != nil {
		return nil, "", fmt.Errorf("parse DSN: %w", err)
	}
	if parsed.DBName == "" {
		return nil, "", errors.New("DSN 必须带库名，例如 .../gotask_bench")
	}
	parsed.ParseTime = true
	name := parsed.DBName

	server := *parsed
	server.DBName = ""
	admin, err := sql.Open("mysql", server.FormatDSN())
	if err != nil {
		return nil, "", fmt.Errorf("open server connection: %w", err)
	}
	defer admin.Close()

	if _, err := admin.ExecContext(ctx,
		"CREATE DATABASE IF NOT EXISTS `"+name+"` CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci"); err != nil {
		return nil, "", fmt.Errorf("create database %s: %w", name, err)
	}

	db, err := sql.Open("mysql", parsed.FormatDSN())
	if err != nil {
		return nil, "", fmt.Errorf("open %s: %w", name, err)
	}
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, "", fmt.Errorf("ping %s: %w", name, err)
	}
	// 固定连接池，保证"连接池占用"这一列可解释：给得比并发数宽，让池不成为瓶颈。
	db.SetMaxOpenConns(32)
	db.SetMaxIdleConns(32)
	db.SetConnMaxLifetime(0)
	return db, name, nil
}

// prepareSchema 建任务表（走产品自己的 migration）与示例业务表。
func prepareSchema(ctx context.Context, db *sql.DB) error {
	if err := mysqlmigrations.Migrate(ctx, db); err != nil {
		return fmt.Errorf("migrate task table: %w", err)
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS `+ordersTable+` (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        note VARCHAR(64) NOT NULL,
        created_at DATETIME(6) NOT NULL,
        PRIMARY KEY (id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`); err != nil {
		return fmt.Errorf("create %s: %w", ordersTable, err)
	}
	return nil
}

func truncateAll(ctx context.Context, db *sql.DB) error {
	for _, table := range []string{"gotask_tasks", ordersTable} {
		if _, err := db.ExecContext(ctx, "TRUNCATE TABLE "+table); err != nil {
			return fmt.Errorf("truncate %s: %w", table, err)
		}
	}
	return nil
}

// --- 入队 -------------------------------------------------------------------

type enqueueResult struct {
	Mode        string  `json:"mode"`
	Producers   int     `json:"producers"`
	Calls       int     `json:"calls"`
	WallSeconds float64 `json:"wall_seconds"`
	Throughput  float64 `json:"throughput_per_s"`
	Latency     summary `json:"call_latency"`
}

const (
	modeEnqueue        = "enqueue"
	modeProducerTx     = "producer_tx"
	modeProducerTxBase = "producer_tx_baseline"
)

func runEnqueueMatrix(ctx context.Context, db *sql.DB, client *mysqlstore.Client,
	producers, calls int) ([]enqueueResult, error) {
	payload := json.RawMessage(`{"bench":"enqueue"}`)
	modes := []string{modeProducerTxBase, modeProducerTx, modeEnqueue}

	out := make([]enqueueResult, 0, len(modes))
	for _, mode := range modes {
		if err := truncateAll(ctx, db); err != nil {
			return nil, err
		}
		res, err := runEnqueue(ctx, db, client, mode, producers, calls, payload)
		if err != nil {
			return nil, err
		}
		out = append(out, res)
	}
	return out, nil
}

func runEnqueue(ctx context.Context, db *sql.DB, client *mysqlstore.Client,
	mode string, producers, calls int, payload json.RawMessage) (enqueueResult, error) {
	per := calls / producers
	if per == 0 {
		per = 1
	}

	lat := make([][]time.Duration, producers)
	errs := make([]error, producers)

	start := time.Now()
	var wg sync.WaitGroup
	wg.Add(producers)
	for i := 0; i < producers; i++ {
		go func(i int) {
			defer wg.Done()
			samples := make([]time.Duration, 0, per)
			for j := 0; j < per; j++ {
				t0 := time.Now()
				err := enqueueOnce(ctx, db, client, mode, payload)
				samples = append(samples, time.Since(t0))
				if err != nil {
					errs[i] = err
					return
				}
			}
			lat[i] = samples
		}(i)
	}
	wg.Wait()
	wall := time.Since(start)

	for i, err := range errs {
		if err != nil {
			return enqueueResult{}, fmt.Errorf("enqueue %s producer #%d: %w", mode, i, err)
		}
	}

	var all []time.Duration
	for _, s := range lat {
		all = append(all, s...)
	}
	return enqueueResult{
		Mode:        mode,
		Producers:   producers,
		Calls:       len(all),
		WallSeconds: wall.Seconds(),
		Throughput:  float64(len(all)) / wall.Seconds(),
		Latency:     summarize(all),
	}, nil
}

func enqueueOnce(ctx context.Context, db *sql.DB, client *mysqlstore.Client,
	mode string, payload json.RawMessage) error {
	task := gorangetask.Task{
		Queue:   benchQueue,
		Type:    benchTaskType,
		Payload: payload,
	}

	switch mode {
	case modeEnqueue:
		_, err := client.Enqueue(ctx, task)
		return err

	case modeProducerTxBase, modeProducerTx:
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("begin: %w", err)
		}
		if _, err := tx.ExecContext(ctx,
			"INSERT INTO "+ordersTable+" (note, created_at) VALUES (?, UTC_TIMESTAMP(6))",
			"bench"); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("insert order: %w", err)
		}
		if mode == modeProducerTx {
			if _, err := client.EnqueueTx(ctx, tx, task); err != nil {
				_ = tx.Rollback()
				return fmt.Errorf("enqueue tx: %w", err)
			}
		}
		return tx.Commit()

	default:
		return fmt.Errorf("unknown enqueue mode %q", mode)
	}
}

// enqueueOverhead 返回"同一事务形状下多写一条任务行"的代价。
//
// **只报均值与吞吐，不报分位点之差**：p95 是两个独立样本各自的 95 分位点，相减
// 得到的数没有统计意义（实测里它一度给出 "+2.0 ms"，而均值差是 +13.4 ms）。要说
// "多花多少时间"，均值是可加的；要"看尾部长什么样"，就分别看两种模式自己的 p99。
func enqueueOverhead(results []enqueueResult) *overhead {
	var tx, base *enqueueResult
	for i := range results {
		switch results[i].Mode {
		case modeProducerTx:
			tx = &results[i]
		case modeProducerTxBase:
			base = &results[i]
		}
	}
	if tx == nil || base == nil {
		return nil
	}
	return &overhead{
		MeanBaseMs:     round3(base.Latency.Mean),
		MeanTxMs:       round3(tx.Latency.Mean),
		MeanDeltaMs:    round3(tx.Latency.Mean - base.Latency.Mean),
		ThroughputBase: round3(base.Throughput),
		ThroughputTx:   round3(tx.Throughput),
		ThroughputDrop: round3((tx.Throughput - base.Throughput) / base.Throughput * 100),
	}
}

type overhead struct {
	MeanBaseMs     float64 `json:"mean_baseline_ms"`
	MeanTxMs       float64 `json:"mean_with_task_ms"`
	MeanDeltaMs    float64 `json:"mean_delta_ms"`
	ThroughputBase float64 `json:"throughput_baseline_per_s"`
	ThroughputTx   float64 `json:"throughput_with_task_per_s"`
	ThroughputDrop float64 `json:"throughput_change_percent"`
}

// --- 排空 -------------------------------------------------------------------

type drainResult struct {
	Workers        int              `json:"workers"`
	PayloadBytes   int              `json:"payload_bytes"`
	Tasks          int              `json:"tasks"`
	PrefillSeconds float64          `json:"prefill_seconds"`
	DrainSeconds   float64          `json:"drain_seconds"`
	Throughput     float64          `json:"throughput_per_s"`
	ClaimLatency   summary          `json:"claim_latency"`
	QueueWait      summary          `json:"queue_wait"`
	Processing     summary          `json:"processing"`
	Outcomes       counts           `json:"outcomes"`
	DB             dbCounters       `json:"db_delta"`
	Pool           poolStats        `json:"pool"`
	CPU            cpuStats         `json:"mysql_cpu"`
	FinalStatuses  map[string]int64 `json:"final_status_counts"`
}

func runDrainMatrix(ctx context.Context, db *sql.DB, client *mysqlstore.Client,
	workers, sizes []int, tasks int, container string, timeout time.Duration) ([]drainResult, error) {
	out := make([]drainResult, 0, len(workers)*len(sizes))
	for _, size := range sizes {
		for _, w := range workers {
			res, err := runDrain(ctx, db, client, w, size, tasks, container, timeout)
			if err != nil {
				return nil, fmt.Errorf("drain workers=%d payload=%d: %w", w, size, err)
			}
			out = append(out, res)
		}
	}
	return out, nil
}

func runDrain(ctx context.Context, db *sql.DB, client *mysqlstore.Client,
	workers, payloadBytes, tasks int, container string, timeout time.Duration) (drainResult, error) {
	if err := truncateAll(ctx, db); err != nil {
		return drainResult{}, err
	}
	payload := payloadOfSize(payloadBytes)

	prefillStart := time.Now()
	if err := prefill(ctx, db, client, payload, tasks); err != nil {
		return drainResult{}, err
	}
	prefillTook := time.Since(prefillStart)

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
		return drainResult{}, fmt.Errorf("register handler: %w", err)
	}
	w, err := gorangetask.NewWorker(client, reg, cfg)
	if err != nil {
		return drainResult{}, fmt.Errorf("NewWorker: %w", err)
	}

	cpu := startCPUProbe(container)
	pool := startPoolProbe(db)
	before, err := readCounters(ctx, db)
	if err != nil {
		return drainResult{}, err
	}

	runCtx, cancel := context.WithCancel(ctx)
	runDone := make(chan error, 1)

	drainStart := time.Now()
	go func() { runDone <- w.Run(runCtx) }()

	if err := waitDrained(ctx, rec, tasks, timeout); err != nil {
		cancel()
		<-runDone
		return drainResult{}, err
	}
	drainTook := time.Since(drainStart)

	cancel()
	select {
	case err := <-runDone:
		if err != nil {
			return drainResult{}, fmt.Errorf("worker Run: %w", err)
		}
	case <-time.After(30 * time.Second):
		return drainResult{}, errors.New("worker 未在停机预算内退出")
	}

	after, err := readCounters(ctx, db)
	if err != nil {
		return drainResult{}, err
	}
	poolSnap := pool.stop()
	cpuSnap := cpu.stop()
	claim, waited, processing := rec.samples()

	statuses, err := finalStatuses(ctx, db)
	if err != nil {
		return drainResult{}, err
	}
	// 计数必须自洽：指标说完成 N 条，库里也必须是 N 条 succeeded。
	if statuses["succeeded"] != int64(tasks) {
		return drainResult{}, fmt.Errorf("库里有 %d 条 succeeded, 期望 %d：指标与持久状态不一致",
			statuses["succeeded"], tasks)
	}

	return drainResult{
		Workers:        workers,
		PayloadBytes:   payloadBytes,
		Tasks:          tasks,
		PrefillSeconds: prefillTook.Seconds(),
		DrainSeconds:   drainTook.Seconds(),
		Throughput:     float64(tasks) / drainTook.Seconds(),
		ClaimLatency:   summarize(claim),
		QueueWait:      summarize(waited),
		Processing:     summarize(processing),
		Outcomes:       rec.snapshot(),
		DB:             after.minus(before),
		Pool:           poolSnap,
		CPU:            cpuSnap,
		FinalStatuses:  statuses,
	}, nil
}

// prefill 用**一个事务**批量写入 backlog。
//
// 为什么不逐条 Enqueue：那会把 1000 次 fsync 算进"预置耗时"，而预置不是被测量的
// 对象。批量入队仍然走产品路径（EnqueueTx），只是共用一次提交。
func prefill(ctx context.Context, db *sql.DB, client *mysqlstore.Client, payload json.RawMessage, tasks int) error {
	const batch = 200
	for done := 0; done < tasks; done += batch {
		n := batch
		if done+n > tasks {
			n = tasks - done
		}
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("begin prefill tx: %w", err)
		}
		for i := 0; i < n; i++ {
			if _, err := client.EnqueueTx(ctx, tx, gorangetask.Task{
				Queue:   benchQueue,
				Type:    benchTaskType,
				Payload: payload,
			}); err != nil {
				_ = tx.Rollback()
				return fmt.Errorf("prefill enqueue: %w", err)
			}
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit prefill: %w", err)
		}
	}
	return nil
}

// waitDrained 等到"成功 + 死信"的总数达到 tasks。
//
// 用指标计数而不是轮询库表：每 20ms 发一条 COUNT 会自己变成可观的 QPS，把
// "DB 语句数"这一列污染掉。库表的最终计数在结束时核对一次即可。
func waitDrained(ctx context.Context, rec *recorder, tasks int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

	for {
		c := rec.snapshot()
		if c.Succeeded+c.Dead >= int64(tasks) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("排空超时（%s）：已完成 %d/%d（succeeded=%d dead=%d retried=%d）",
				timeout, c.Succeeded+c.Dead, tasks, c.Succeeded, c.Dead, c.Retried)
		}
	}
}

func finalStatuses(ctx context.Context, db *sql.DB) (map[string]int64, error) {
	rows, err := db.QueryContext(ctx,
		"SELECT status, COUNT(*) FROM gotask_tasks GROUP BY status")
	if err != nil {
		return nil, fmt.Errorf("count statuses: %w", err)
	}
	defer rows.Close()

	out := map[string]int64{}
	for rows.Next() {
		var status string
		var n int64
		if err := rows.Scan(&status, &n); err != nil {
			return nil, fmt.Errorf("scan status count: %w", err)
		}
		out[status] = n
	}
	return out, rows.Err()
}

// --- 空队列轮询 -------------------------------------------------------------

type idleResult struct {
	Workers      int      `json:"workers"`
	Seconds      float64  `json:"seconds"`
	Questions    int64    `json:"db_questions"`
	QPS          float64  `json:"db_qps"`
	QPSPerWorker float64  `json:"db_qps_per_worker"`
	CPU          cpuStats `json:"mysql_cpu"`
}

func runIdleMatrix(ctx context.Context, db *sql.DB, client *mysqlstore.Client,
	workers []int, duration time.Duration, container string) ([]idleResult, error) {
	out := make([]idleResult, 0, len(workers))
	for _, w := range workers {
		res, err := runIdle(ctx, db, client, w, duration, container)
		if err != nil {
			return nil, fmt.Errorf("idle workers=%d: %w", w, err)
		}
		out = append(out, res)
	}
	return out, nil
}

func runIdle(ctx context.Context, db *sql.DB, client *mysqlstore.Client,
	workers int, duration time.Duration, container string) (idleResult, error) {
	if err := truncateAll(ctx, db); err != nil {
		return idleResult{}, err
	}

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
	}
	reg := gorangetask.NewRegistry()
	if err := reg.Register(benchTaskType, func(context.Context, json.RawMessage) error { return nil }); err != nil {
		return idleResult{}, fmt.Errorf("register handler: %w", err)
	}
	w, err := gorangetask.NewWorker(client, reg, cfg)
	if err != nil {
		return idleResult{}, fmt.Errorf("NewWorker: %w", err)
	}

	cpu := startCPUProbe(container)

	runCtx, cancel := context.WithCancel(ctx)
	runDone := make(chan error, 1)
	go func() { runDone <- w.Run(runCtx) }()

	// 先让退避长到稳态（250ms→500ms→1s→2s），再开始计数，否则窗口里混进起步阶段
	// 那段密集轮询，QPS 会偏高。
	time.Sleep(5 * time.Second)

	measureStart := time.Now()
	from, err := readCounters(ctx, db)
	if err != nil {
		cancel()
		return idleResult{}, err
	}
	time.Sleep(duration - 5*time.Second)
	to, err := readCounters(ctx, db)
	if err != nil {
		cancel()
		return idleResult{}, err
	}
	measured := time.Since(measureStart)

	cancel()
	select {
	case err := <-runDone:
		if err != nil {
			return idleResult{}, fmt.Errorf("worker Run: %w", err)
		}
	case <-time.After(30 * time.Second):
		return idleResult{}, errors.New("worker 未在停机预算内退出")
	}
	cpuSnap := cpu.stop()

	delta := to.minus(from)
	// 读数本身各算一条语句，扣掉，别把探针自己的开销算进轮询成本。
	delta.Questions -= 2
	if delta.Questions < 0 {
		delta.Questions = 0
	}
	return idleResult{
		Workers:      workers,
		Seconds:      measured.Seconds(),
		Questions:    delta.Questions,
		QPS:          float64(delta.Questions) / measured.Seconds(),
		QPSPerWorker: float64(delta.Questions) / measured.Seconds() / float64(workers),
		CPU:          cpuSnap,
	}, nil
}

// --- 小工具 -----------------------------------------------------------------

func payloadOfSize(n int) json.RawMessage {
	// 构造一个约 n 字节的合法 JSON：{"blob":"aaa..."}。
	const overhead = len(`{"blob":""}`)
	filler := n - overhead
	if filler < 0 {
		filler = 0
	}
	return json.RawMessage(`{"blob":"` + strings.Repeat("a", filler) + `"}`)
}

func splitInts(s string) []int {
	parts := strings.Split(s, ",")
	out := make([]int, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		v, err := strconv.Atoi(p)
		if err != nil {
			continue
		}
		out = append(out, v)
	}
	return out
}

func round3(v float64) float64 {
	return math.Round(v*1000) / 1000
}

// splitDurations 解析 "100ms,500ms" 这样的间隔列表。
//
// 解析失败的项**报错而不是静默丢弃**：多一个或少一个到达间隔，测出来的样本量就
// 完全不同，静默跳过会让"我明明写了 500ms"变成一次看不见的空跑。
func splitDurations(s string) ([]time.Duration, error) {
	parts := strings.Split(s, ",")
	out := make([]time.Duration, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		d, err := time.ParseDuration(p)
		if err != nil {
			return nil, fmt.Errorf("解析到达间隔 %q: %w", p, err)
		}
		if d <= 0 {
			return nil, fmt.Errorf("到达间隔 %q 必须大于 0", p)
		}
		out = append(out, d)
	}
	return out, nil
}

func firstLineValue(path, key string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return "unknown"
	}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, key) {
			if _, v, ok := strings.Cut(line, ":"); ok {
				return strings.TrimSpace(v)
			}
		}
	}
	return "unknown"
}

func memTotalKB() int64 {
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "MemTotal:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				n, _ := strconv.ParseInt(fields[1], 10, 64)
				return n
			}
		}
	}
	return 0
}

func loadAvg() string {
	data, err := os.ReadFile("/proc/loadavg")
	if err != nil {
		return "unknown"
	}
	fields := strings.Fields(string(data))
	if len(fields) < 3 {
		return "unknown"
	}
	return strings.Join(fields[:3], " ")
}

// hostOf 只取出 host:port，用来在报告里说明数据库在哪；DSN 里的口令不进结果文件。
func hostOf(dsn string) string {
	parsed, err := gomysql.ParseDSN(dsn)
	if err != nil {
		return "unknown"
	}
	return parsed.Addr
}
