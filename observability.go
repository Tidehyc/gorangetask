package gorangetask

import (
	"context"
	"time"
)

// Level 是结构化日志的级别。
//
// 只保留四档，且与主流日志库一一对应：接 log/slog 时可直接映射到 slog.Level，
// 接 zap / zerolog 时映射到各自的同名方法。刻意不提供 Fatal / Panic 一类级别
// ——库没有"决定进程去死"的权力。
type Level int

const (
	// LevelDebug 是排查用的细节：每轮领取、回收与清理的逐次结果。
	LevelDebug Level = iota
	// LevelInfo 是正常的生命周期事件：启动、停机、单条任务成功。
	LevelInfo
	// LevelWarn 是预期内但值得关注的事件：任务失败待重试、进入死信、失去租约。
	LevelWarn
	// LevelError 是与单条任务无关的基础设施错误：领取、写回、回收、清理失败。
	LevelError
)

// String 返回级别的名字（"debug" / "info" / "warn" / "error"）。
// 未定义的取值返回 "unknown"，而不是 panic——日志实现不该因为一个意外的
// 级别把整个 Worker 打挂。
func (l Level) String() string {
	switch l {
	case LevelDebug:
		return "debug"
	case LevelInfo:
		return "info"
	case LevelWarn:
		return "warn"
	case LevelError:
		return "error"
	default:
		return "unknown"
	}
}

// Logger 接收库产生的结构化日志。
//
// 它刻意是最小接口——一个方法、一条消息加一串键值对——不绑定任何日志库：
// 接 log/slog 是十几行适配，接 zap / zerolog 同样简单。库也允许完全不配
// 日志（Config.Logger 为 nil 时静默），因为"要不要日志、往哪写"是部署决定。
//
// # 与指标的分工
//
// 日志面向**人**：它带 task ID 与错误原文，用来追溯"这一条任务到底发生了什么"。
// 指标面向**聚合**：它不带任何高基数维度（需求 §8）。两者不要混用——把
// task ID 塞进指标标签会让监控系统的基数爆炸。
//
// # 库不打印 payload
//
// 库不会把任务 payload 写进日志或错误消息（需求 §8「不默认打印完整 payload」）。
// 但 Handler 返回的错误文本会被写进日志与 last_error 列，而它可能含敏感数据；
// 脱敏是部署方的职责——在 Logger 实现里过滤，或让 Handler 不要返回敏感内容。
type Logger interface {
	// Log 记录一条日志。ctx 来自产生该事件的调用链，可用来带上 trace 信息；
	// kv 是交替出现的键值对（键为 string，值为任意类型）。
	//
	// 实现必须**并发安全**：多个 Worker goroutine 会同时调用它。实现也不应
	// 长时间阻塞——它被调用在任务处理路径上。
	Log(ctx context.Context, level Level, msg string, kv ...any)
}

// 日志字段名。这些名字是稳定契约，与需求 §8 列出的字段一一对应
// （task_id / task_type / queue / attempt / lease_epoch / worker_id），
// 便于接入方在建索引、写告警查询时直接引用。**payload 不在其中**。
const (
	logKeyTaskID      = "task_id"
	logKeyTaskType    = "task_type"
	logKeyQueue       = "queue"
	logKeyAttempt     = "attempt"
	logKeyMaxAttempts = "max_attempts"
	logKeyLeaseEpoch  = "lease_epoch"
	logKeyWorkerID    = "worker_id"
	logKeyError       = "error"
)

// Outcome 是任务一次执行的去向。
type Outcome string

const (
	// OutcomeSucceeded 表示 Handler 成功返回且结果已写回。
	OutcomeSucceeded Outcome = "succeeded"
	// OutcomeRetried 表示这次失败后任务被放回 pending 等待重试。
	OutcomeRetried Outcome = "retried"
	// OutcomeDead 表示任务进入死信：永久失败或尝试次数已耗尽。
	OutcomeDead Outcome = "dead"
)

// Metrics 接收库产生的指标事件。
//
// # 为什么是具名事件，而不是通用的 Counter / Histogram 原语
//
// 指标名与标签集合由**库**固定，调用方无法（也无法不小心）把 task ID、用户 ID
// 这类高基数维度塞进标签——需求 §8 明确禁止这件事，而"提供 AddCounter(name,
// labels)"式的原语恰好把犯错的机会留给每一个使用方。要用 Prometheus 时，在实现
// 里把每个事件映射到自己的指标即可：
//
//	func (m *prom) TaskFinished(queue, taskType string, outcome gorangetask.Outcome, d time.Duration) {
//		m.tasks.WithLabelValues(queue, taskType, string(outcome)).Inc()
//		m.duration.WithLabelValues(queue, taskType).Observe(d.Seconds())
//	}
//
// 标签里的 queue 与 taskType 都是调用方自己定义的有限集合，因此天然有界。
//
// # 实现约定
//
//   - 所有方法必须**并发安全**：多个 Worker goroutine 会同时调用。
//   - 实现**不应阻塞**：它们在任务处理路径上被调用。
//   - 不想实现全部方法时，嵌入 NopMetrics 后只覆盖关心的几个即可。
//
// # 库不提供的指标
//
// 两条需求条目落在库外，因为它们要么需要库主动轮询、要么本来就属于宿主：
//
//   - **按队列和状态的任务数量 / 积压量**：库不主动查表。宿主可自行执行
//     `SELECT queue_name, status, COUNT(*) FROM gotask_tasks GROUP BY 1, 2`，
//     或在告警系统里用同一句话。
//   - **数据库连接池占用**：连接池由宿主创建，用 `db.Stats()` 取。
type Metrics interface {
	// TaskEnqueued 在一条任务行**被写入**后调用，Enqueue 与 EnqueueTx 都会触发。
	// queue 是**归一化之后**的队列名（调用方留空时由库填成默认队列），这是库才
	// 知道的信息。
	//
	// 注意 EnqueueTx 的局限：库无从知道调用方的事务最终是提交还是回滚（Tx 接口
	// 刻意不暴露 Commit/Rollback），因此回滚也会被计一次。要得到"真正落库"的
	// 精确计数，请在事务提交成功之后于调用方侧自行记录。
	TaskEnqueued(queue string)

	// TaskCanceled 在一条 pending 任务被成功取消后调用。
	//
	// 刻意不带队列标签：取消只按任务 ID 进行，要拿到队列名就得为此多回读一次。
	// 取消是调用方自己的动作，需要按队列拆分时在调用点记即可。
	TaskCanceled()

	// TaskClaimed 在一条任务被领取后调用。
	TaskClaimed(queue, taskType string)

	// TaskFinished 在一条任务的执行结果**成功写回**后调用，outcome 是它的去向；
	// d 是 Handler 的执行耗时。写回被 fencing 拒绝时不会调用它——那时任务的
	// 去向由别的持有者决定，本次的结果没有落地。
	TaskFinished(queue, taskType string, outcome Outcome, d time.Duration)

	// TaskWaited 在一条任务被领取时报告它的排队等待时长：从"最早可被领取"
	// （created_at 与 run_at 的较晚者）到实际被领取，由**数据库时钟**计算，
	// 因此不受各机器时钟偏差影响。
	TaskWaited(queue, taskType string, d time.Duration)

	// LeaseLost 在本次执行失去租约时调用，两个场景共用：续租被拒绝，或写回
	// 因租约已易主被拒。两者在同一次执行里互斥（续租被拒后本次不再写回），
	// 因此一次失租只会产生一次调用。
	LeaseLost(queue, taskType string)

	// HandlerPanicked 在 Handler panic 被兜底捕获后调用。它是一次普通的失败
	// （走重试或死信，也会计入 TaskFinished），这个计数是额外的诊断维度。
	HandlerPanicked(queue, taskType string)

	// HandlerTimedOut 在 Handler 因执行超时被取消后调用。它同样是一次普通失败，
	// 单独计数的理由是"超时"通常意味着容量或超时配置需要调整。
	HandlerTimedOut(queue, taskType string)

	// TasksReaped 报告一轮过期租约回收的结果：交还队列的条数与直接判死的条数。
	// 非零即说明有 Worker 没能正常收尾（崩溃、停机超时，或 Handler 不理会取消）。
	TasksReaped(toPending, toDead int)

	// TasksCleaned 报告一轮终态清理删除的行数。
	TasksCleaned(status Status, deleted int64)

	// ClaimLatency 报告一次领取调用（含事务与写回）的往返耗时，用于观察
	// 领取语句本身是否成为瓶颈。
	ClaimLatency(d time.Duration)

	// ConcurrencyInUse 报告当前正在执行 Handler 的数量，用于观察并发池水位。
	ConcurrencyInUse(n int)
}

// NopMetrics 是 Metrics 的空实现：所有方法什么都不做。
//
// 把它嵌进自己的类型里，就只需要覆盖关心的事件，不必为其余方法写空壳：
//
//	type myMetrics struct {
//		gorangetask.NopMetrics
//		succeeded prometheus.Counter
//	}
//
//	func (m *myMetrics) TaskFinished(q, t string, o gorangetask.Outcome, _ time.Duration) {
//		if o == gorangetask.OutcomeSucceeded {
//			m.succeeded.Inc()
//		}
//	}
type NopMetrics struct{}

func (NopMetrics) TaskEnqueued(string)                                 {}
func (NopMetrics) TaskCanceled()                                       {}
func (NopMetrics) TaskClaimed(string, string)                          {}
func (NopMetrics) TaskFinished(string, string, Outcome, time.Duration) {}
func (NopMetrics) TaskWaited(string, string, time.Duration)            {}
func (NopMetrics) LeaseLost(string, string)                            {}
func (NopMetrics) HandlerPanicked(string, string)                      {}
func (NopMetrics) HandlerTimedOut(string, string)                      {}
func (NopMetrics) TasksReaped(int, int)                                {}
func (NopMetrics) TasksCleaned(Status, int64)                          {}
func (NopMetrics) ClaimLatency(time.Duration)                          {}
func (NopMetrics) ConcurrencyInUse(int)                                {}

// 编译期断言：NopMetrics 覆盖了接口的全部方法。
var _ Metrics = NopMetrics{}
