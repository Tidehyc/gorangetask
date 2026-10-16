package gorangetask

import "context"

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
