package gorangetask

import (
	"encoding/json"
	"time"
)

// TaskID 是任务的唯一标识，对应任务表的主键。
type TaskID uint64

// Status 表示任务在其生命周期中的状态。
//
// 状态迁移：
//
//	pending -> running -> succeeded
//	                   -> pending（可重试错误，等待下一次 run_at）
//	                   -> dead   （永久错误或尝试次数耗尽）
//	pending -> canceled        （仅限尚未被领取的任务）
//	running -> pending         （租约过期，由回收器处理）
//	running -> dead            （租约过期且尝试次数已耗尽）
type Status string

const (
	// StatusPending 表示任务已创建、等待被 Worker 领取。失败后等待重试的
	// 任务同样处于此状态，通过 RunAt 表示最早可领取时间。
	StatusPending Status = "pending"
	// StatusRunning 表示任务已被某个 Worker 领取并持有一个有效租约。
	StatusRunning Status = "running"
	// StatusSucceeded 表示 Handler 成功执行并已写回终态。
	StatusSucceeded Status = "succeeded"
	// StatusDead 表示任务进入死信：永久失败或尝试次数耗尽，不再自动重试。
	StatusDead Status = "dead"
	// StatusCanceled 表示尚未被领取的任务被调用方取消。
	StatusCanceled Status = "canceled"
)

// IsTerminal 报告该状态是否为终态（不会再自动迁移）。
func (s Status) IsTerminal() bool {
	switch s {
	case StatusSucceeded, StatusDead, StatusCanceled:
		return true
	default:
		return false
	}
}

// IsValid 报告该状态是否为已定义的合法状态。
func (s Status) IsValid() bool {
	switch s {
	case StatusPending, StatusRunning, StatusSucceeded, StatusDead, StatusCanceled:
		return true
	default:
		return false
	}
}

// Task 描述一个待入队的任务。它是调用方构造并提交的输入类型。
type Task struct {
	// Queue 是目标队列名。空值是否被接受由实现决定；推荐显式指定。
	Queue string
	// Type 是任务类型名，用于映射到已注册的 Handler。必填。
	Type string
	// Payload 是任务的 JSON 参数。必须是合法 JSON，且不超过配置的 payload 上限。
	Payload json.RawMessage
	// RunAt 是任务最早可被领取的时间。零值表示立即执行。
	// 非零值应由调用方以 UTC 传入。
	RunAt time.Time
	// MaxAttempts 是最大尝试次数。零值表示使用配置默认值；必须大于 0。
	MaxAttempts int
	// Timeout 是本次执行的 Handler 超时。零值表示使用 Worker 配置的默认
	// Handler 超时；非零值覆盖该默认值。
	//
	// 超时通过传给 Handler 的 context.Context 的 deadline 实现，与租约丢失、
	// shutdown 复用同一套协作式取消机制。超时只请求 Handler 停止，不保证
	// 撤销已经发生的外部副作用。
	Timeout time.Duration
	// IdempotencyKey 是可选的幂等键。同队列内相同的非空键在任务记录保留
	// 期间最多只能创建一条有效任务。
	IdempotencyKey string
}

// TaskInfo 描述一条任务的当前状态，是只读的查询结果。
type TaskInfo struct {
	// ID 是任务标识。
	ID TaskID
	// Queue 是任务所属队列。
	Queue string
	// Type 是任务类型名。
	Type string
	// Payload 是任务的 JSON 参数。
	Payload json.RawMessage
	// Status 是任务当前状态。
	Status Status
	// RunAt 是任务最早可被领取的时间（UTC）。
	RunAt time.Time
	// Attempt 是已发生的领取/执行尝试次数。
	Attempt int
	// MaxAttempts 是最大尝试次数。
	MaxAttempts int
	// Timeout 是本次执行的 Handler 超时；未设置时为 0，表示使用 Worker 配置的
	// 默认 Handler 超时。
	Timeout time.Duration
	// LastError 是最近一次错误的摘要；无错误时为空。
	LastError string
	// IdempotencyKey 是任务的幂等键；未设置时为空。
	IdempotencyKey string
	// CreatedAt 是任务创建时间（UTC）。
	CreatedAt time.Time
	// UpdatedAt 是任务最近更新时间（UTC）。
	UpdatedAt time.Time
	// FinishedAt 是任务进入终态的时间（UTC）；未进入终态时为零值。
	FinishedAt time.Time
}
