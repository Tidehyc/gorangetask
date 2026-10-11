package gorangetask

import "errors"

// 稳定、可判断的公开错误。调用方应使用 errors.Is 判断，而不是比较错误字符串。
var (
	// ErrDuplicateTask 表示入队时命中了同队列内已存在的幂等键。
	// 冲突时不会覆盖旧任务，也不自动返回旧任务 ID。
	ErrDuplicateTask = errors.New("gorangetask: duplicate task")

	// ErrNotFound 表示按 ID 查询或操作的任务不存在。
	ErrNotFound = errors.New("gorangetask: task not found")

	// ErrNotPending 表示试图取消一个非 pending 状态的任务。
	// MVP 只允许取消尚未被领取的任务。
	ErrNotPending = errors.New("gorangetask: task is not pending")

	// ErrNoHandler 表示任务没有匹配到已注册的 Handler。
	ErrNoHandler = errors.New("gorangetask: no handler registered for task type")

	// ErrInvalidPayload 表示任务参数不是合法 JSON，或超过了配置的 payload 上限。
	ErrInvalidPayload = errors.New("gorangetask: invalid payload")

	// ErrInvalidTask 表示任务定义本身不合法（例如空 task type、非正数 max attempts）。
	ErrInvalidTask = errors.New("gorangetask: invalid task")

	// ErrLeaseLost 表示当前执行者已失去任务租约，无法写回状态。
	// 出现该错误时，本次执行的结果不得覆盖任务记录。
	ErrLeaseLost = errors.New("gorangetask: lease lost")
)
