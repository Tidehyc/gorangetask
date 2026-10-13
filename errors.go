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

	// ErrInvalidConfig 表示配置不合法。Validate 返回的错误可用 errors.Is 匹配它。
	ErrInvalidConfig = errors.New("gorangetask: invalid config")

	// ErrLeaseLost 表示当前执行者已失去任务租约，无法写回状态。
	// 出现该错误时，本次执行的结果不得覆盖任务记录。
	ErrLeaseLost = errors.New("gorangetask: lease lost")
)

// permanentError 给错误打上"重试没有意义"的标记。
//
// 用私有类型而不是新增一个要 %w 包装的公开哨兵错误：调用方只需记住
// Permanent / IsPermanent 两个函数，不会因为漏包一层而把永久失败
// 当成可重试错误——那会让任务白白重试到次数耗尽。
type permanentError struct {
	err error
}

func (e permanentError) Error() string {
	return "permanent failure: " + e.err.Error()
}

// Unwrap 让 errors.Is / errors.As 仍能在错误链上找到被标记的原始错误。
func (e permanentError) Unwrap() error { return e.err }

// Permanent 把 err 标记为永久失败：重试不会让它变好。
// Worker 见到带该标记的错误会让任务直接进入 dead，而不是回到 pending 等下一次重试。
//
// err 为 nil 时返回 nil，因此 Handler 里写 return gorangetask.Permanent(err) 是安全的。
// 重复标记也是安全的：IsPermanent 仍返回 true，错误链不受影响。
//
//	// 参数本身有问题，重试多少次都一样
//	return gorangetask.Permanent(fmt.Errorf("order %d does not exist", id))
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return permanentError{err: err}
}

// IsPermanent 报告 err 的错误链上是否带永久失败标记。
// 永久失败标记可以穿过 fmt.Errorf 的多层 %w 包装。
func IsPermanent(err error) bool {
	var target permanentError
	return errors.As(err, &target)
}
