package gorangetask

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
)

// Handler 是任务的处理函数。payload 为任务入队时提交的 JSON 参数。
//
// Handler 可能被重复调用（至少一次语义），因此实现必须幂等。
// ctx 在以下情况会被取消：Worker 关闭、Handler 超时、或租约丢失。
// ctx 取消是协作式的，不会撤销已经发生的外部副作用。
type Handler func(ctx context.Context, payload json.RawMessage) error

// Registry 保存 task type 到 Handler 的映射。它在 Worker 启动前完成注册，
// 之后被并发只读访问，因此内部使用读写锁保护。
//
// 同一个 task type 不允许重复注册。
type Registry struct {
	mu       sync.RWMutex
	handlers map[string]Handler
}

// NewRegistry 返回一个空的 Registry。
func NewRegistry() *Registry {
	return &Registry{handlers: make(map[string]Handler)}
}

// Register 为 taskType 注册 handler。taskType 不能为空，handler 不能为 nil，
// 且同一个 taskType 不能重复注册；违反时返回 ErrInvalidTask。
func (r *Registry) Register(taskType string, handler Handler) error {
	if taskType == "" {
		return fmt.Errorf("%w: empty task type", ErrInvalidTask)
	}
	if handler == nil {
		return fmt.Errorf("%w: nil handler for task type %q", ErrInvalidTask, taskType)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.handlers[taskType]; exists {
		return fmt.Errorf("%w: task type %q already registered", ErrInvalidTask, taskType)
	}
	r.handlers[taskType] = handler
	return nil
}

// Lookup 返回 taskType 对应的 Handler。未注册时返回 ErrNoHandler。
func (r *Registry) Lookup(taskType string) (Handler, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	handler, ok := r.handlers[taskType]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrNoHandler, taskType)
	}
	return handler, nil
}

// Has 报告 taskType 是否已注册。
func (r *Registry) Has(taskType string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	_, ok := r.handlers[taskType]
	return ok
}

// Resolve 返回 taskType 对应的 Handler，供执行路径使用。
//
// 与 Lookup 的唯一区别是错误分类：未注册的类型返回的错误带**永久失败**标记
// （IsPermanent 为 true），同时仍可用 errors.Is(err, ErrNoHandler) 判断。
// 这样分类是有意的——task_type 是入队时就写进库里的值，重试不会让它变得可处理，
// 所以 Worker 应当让这类任务直接进入 dead，而不是反复领取、反复失败。
// 只想判断"能不能查到"、不打算据此决定任务命运的场景，用 Lookup。
func (r *Registry) Resolve(taskType string) (Handler, error) {
	handler, err := r.Lookup(taskType)
	if err != nil {
		return nil, Permanent(err)
	}
	return handler, nil
}

// TypedHandler 把接收具体类型的处理函数适配成 Handler。
//
// 它的价值不只是省一次 json.Unmarshal，而是把两件本来只能靠约定保证的事
// 变成库能保证的事——因为解码发生在库里：
//
//   - payload 不是合法 JSON，或结构与 T 对不上（字段类型不符、数字/字符串互换等），
//     返回带永久失败标记的 ErrInvalidPayload。payload 在入队时就固定了，
//     重试不会让它变成合法 JSON。
//   - 因此"无法解码 payload 的任务不能无限快速重试"这条要求有了确定的落点。
//
// fn 为 nil 时在构造期 panic：这是启动时的配置错误，应该在 Worker 起来之前
// 就暴露，而不是等某个任务执行时才变成一次任务失败。
//
// 例：
//
//	type SendEmail struct {
//		To   string `json:"to"`
//		Body string `json:"body"`
//	}
//	if err := reg.Register("email.send", gorangetask.TypedHandler(
//		func(ctx context.Context, in SendEmail) error {
//			return send(ctx, in.To, in.Body)
//		},
//	)); err != nil {
//		return err
//	}
func TypedHandler[T any](fn func(ctx context.Context, payload T) error) Handler {
	if fn == nil {
		panic("gorangetask: TypedHandler called with a nil handler function")
	}
	return func(ctx context.Context, raw json.RawMessage) error {
		var payload T
		if err := json.Unmarshal(raw, &payload); err != nil {
			return Permanent(fmt.Errorf("%w: cannot decode payload: %w", ErrInvalidPayload, err))
		}
		return fn(ctx, payload)
	}
}

// Types 返回已注册的所有 task type，按字典序排序。
func (r *Registry) Types() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	types := make([]string, 0, len(r.handlers))
	for t := range r.handlers {
		types = append(types, t)
	}
	sort.Strings(types)
	return types
}

// Len 返回已注册的 Handler 数量。
func (r *Registry) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return len(r.handlers)
}
