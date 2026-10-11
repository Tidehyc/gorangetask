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
