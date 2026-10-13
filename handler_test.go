package gorangetask

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
)

func noopHandler(context.Context, json.RawMessage) error { return nil }

func TestRegistryRegisterAndLookup(t *testing.T) {
	r := NewRegistry()

	handler := func(context.Context, json.RawMessage) error { return nil }
	if err := r.Register("email.send", handler); err != nil {
		t.Fatalf("Register() unexpected error: %v", err)
	}

	got, err := r.Lookup("email.send")
	if err != nil {
		t.Fatalf("Lookup() unexpected error: %v", err)
	}
	if got == nil {
		t.Fatal("Lookup() returned nil handler")
	}
}

func TestRegistryRejectsInvalidRegistration(t *testing.T) {
	tests := []struct {
		name     string
		taskType string
		handler  Handler
	}{
		{"empty type", "", noopHandler},
		{"nil handler", "email.send", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewRegistry()
			err := r.Register(tt.taskType, tt.handler)
			if !errors.Is(err, ErrInvalidTask) {
				t.Fatalf("Register() error = %v, want ErrInvalidTask", err)
			}
		})
	}
}

func TestRegistryRejectsDuplicateRegistration(t *testing.T) {
	r := NewRegistry()
	if err := r.Register("email.send", noopHandler); err != nil {
		t.Fatalf("first Register() unexpected error: %v", err)
	}
	if err := r.Register("email.send", noopHandler); !errors.Is(err, ErrInvalidTask) {
		t.Fatalf("duplicate Register() error = %v, want ErrInvalidTask", err)
	}
}

func TestRegistryLookupUnknownType(t *testing.T) {
	r := NewRegistry()
	if _, err := r.Lookup("missing"); !errors.Is(err, ErrNoHandler) {
		t.Fatalf("Lookup() error = %v, want ErrNoHandler", err)
	}
}

func TestRegistryHasAndLen(t *testing.T) {
	r := NewRegistry()
	if r.Has("a") || r.Len() != 0 {
		t.Fatal("new registry should be empty")
	}

	_ = r.Register("a", noopHandler)
	_ = r.Register("b", noopHandler)

	if !r.Has("a") || r.Has("c") {
		t.Error("Has() returned wrong result")
	}
	if r.Len() != 2 {
		t.Errorf("Len() = %d, want 2", r.Len())
	}
}

func TestRegistryTypesSorted(t *testing.T) {
	r := NewRegistry()
	for _, tt := range []string{"c", "a", "b"} {
		if err := r.Register(tt, noopHandler); err != nil {
			t.Fatalf("Register(%q) error: %v", tt, err)
		}
	}

	got := r.Types()
	want := []string{"a", "b", "c"}
	if len(got) != len(want) {
		t.Fatalf("Types() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Types() = %v, want %v", got, want)
		}
	}
}

func TestRegistryConcurrentAccess(t *testing.T) {
	r := NewRegistry()
	const n = 50

	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			taskType := string(rune('A'+i%26)) + "-type"
			_ = r.Register(taskType, noopHandler)
			_, _ = r.Lookup(taskType)
			_ = r.Has(taskType)
			_ = r.Types()
		}(i)
	}
	wg.Wait()
}

// TestRegistryResolve 验证执行路径使用的解析入口：未注册类型要同时满足
// errors.Is(err, ErrNoHandler) 与 IsPermanent(err)，因为 task_type 是入队时
// 定下的值，重试不会让它变得可处理。
func TestRegistryResolve(t *testing.T) {
	r := NewRegistry()
	if err := r.Register("email.send", noopHandler); err != nil {
		t.Fatalf("Register() unexpected error: %v", err)
	}

	t.Run("registered type", func(t *testing.T) {
		handler, err := r.Resolve("email.send")
		if err != nil {
			t.Fatalf("Resolve() unexpected error: %v", err)
		}
		if handler == nil {
			t.Fatal("Resolve() returned nil handler")
		}
	})

	t.Run("unknown type is a permanent failure", func(t *testing.T) {
		handler, err := r.Resolve("missing")
		if handler != nil {
			t.Error("Resolve() returned a handler for an unknown type")
		}
		if !errors.Is(err, ErrNoHandler) {
			t.Fatalf("Resolve() error = %v, want ErrNoHandler", err)
		}
		if !IsPermanent(err) {
			t.Fatalf("Resolve() error = %v should be marked permanent", err)
		}
	})

	t.Run("Lookup stays unmarked", func(t *testing.T) {
		// Lookup 只回答"能不能查到"，不替调用方决定任务命运，因此不打永久标记。
		if _, err := r.Lookup("missing"); IsPermanent(err) {
			t.Error("Lookup() should not mark unknown types as permanent")
		}
	})
}

// testPayload 是 TypedHandler 测试用的 payload 结构。
type testPayload struct {
	To   string `json:"to"`
	Body string `json:"body"`
}

func TestTypedHandlerDecodesAndInvokes(t *testing.T) {
	var got testPayload
	handler := TypedHandler(func(_ context.Context, in testPayload) error {
		got = in
		return nil
	})

	if err := handler(context.Background(), json.RawMessage(`{"to":"a@example.com","body":"hi"}`)); err != nil {
		t.Fatalf("handler returned %v, want nil", err)
	}
	if got.To != "a@example.com" || got.Body != "hi" {
		t.Errorf("handler received %+v, want To=a@example.com Body=hi", got)
	}
}

// TestTypedHandlerDecodeFailureIsPermanent 是这一步的核心承诺：
// "无法解码 payload 的任务不能无限快速重试"由库自己保证，而不是靠调用方自觉。
func TestTypedHandlerDecodeFailureIsPermanent(t *testing.T) {
	tests := []struct {
		name    string
		payload string
	}{
		{"truncated json", `{"to":`},
		{"not json at all", `to=a@example.com`},
		{"field type mismatch", `{"to":42}`},
		{"array instead of object", `[1,2,3]`},
		{"string instead of object", `"hello"`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			called := false
			handler := TypedHandler(func(context.Context, testPayload) error {
				called = true
				return nil
			})

			err := handler(context.Background(), json.RawMessage(tt.payload))
			if err == nil {
				t.Fatal("decode failure should return an error")
			}
			if called {
				t.Error("handler must not be called when the payload cannot be decoded")
			}
			if !IsPermanent(err) {
				t.Errorf("decode error %v should be permanent", err)
			}
			if !errors.Is(err, ErrInvalidPayload) {
				t.Errorf("decode error %v should wrap ErrInvalidPayload", err)
			}
		})
	}
}

// TestTypedHandlerNullPayloadDecodesToZeroValue 固化一个容易被忽略的边界：
// 入队允许 payload 是 JSON 的 null，它解码到结构体是成功的，得到零值，
// 因此 Handler 会被正常调用，而不是被判成解码失败。
func TestTypedHandlerNullPayloadDecodesToZeroValue(t *testing.T) {
	called := false
	handler := TypedHandler(func(_ context.Context, in testPayload) error {
		called = true
		if in.To != "" || in.Body != "" {
			t.Errorf("null payload decoded to %+v, want zero value", in)
		}
		return nil
	})

	if err := handler(context.Background(), json.RawMessage(`null`)); err != nil {
		t.Fatalf("handler returned %v, want nil", err)
	}
	if !called {
		t.Fatal("handler was not called for a null payload")
	}
}

func TestTypedHandlerPreservesHandlerError(t *testing.T) {
	t.Run("retryable error stays retryable", func(t *testing.T) {
		handler := TypedHandler(func(context.Context, testPayload) error {
			return errors.New("upstream timeout")
		})
		err := handler(context.Background(), json.RawMessage(`{}`))
		if err == nil || IsPermanent(err) {
			t.Fatalf("error = %v, want a non-permanent failure", err)
		}
	})

	t.Run("permanent error stays permanent and keeps its sentinel", func(t *testing.T) {
		handler := TypedHandler(func(context.Context, testPayload) error {
			return Permanent(fmt.Errorf("order missing: %w", ErrNotFound))
		})
		err := handler(context.Background(), json.RawMessage(`{}`))
		if !IsPermanent(err) {
			t.Fatalf("error = %v, want a permanent failure", err)
		}
		if !errors.Is(err, ErrNotFound) {
			t.Errorf("error = %v should still wrap ErrNotFound", err)
		}
	})
}

func TestTypedHandlerRejectsNilFunction(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("TypedHandler(nil) should panic at construction time")
		}
	}()
	_ = TypedHandler[testPayload](nil)
}

// TestTypedHandlerThroughRegistry 走一遍真实使用姿势：注册 → 解析 → 执行。
func TestTypedHandlerThroughRegistry(t *testing.T) {
	r := NewRegistry()
	if err := r.Register("email.send", TypedHandler(func(_ context.Context, in testPayload) error {
		if in.To == "" {
			return Permanent(errors.New("missing recipient"))
		}
		return nil
	})); err != nil {
		t.Fatalf("Register() unexpected error: %v", err)
	}

	handler, err := r.Resolve("email.send")
	if err != nil {
		t.Fatalf("Resolve() unexpected error: %v", err)
	}

	if err := handler(context.Background(), json.RawMessage(`{"to":"a@example.com"}`)); err != nil {
		t.Errorf("valid payload returned %v, want nil", err)
	}

	err = handler(context.Background(), json.RawMessage(`{}`))
	if !IsPermanent(err) {
		t.Errorf("error = %v, want the handler's permanent error to survive", err)
	}

	err = handler(context.Background(), json.RawMessage(`{`))
	if !IsPermanent(err) || !errors.Is(err, ErrInvalidPayload) {
		t.Errorf("error = %v, want a permanent ErrInvalidPayload", err)
	}
}
