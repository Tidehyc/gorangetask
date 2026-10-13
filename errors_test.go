package gorangetask

import (
	"errors"
	"fmt"
	"io/fs"
	"strings"
	"testing"
)

func TestPermanentNilIsNil(t *testing.T) {
	if err := Permanent(nil); err != nil {
		t.Fatalf("Permanent(nil) = %v, want nil（否则 Handler 里 `return Permanent(err)` 会把成功当失败）", err)
	}
}

func TestIsPermanent(t *testing.T) {
	plain := errors.New("boom")

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"plain error", plain, false},
		{"wrapped plain error", fmt.Errorf("job failed: %w", plain), false},
		{"sentinel alone is not permanent", ErrNoHandler, false},
		{"permanent", Permanent(plain), true},
		{"permanent sentinel", Permanent(ErrNoHandler), true},
		{"double permanent", Permanent(Permanent(plain)), true},
		{"permanent wrapped again", fmt.Errorf("job failed: %w", Permanent(plain)), true},
		{"deeply wrapped permanent", fmt.Errorf("a: %w", fmt.Errorf("b: %w", Permanent(ErrLeaseLost))), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsPermanent(tt.err); got != tt.want {
				t.Fatalf("IsPermanent(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

// TestPermanentPreservesErrorChain 固化"打标记不影响原有错误链"：
// errors.Is 仍认哨兵错误，errors.As 仍能取出原始的具体错误类型。
func TestPermanentPreservesErrorChain(t *testing.T) {
	t.Run("errors.Is reaches the sentinel", func(t *testing.T) {
		err := Permanent(fmt.Errorf("resolve handler: %w", ErrNoHandler))
		if !errors.Is(err, ErrNoHandler) {
			t.Fatalf("errors.Is(%v, ErrNoHandler) = false, want true", err)
		}
	})

	t.Run("errors.As reaches the concrete error", func(t *testing.T) {
		original := &fs.PathError{Op: "open", Path: "/var/data/x", Err: fs.ErrNotExist}
		var got *fs.PathError
		if !errors.As(Permanent(original), &got) {
			t.Fatal("errors.As did not find the wrapped *fs.PathError")
		}
		if got.Path != "/var/data/x" {
			t.Errorf("recovered PathError.Path = %q, want %q", got.Path, "/var/data/x")
		}
	})

	t.Run("errors.Is still reaches the innermost sentinel", func(t *testing.T) {
		inner := fmt.Errorf("decode: %w", ErrInvalidPayload)
		if !errors.Is(Permanent(inner), ErrInvalidPayload) {
			t.Fatal("permanent marking hid the inner sentinel")
		}
	})
}

func TestPermanentErrorMessageMentionsOriginal(t *testing.T) {
	err := Permanent(errors.New("order 42 does not exist"))
	if !strings.Contains(err.Error(), "order 42 does not exist") {
		t.Errorf("error %q should contain the original message", err)
	}
	if !strings.Contains(err.Error(), "permanent") {
		t.Errorf("error %q should be recognisable as a permanent failure", err)
	}
}
