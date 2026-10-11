package gorangetask

import (
	"context"
	"encoding/json"
	"errors"
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
