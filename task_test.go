package gorangetask

import "testing"

func TestStatusIsTerminal(t *testing.T) {
	tests := []struct {
		status Status
		want   bool
	}{
		{StatusPending, false},
		{StatusRunning, false},
		{StatusSucceeded, true},
		{StatusDead, true},
		{StatusCanceled, true},
		{Status("unknown"), false},
	}
	for _, tt := range tests {
		if got := tt.status.IsTerminal(); got != tt.want {
			t.Errorf("Status(%q).IsTerminal() = %v, want %v", tt.status, got, tt.want)
		}
	}
}

func TestStatusIsValid(t *testing.T) {
	tests := []struct {
		status Status
		want   bool
	}{
		{StatusPending, true},
		{StatusRunning, true},
		{StatusSucceeded, true},
		{StatusDead, true},
		{StatusCanceled, true},
		{Status(""), false},
		{Status("unknown"), false},
	}
	for _, tt := range tests {
		if got := tt.status.IsValid(); got != tt.want {
			t.Errorf("Status(%q).IsValid() = %v, want %v", tt.status, got, tt.want)
		}
	}
}

func TestStatusConstants(t *testing.T) {
	// 状态值是与数据库存储的字符串契约，改动需同步 migration。
	pairs := map[Status]string{
		StatusPending:   "pending",
		StatusRunning:   "running",
		StatusSucceeded: "succeeded",
		StatusDead:      "dead",
		StatusCanceled:  "canceled",
	}
	for status, want := range pairs {
		if string(status) != want {
			t.Errorf("status constant = %q, want %q", status, want)
		}
	}
}
