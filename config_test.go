package gorangetask

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestConfigZeroValueIsValidAfterDefaults(t *testing.T) {
	cfg := Config{}.WithDefaults()

	if err := cfg.Validate(); err != nil {
		t.Fatalf("zero-value config after WithDefaults should be valid, got: %v", err)
	}

	if len(cfg.Queues) != 1 || cfg.Queues[0] != DefaultQueue {
		t.Errorf("Queues = %v, want [%q]", cfg.Queues, DefaultQueue)
	}
	if cfg.Concurrency != DefaultConcurrency {
		t.Errorf("Concurrency = %d, want %d", cfg.Concurrency, DefaultConcurrency)
	}
	if cfg.PollInterval != DefaultPollInterval {
		t.Errorf("PollInterval = %s, want %s", cfg.PollInterval, DefaultPollInterval)
	}
	if cfg.MaxPollInterval != DefaultMaxPollInterval {
		t.Errorf("MaxPollInterval = %s, want %s", cfg.MaxPollInterval, DefaultMaxPollInterval)
	}
	if cfg.BatchSize != DefaultBatchSize {
		t.Errorf("BatchSize = %d, want %d", cfg.BatchSize, DefaultBatchSize)
	}
	if cfg.LeaseDuration != DefaultLeaseDuration {
		t.Errorf("LeaseDuration = %s, want %s", cfg.LeaseDuration, DefaultLeaseDuration)
	}
	if cfg.HeartbeatInterval != DefaultHeartbeatInterval {
		t.Errorf("HeartbeatInterval = %s, want %s", cfg.HeartbeatInterval, DefaultHeartbeatInterval)
	}
	if cfg.ShutdownTimeout != DefaultShutdownTimeout {
		t.Errorf("ShutdownTimeout = %s, want %s", cfg.ShutdownTimeout, DefaultShutdownTimeout)
	}
	if cfg.HandlerTimeout != DefaultHandlerTimeout {
		t.Errorf("HandlerTimeout = %s, want %s", cfg.HandlerTimeout, DefaultHandlerTimeout)
	}
	if cfg.MaxPayloadSize != DefaultMaxPayloadSize {
		t.Errorf("MaxPayloadSize = %d, want %d", cfg.MaxPayloadSize, DefaultMaxPayloadSize)
	}
	if cfg.RetryBaseDelay != DefaultRetryBaseDelay {
		t.Errorf("RetryBaseDelay = %s, want %s", cfg.RetryBaseDelay, DefaultRetryBaseDelay)
	}
	if cfg.RetryMaxDelay != DefaultRetryMaxDelay {
		t.Errorf("RetryMaxDelay = %s, want %s", cfg.RetryMaxDelay, DefaultRetryMaxDelay)
	}
	if cfg.SucceededRetention != DefaultSucceededRetention {
		t.Errorf("SucceededRetention = %s, want %s", cfg.SucceededRetention, DefaultSucceededRetention)
	}
	if cfg.DeadRetention != DefaultDeadRetention {
		t.Errorf("DeadRetention = %s, want %s", cfg.DeadRetention, DefaultDeadRetention)
	}
	if cfg.CleanupBatchSize != DefaultCleanupBatchSize {
		t.Errorf("CleanupBatchSize = %d, want %d", cfg.CleanupBatchSize, DefaultCleanupBatchSize)
	}
	if cfg.CleanupInterval != DefaultCleanupInterval {
		t.Errorf("CleanupInterval = %s, want %s", cfg.CleanupInterval, DefaultCleanupInterval)
	}
}

func TestConfigDefaultsDoNotOverrideExplicitValues(t *testing.T) {
	cfg := Config{
		Queues:             []string{"orders", "reports"},
		Concurrency:        16,
		PollInterval:       100 * time.Millisecond,
		MaxPollInterval:    time.Second,
		BatchSize:          5,
		LeaseDuration:      time.Minute,
		HeartbeatInterval:  20 * time.Second,
		ShutdownTimeout:    15 * time.Second,
		HandlerTimeout:     2 * time.Minute,
		MaxPayloadSize:     1024,
		RetryBaseDelay:     500 * time.Millisecond,
		RetryMaxDelay:      30 * time.Minute,
		SucceededRetention: time.Hour,
		DeadRetention:      48 * time.Hour,
		CleanupBatchSize:   50,
		CleanupInterval:    10 * time.Minute,
	}
	got := cfg.WithDefaults()

	if len(got.Queues) != 2 || got.Queues[0] != "orders" || got.Queues[1] != "reports" {
		t.Errorf("Queues = %v, want [orders reports]", got.Queues)
	}
	if got.Concurrency != 16 || got.PollInterval != 100*time.Millisecond {
		t.Error("explicit scalar values should not be overwritten")
	}
	if got.BatchSize != 5 || got.MaxPayloadSize != 1024 || got.CleanupBatchSize != 50 {
		t.Error("explicit int values should not be overwritten")
	}
	if got.HandlerTimeout != 2*time.Minute || got.LeaseDuration != time.Minute {
		t.Error("explicit duration values should not be overwritten")
	}
}

func TestConfigWithDefaultsDoesNotMutateReceiver(t *testing.T) {
	original := Config{}
	_ = original.WithDefaults()

	if original.Concurrency != 0 || len(original.Queues) != 0 {
		t.Errorf("WithDefaults mutated the receiver: %+v", original)
	}
}

func TestConfigValidateRejectsInvalid(t *testing.T) {
	base := Config{}.WithDefaults()

	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{"no queues", func(c *Config) { c.Queues = nil }},
		{"empty queue name", func(c *Config) { c.Queues = []string{""} }},
		{"negative concurrency", func(c *Config) { c.Concurrency = -1 }},
		{"zero poll interval", func(c *Config) { c.PollInterval = 0 }},
		{"max poll below poll", func(c *Config) {
			c.PollInterval = time.Second
			c.MaxPollInterval = 100 * time.Millisecond
		}},
		{"batch size zero", func(c *Config) { c.BatchSize = 0 }},
		{"batch size over limit", func(c *Config) { c.BatchSize = MaxBatchSize + 1 }},
		{"negative lease", func(c *Config) { c.LeaseDuration = -time.Second }},
		{"heartbeat equals lease", func(c *Config) {
			c.LeaseDuration = 10 * time.Second
			c.HeartbeatInterval = 10 * time.Second
		}},
		{"heartbeat exceeds lease", func(c *Config) {
			c.LeaseDuration = 5 * time.Second
			c.HeartbeatInterval = 10 * time.Second
		}},
		{"zero shutdown timeout", func(c *Config) { c.ShutdownTimeout = 0 }},
		{"zero handler timeout", func(c *Config) { c.HandlerTimeout = 0 }},
		{"zero max payload", func(c *Config) { c.MaxPayloadSize = 0 }},
		{"zero retry base", func(c *Config) { c.RetryBaseDelay = 0 }},
		{"retry max below base", func(c *Config) {
			c.RetryBaseDelay = time.Minute
			c.RetryMaxDelay = time.Second
		}},
		{"zero succeeded retention", func(c *Config) { c.SucceededRetention = 0 }},
		{"zero dead retention", func(c *Config) { c.DeadRetention = 0 }},
		{"zero cleanup batch", func(c *Config) { c.CleanupBatchSize = 0 }},
		{"zero cleanup interval", func(c *Config) { c.CleanupInterval = 0 }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := base
			tt.mutate(&cfg)
			err := cfg.Validate()
			if err == nil {
				t.Fatal("Validate() = nil, want error")
			}
			if !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("Validate() error = %v, want to wrap ErrInvalidConfig", err)
			}
		})
	}
}

func TestConfigValidateReportsAllProblems(t *testing.T) {
	cfg := Config{
		Queues:      nil,
		BatchSize:   -1,
		Concurrency: -5,
	}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("Validate() = nil, want error")
	}

	msg := err.Error()
	for _, want := range []string{"queue", "batch size", "concurrency"} {
		if !strings.Contains(msg, want) {
			t.Errorf("Validate() error %q should mention %q", msg, want)
		}
	}
}

func TestConfigValidateAcceptsBoundaryBatchSize(t *testing.T) {
	for _, size := range []int{1, MaxBatchSize} {
		cfg := Config{}.WithDefaults()
		cfg.BatchSize = size
		if err := cfg.Validate(); err != nil {
			t.Errorf("batch size %d should be valid, got: %v", size, err)
		}
	}
}
