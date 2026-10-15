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
	if cfg.ReapInterval != DefaultReapInterval {
		t.Errorf("ReapInterval = %s, want %s", cfg.ReapInterval, DefaultReapInterval)
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
		ReapInterval:       45 * time.Second,
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
	// 回收间隔与清理间隔是"负值 = 关闭"这类有含义的开关，默认值不得覆盖显式值。
	if got.ReapInterval != 45*time.Second {
		t.Errorf("ReapInterval = %s, want 45s: an explicit value must not be overwritten", got.ReapInterval)
	}
}

// TestConfigReapIntervalNegativeDisables 固定"负值 = 关闭回收器"这条开关约定：
// 它既是 WithDefaults 不能覆盖的值，也是 Validate 必须放行的值。
func TestConfigReapIntervalNegativeDisables(t *testing.T) {
	cfg := Config{ReapInterval: -1}.WithDefaults()

	if cfg.ReapInterval != -1 {
		t.Errorf("ReapInterval = %s after WithDefaults, want -1: 负值是有含义的开关，不能被默认值吃掉",
			cfg.ReapInterval)
	}
	if err := cfg.Validate(); err != nil {
		t.Errorf("Validate() = %v, want nil: 负值表示关闭回收器，是合法配置", err)
	}
}

// TestConfigCleanupIntervalNegativeDisables 把清理器也钉在同一个开关约定上。
//
// 它与回收器分开断言、而不是并到一个表里：两个开关的**后果完全不同**——
// 关闭回收器只是让崩溃遗留的任务没人接手（有租约兜底），关闭清理器则会让终态
// 记录无限增长。约定相同不代表可以在测试里含糊过去，关闭清理器的部署必须在
// README 里写明代价。
func TestConfigCleanupIntervalNegativeDisables(t *testing.T) {
	cfg := Config{CleanupInterval: -1}.WithDefaults()

	if cfg.CleanupInterval != -1 {
		t.Errorf("CleanupInterval = %s after WithDefaults, want -1: 负值是有含义的开关，不能被默认值吃掉",
			cfg.CleanupInterval)
	}
	if err := cfg.Validate(); err != nil {
		t.Errorf("Validate() = %v, want nil: 负值表示关闭清理器，是合法配置", err)
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
		{"zero reap interval", func(c *Config) { c.ReapInterval = 0 }},
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

func TestClientConfigZeroValueIsValidAfterDefaults(t *testing.T) {
	cfg := ClientConfig{}.WithDefaults()

	if err := cfg.Validate(); err != nil {
		t.Fatalf("zero-value client config after WithDefaults should be valid, got: %v", err)
	}

	if cfg.DefaultQueue != DefaultQueue {
		t.Errorf("DefaultQueue = %q, want %q", cfg.DefaultQueue, DefaultQueue)
	}
	if cfg.DefaultMaxAttempts != DefaultMaxAttempts {
		t.Errorf("DefaultMaxAttempts = %d, want %d", cfg.DefaultMaxAttempts, DefaultMaxAttempts)
	}
	if cfg.MaxPayloadSize != DefaultMaxPayloadSize {
		t.Errorf("MaxPayloadSize = %d, want %d", cfg.MaxPayloadSize, DefaultMaxPayloadSize)
	}
}

func TestClientConfigDefaultsDoNotOverrideExplicitValues(t *testing.T) {
	cfg := ClientConfig{
		DefaultQueue:       "orders",
		DefaultMaxAttempts: 3,
		MaxPayloadSize:     1024,
	}.WithDefaults()

	if cfg.DefaultQueue != "orders" {
		t.Errorf("DefaultQueue = %q, want %q", cfg.DefaultQueue, "orders")
	}
	if cfg.DefaultMaxAttempts != 3 {
		t.Errorf("DefaultMaxAttempts = %d, want 3", cfg.DefaultMaxAttempts)
	}
	if cfg.MaxPayloadSize != 1024 {
		t.Errorf("MaxPayloadSize = %d, want 1024", cfg.MaxPayloadSize)
	}
}

func TestClientConfigWithDefaultsDoesNotMutateReceiver(t *testing.T) {
	original := ClientConfig{}
	_ = original.WithDefaults()

	if original.DefaultQueue != "" || original.DefaultMaxAttempts != 0 || original.MaxPayloadSize != 0 {
		t.Errorf("WithDefaults mutated the receiver: %+v", original)
	}
}

func TestClientConfigValidateRejectsInvalid(t *testing.T) {
	tests := []struct {
		name string
		cfg  ClientConfig
	}{
		{"empty default queue", ClientConfig{DefaultMaxAttempts: 1, MaxPayloadSize: 1}},
		{"zero max attempts", ClientConfig{DefaultQueue: "q", MaxPayloadSize: 1}},
		{"negative max attempts", ClientConfig{DefaultQueue: "q", DefaultMaxAttempts: -1, MaxPayloadSize: 1}},
		{"zero payload size", ClientConfig{DefaultQueue: "q", DefaultMaxAttempts: 1}},
		{"negative payload size", ClientConfig{DefaultQueue: "q", DefaultMaxAttempts: 1, MaxPayloadSize: -1}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if err == nil {
				t.Fatal("Validate() = nil, want error")
			}
			if !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("Validate() error = %v, want to wrap ErrInvalidConfig", err)
			}
		})
	}
}
