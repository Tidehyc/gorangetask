package gorangetask

import (
	"errors"
	"fmt"
	"time"
)

// 默认配置值。这些是起步值，压测后再校准；所有值均可被调用方覆盖。
const (
	// DefaultQueue 是未显式指定队列时使用的队列名。
	DefaultQueue = "default"

	// DefaultMaxAttempts 是任务未指定 MaxAttempts 时使用的最大尝试次数。
	DefaultMaxAttempts = 10

	// DefaultConcurrency 是 Worker 同时执行 Handler 的最大数量。
	DefaultConcurrency = 4

	// DefaultPollInterval 是队列为空时的轮询间隔基准值。
	DefaultPollInterval = 250 * time.Millisecond

	// DefaultMaxPollInterval 是空队列指数退避的上限。
	DefaultMaxPollInterval = 2 * time.Second

	// DefaultBatchSize 是每次 claim 周期最多领取的任务数。
	// 默认 1：避免任务在本地排队时租约已被消耗。
	DefaultBatchSize = 1

	// MaxBatchSize 是 batch size 允许配置的上限。
	MaxBatchSize = 100

	// DefaultLeaseDuration 是任务的租约时长。
	DefaultLeaseDuration = 30 * time.Second

	// DefaultHeartbeatInterval 是租约续期周期。默认取租约时长的 1/3。
	DefaultHeartbeatInterval = 10 * time.Second

	// DefaultShutdownTimeout 是优雅停机时等待运行中 Handler 退出的上限。
	DefaultShutdownTimeout = 30 * time.Second

	// DefaultHandlerTimeout 是任务未指定 Timeout 时 Handler 的执行超时。
	DefaultHandlerTimeout = 5 * time.Minute

	// DefaultMaxPayloadSize 是单个任务 payload 的默认上限：256 KiB。
	DefaultMaxPayloadSize = 256 * 1024

	// DefaultRetryBaseDelay 是指数退避的基准延迟。
	DefaultRetryBaseDelay = time.Second

	// DefaultRetryMaxDelay 是指数退避的单次延迟上限。
	DefaultRetryMaxDelay = time.Hour

	// DefaultSucceededRetention 是成功任务（含 canceled）的保留期。
	DefaultSucceededRetention = 7 * 24 * time.Hour

	// DefaultDeadRetention 是死信任务的保留期。
	DefaultDeadRetention = 30 * 24 * time.Hour

	// DefaultCleanupBatchSize 是每轮清理最多删除的行数。
	DefaultCleanupBatchSize = 1000

	// DefaultCleanupInterval 是清理任务的运行间隔。
	DefaultCleanupInterval = time.Hour
)

// Config 是 Worker 的运行配置。零值 Config 是合法的：缺省字段会被填充为
// 上表中的默认值，因此调用方只需设置想覆盖的字段。
//
// 使用方式：
//
//	cfg := gorangetask.Config{Queues: []string{"default"}, Concurrency: 8}
//	cfg = cfg.WithDefaults()   // 填充其余默认值
//	if err := cfg.Validate(); err != nil { ... }
type Config struct {
	// Queues 是 Worker 监听的队列名列表。至少需要一个非空队列名。
	Queues []string

	// Concurrency 是同时执行 Handler 的最大数量。必须大于 0。
	Concurrency int

	// PollInterval 是空队列时的轮询间隔基准值。必须大于 0。
	PollInterval time.Duration

	// MaxPollInterval 是空队列指数退避的上限，须不小于 PollInterval。
	MaxPollInterval time.Duration

	// BatchSize 是每次 claim 周期最多领取的任务数，取值 1..MaxBatchSize。
	BatchSize int

	// LeaseDuration 是任务租约时长。必须大于 0。
	LeaseDuration time.Duration

	// HeartbeatInterval 是租约续期周期，必须大于 0 且小于 LeaseDuration。
	HeartbeatInterval time.Duration

	// ShutdownTimeout 是优雅停机等待运行中 Handler 退出的上限，必须大于 0。
	ShutdownTimeout time.Duration

	// HandlerTimeout 是任务未指定 Timeout 时的默认 Handler 超时，必须大于 0。
	HandlerTimeout time.Duration

	// MaxPayloadSize 是单个任务 payload 的字节上限，必须大于 0。
	MaxPayloadSize int

	// RetryBaseDelay 是指数退避的基准延迟，必须大于 0。
	RetryBaseDelay time.Duration

	// RetryMaxDelay 是退避单次延迟上限，必须不小于 RetryBaseDelay。
	RetryMaxDelay time.Duration

	// SucceededRetention 是成功与取消任务的保留期，必须大于 0。
	SucceededRetention time.Duration

	// DeadRetention 是死信任务的保留期，必须大于 0。
	DeadRetention time.Duration

	// CleanupBatchSize 是每轮清理最多删除的行数，必须大于 0。
	CleanupBatchSize int

	// CleanupInterval 是清理任务的运行间隔，必须大于 0。
	CleanupInterval time.Duration
}

// WithDefaults 返回一个副本，其中所有零值字段被替换为默认值。
// 它不修改接收者，也不做合法性校验；校验请调用 Validate。
func (c Config) WithDefaults() Config {
	if len(c.Queues) == 0 {
		c.Queues = []string{DefaultQueue}
	}
	if c.Concurrency == 0 {
		c.Concurrency = DefaultConcurrency
	}
	if c.PollInterval == 0 {
		c.PollInterval = DefaultPollInterval
	}
	if c.MaxPollInterval == 0 {
		c.MaxPollInterval = DefaultMaxPollInterval
	}
	if c.BatchSize == 0 {
		c.BatchSize = DefaultBatchSize
	}
	if c.LeaseDuration == 0 {
		c.LeaseDuration = DefaultLeaseDuration
	}
	if c.HeartbeatInterval == 0 {
		c.HeartbeatInterval = DefaultHeartbeatInterval
	}
	if c.ShutdownTimeout == 0 {
		c.ShutdownTimeout = DefaultShutdownTimeout
	}
	if c.HandlerTimeout == 0 {
		c.HandlerTimeout = DefaultHandlerTimeout
	}
	if c.MaxPayloadSize == 0 {
		c.MaxPayloadSize = DefaultMaxPayloadSize
	}
	if c.RetryBaseDelay == 0 {
		c.RetryBaseDelay = DefaultRetryBaseDelay
	}
	if c.RetryMaxDelay == 0 {
		c.RetryMaxDelay = DefaultRetryMaxDelay
	}
	if c.SucceededRetention == 0 {
		c.SucceededRetention = DefaultSucceededRetention
	}
	if c.DeadRetention == 0 {
		c.DeadRetention = DefaultDeadRetention
	}
	if c.CleanupBatchSize == 0 {
		c.CleanupBatchSize = DefaultCleanupBatchSize
	}
	if c.CleanupInterval == 0 {
		c.CleanupInterval = DefaultCleanupInterval
	}
	return c
}

// Validate 校验配置的合法性。返回的错误可用 errors.Is 匹配 ErrInvalidConfig；
// 存在多个问题时返回一个合并错误，逐条说明原因。
// 调用方通常在 WithDefaults 之后调用 Validate，使缺省字段已就位。
func (c Config) Validate() error {
	var errs []error

	if len(c.Queues) == 0 {
		errs = append(errs, errors.New("at least one queue is required"))
	}
	for i, q := range c.Queues {
		if q == "" {
			errs = append(errs, fmt.Errorf("queue[%d] is empty", i))
		}
	}

	if c.Concurrency <= 0 {
		errs = append(errs, fmt.Errorf("concurrency must be > 0, got %d", c.Concurrency))
	}
	if c.PollInterval <= 0 {
		errs = append(errs, fmt.Errorf("poll interval must be > 0, got %s", c.PollInterval))
	}
	if c.MaxPollInterval <= 0 {
		errs = append(errs, fmt.Errorf("max poll interval must be > 0, got %s", c.MaxPollInterval))
	}
	if c.MaxPollInterval > 0 && c.PollInterval > 0 && c.MaxPollInterval < c.PollInterval {
		errs = append(errs, fmt.Errorf("max poll interval (%s) must be >= poll interval (%s)", c.MaxPollInterval, c.PollInterval))
	}

	if c.BatchSize < 1 || c.BatchSize > MaxBatchSize {
		errs = append(errs, fmt.Errorf("batch size must be in [1, %d], got %d", MaxBatchSize, c.BatchSize))
	}

	if c.LeaseDuration <= 0 {
		errs = append(errs, fmt.Errorf("lease duration must be > 0, got %s", c.LeaseDuration))
	}
	if c.HeartbeatInterval <= 0 {
		errs = append(errs, fmt.Errorf("heartbeat interval must be > 0, got %s", c.HeartbeatInterval))
	}
	// 心跳必须严格小于租约，否则续期来不及在租约到期前完成。
	if c.HeartbeatInterval > 0 && c.LeaseDuration > 0 && c.HeartbeatInterval >= c.LeaseDuration {
		errs = append(errs, fmt.Errorf("heartbeat interval (%s) must be < lease duration (%s)", c.HeartbeatInterval, c.LeaseDuration))
	}

	if c.ShutdownTimeout <= 0 {
		errs = append(errs, fmt.Errorf("shutdown timeout must be > 0, got %s", c.ShutdownTimeout))
	}
	if c.HandlerTimeout <= 0 {
		errs = append(errs, fmt.Errorf("handler timeout must be > 0, got %s", c.HandlerTimeout))
	}
	if c.MaxPayloadSize <= 0 {
		errs = append(errs, fmt.Errorf("max payload size must be > 0, got %d", c.MaxPayloadSize))
	}

	if c.RetryBaseDelay <= 0 {
		errs = append(errs, fmt.Errorf("retry base delay must be > 0, got %s", c.RetryBaseDelay))
	}
	if c.RetryMaxDelay <= 0 {
		errs = append(errs, fmt.Errorf("retry max delay must be > 0, got %s", c.RetryMaxDelay))
	}
	if c.RetryBaseDelay > 0 && c.RetryMaxDelay > 0 && c.RetryMaxDelay < c.RetryBaseDelay {
		errs = append(errs, fmt.Errorf("retry max delay (%s) must be >= retry base delay (%s)", c.RetryMaxDelay, c.RetryBaseDelay))
	}

	if c.SucceededRetention <= 0 {
		errs = append(errs, fmt.Errorf("succeeded retention must be > 0, got %s", c.SucceededRetention))
	}
	if c.DeadRetention <= 0 {
		errs = append(errs, fmt.Errorf("dead retention must be > 0, got %s", c.DeadRetention))
	}
	if c.CleanupBatchSize <= 0 {
		errs = append(errs, fmt.Errorf("cleanup batch size must be > 0, got %d", c.CleanupBatchSize))
	}
	if c.CleanupInterval <= 0 {
		errs = append(errs, fmt.Errorf("cleanup interval must be > 0, got %s", c.CleanupInterval))
	}

	if len(errs) == 0 {
		return nil
	}
	return fmt.Errorf("%w: %w", ErrInvalidConfig, errors.Join(errs...))
}
