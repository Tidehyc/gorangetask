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

	// DefaultReapInterval 是过期租约回收器的运行间隔。默认与租约时长同量级：
	// 一个进程崩溃后，它的任务最坏要等"租约到期 + 一个回收周期"才会被重新领取。
	DefaultReapInterval = 30 * time.Second

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

	// ReapInterval 是过期租约回收器的运行间隔，不得为 0。
	//
	// 回收器负责把"持有者已经没了、租约也已过期"的 running 任务重新交还队列
	// （次数未耗尽）或判为死信（次数已耗尽）。它是进程崩溃、停机超时、失租之后
	// 任务仍能继续的唯一保障，所以默认开启。
	//
	// **负值表示关闭回收器**（该 Worker 不承担回收职责）。0 会被 WithDefaults
	// 填成 DefaultReapInterval，因此"不设置"与"关闭"是两个不同的表达。
	ReapInterval time.Duration

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

	// CleanupInterval 是终态任务清理器的运行间隔，不得为 0。
	//
	// 清理器按保留期删除 succeeded / canceled / dead 三类终态任务（技术设计
	// §5.7）。这些行只是执行记录，留着只会让表无限增长、拖慢索引与领取扫描。
	// 一"轮"最多跑固定批数，每批各用一个独立事务，避免长事务。
	//
	// **负值表示关闭清理器**：该 Worker 不承担清理职责，终态任务不再被删除，
	// 表会持续增长，需要外部运维手段（定时 SQL）接手。0 会被 WithDefaults 填成
	// DefaultCleanupInterval，因此"不设置"与"关闭"是两个不同的表达——语义与
	// ReapInterval 保持一致。
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
	// 只填 0：负值是有含义的（关闭回收器），不能被默认值覆盖掉。
	if c.ReapInterval == 0 {
		c.ReapInterval = DefaultReapInterval
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

	// 0 是"未设置"，WithDefaults 会填成默认值；走到这里还是 0 说明调用方
	// 直接对零值配置调了 Validate，或显式传了 0。负值是合法的关闭开关。
	if c.ReapInterval == 0 {
		errs = append(errs, errors.New("reap interval must not be 0 (use a negative value to disable the reaper)"))
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
	// 清理器与回收器共用同一套开关约定：0 是"未设置"，负值是关闭开关。
	if c.CleanupInterval == 0 {
		errs = append(errs, errors.New("cleanup interval must not be 0 (use a negative value to disable the cleanup)"))
	}

	if len(errs) == 0 {
		return nil
	}
	return fmt.Errorf("%w: %w", ErrInvalidConfig, errors.Join(errs...))
}

// ClientConfig 是入队侧（Client）的配置。零值 ClientConfig 是合法的：缺省字段
// 会被填充为默认值，因此调用方只需设置想覆盖的字段。
//
// 它与 Config（Worker 运行配置）刻意分开：Worker 配置描述"怎么执行任务"，
// ClientConfig 描述"任务怎么写进库"。两者都不需要对方存在——只想入队、
// 不跑 Worker 的进程不必构造 Worker 配置。
//
// 使用方式：
//
//	cfg := gorangetask.ClientConfig{MaxPayloadSize: 32 * 1024}
//	cfg = cfg.WithDefaults()
//	if err := cfg.Validate(); err != nil { ... }
type ClientConfig struct {
	// DefaultQueue 是任务未指定 Queue 时使用的队列名。不能为空。
	//
	// 该字段的实际长度上限由各 store 方言决定（MySQL 为 64 字符），
	// 由方言实现在构造时校验，根包不绑定具体数据库的列宽。
	DefaultQueue string

	// DefaultMaxAttempts 是任务未指定 MaxAttempts 时使用的最大尝试次数。
	// 必须大于 0。
	DefaultMaxAttempts int

	// MaxPayloadSize 是单个任务 payload 的字节上限。必须大于 0。
	MaxPayloadSize int
}

// WithDefaults 返回一个副本，其中所有零值字段被替换为默认值。
// 它不修改接收者，也不做合法性校验；校验请调用 Validate。
func (c ClientConfig) WithDefaults() ClientConfig {
	if c.DefaultQueue == "" {
		c.DefaultQueue = DefaultQueue
	}
	if c.DefaultMaxAttempts == 0 {
		c.DefaultMaxAttempts = DefaultMaxAttempts
	}
	if c.MaxPayloadSize == 0 {
		c.MaxPayloadSize = DefaultMaxPayloadSize
	}
	return c
}

// Validate 校验入队配置的合法性。返回的错误可用 errors.Is 匹配 ErrInvalidConfig；
// 存在多个问题时返回一个合并错误，逐条说明原因。
func (c ClientConfig) Validate() error {
	var errs []error

	if c.DefaultQueue == "" {
		errs = append(errs, errors.New("client default queue is required"))
	}
	if c.DefaultMaxAttempts <= 0 {
		errs = append(errs, fmt.Errorf("client default max attempts must be > 0, got %d", c.DefaultMaxAttempts))
	}
	if c.MaxPayloadSize <= 0 {
		errs = append(errs, fmt.Errorf("client max payload size must be > 0, got %d", c.MaxPayloadSize))
	}

	if len(errs) == 0 {
		return nil
	}
	return fmt.Errorf("%w: %w", ErrInvalidConfig, errors.Join(errs...))
}
