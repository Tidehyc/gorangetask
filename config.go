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

	// Logger 接收 Worker 产生的结构化日志；为 nil 时静默。
	//
	// nil 是**合法且默认**的取值，因此 WithDefaults 不会给它填默认值，Validate
	// 也不校验它——"要不要日志"是部署决定，不是配置错误。
	Logger Logger

	// Metrics 接收 Worker 产生的指标事件；为 nil 时会在构造期被替换成
	// NopMetrics，因此库内部的调用点不必各自判空。
	Metrics Metrics
}

// defaultIfZero 是泛型辅助函数：若 val 为其类型的零值，则返回 def。
func defaultIfZero[T comparable](val, def T) T {
	var zero T
	if val == zero {
		return def
	}
	return val
}

// checkPositive 校验整数或时长是否严格大于 0，违规时向 errs 追加格式化错误。
func checkPositive[T ~int | ~int64](errs *[]error, name string, val T) {
	if val <= 0 {
		*errs = append(*errs, fmt.Errorf("%s must be > 0, got %v", name, val))
	}
}

// checkOrder 校验 maxVal 须不小于 minVal。
func checkOrder(errs *[]error, minName, maxName string, minVal, maxVal time.Duration) {
	if maxVal > 0 && minVal > 0 && maxVal < minVal {
		*errs = append(*errs, fmt.Errorf("%s (%s) must be >= %s (%s)", maxName, maxVal, minName, minVal))
	}
}

// WithDefaults 返回一个副本，其中所有零值字段被替换为默认值。
// 它不修改接收者，也不做合法性校验；校验请调用 Validate。
func (c Config) WithDefaults() Config {
	if len(c.Queues) == 0 {
		c.Queues = []string{DefaultQueue}
	}
	c.Concurrency = defaultIfZero(c.Concurrency, DefaultConcurrency)
	c.PollInterval = defaultIfZero(c.PollInterval, DefaultPollInterval)
	c.MaxPollInterval = defaultIfZero(c.MaxPollInterval, DefaultMaxPollInterval)
	c.BatchSize = defaultIfZero(c.BatchSize, DefaultBatchSize)
	c.LeaseDuration = defaultIfZero(c.LeaseDuration, DefaultLeaseDuration)
	c.HeartbeatInterval = defaultIfZero(c.HeartbeatInterval, DefaultHeartbeatInterval)
	// 只填 0：负值是有含义的（关闭回收器），不能被默认值覆盖掉。
	c.ReapInterval = defaultIfZero(c.ReapInterval, DefaultReapInterval)
	c.ShutdownTimeout = defaultIfZero(c.ShutdownTimeout, DefaultShutdownTimeout)
	c.HandlerTimeout = defaultIfZero(c.HandlerTimeout, DefaultHandlerTimeout)
	c.MaxPayloadSize = defaultIfZero(c.MaxPayloadSize, DefaultMaxPayloadSize)
	c.RetryBaseDelay = defaultIfZero(c.RetryBaseDelay, DefaultRetryBaseDelay)
	c.RetryMaxDelay = defaultIfZero(c.RetryMaxDelay, DefaultRetryMaxDelay)
	c.SucceededRetention = defaultIfZero(c.SucceededRetention, DefaultSucceededRetention)
	c.DeadRetention = defaultIfZero(c.DeadRetention, DefaultDeadRetention)
	c.CleanupBatchSize = defaultIfZero(c.CleanupBatchSize, DefaultCleanupBatchSize)
	c.CleanupInterval = defaultIfZero(c.CleanupInterval, DefaultCleanupInterval)
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

	checkPositive(&errs, "concurrency", c.Concurrency)
	checkPositive(&errs, "poll interval", c.PollInterval)
	checkPositive(&errs, "max poll interval", c.MaxPollInterval)
	checkOrder(&errs, "poll interval", "max poll interval", c.PollInterval, c.MaxPollInterval)

	if c.BatchSize < 1 || c.BatchSize > MaxBatchSize {
		errs = append(errs, fmt.Errorf("batch size must be in [1, %d], got %d", MaxBatchSize, c.BatchSize))
	}

	checkPositive(&errs, "lease duration", c.LeaseDuration)
	checkPositive(&errs, "heartbeat interval", c.HeartbeatInterval)
	// 心跳必须严格小于租约，否则续期来不及在租约到期前完成。
	if c.HeartbeatInterval > 0 && c.LeaseDuration > 0 && c.HeartbeatInterval >= c.LeaseDuration {
		errs = append(errs, fmt.Errorf("heartbeat interval (%s) must be < lease duration (%s)", c.HeartbeatInterval, c.LeaseDuration))
	}

	// 0 是"未设置"，WithDefaults 会填成默认值；走到这里还是 0 说明调用方
	// 直接对零值配置调了 Validate，或显式传了 0。负值是合法的关闭开关。
	if c.ReapInterval == 0 {
		errs = append(errs, errors.New("reap interval must not be 0 (use a negative value to disable the reaper)"))
	}

	checkPositive(&errs, "shutdown timeout", c.ShutdownTimeout)
	checkPositive(&errs, "handler timeout", c.HandlerTimeout)
	checkPositive(&errs, "max payload size", c.MaxPayloadSize)

	checkPositive(&errs, "retry base delay", c.RetryBaseDelay)
	checkPositive(&errs, "retry max delay", c.RetryMaxDelay)
	checkOrder(&errs, "retry base delay", "retry max delay", c.RetryBaseDelay, c.RetryMaxDelay)

	checkPositive(&errs, "succeeded retention", c.SucceededRetention)
	checkPositive(&errs, "dead retention", c.DeadRetention)
	checkPositive(&errs, "cleanup batch size", c.CleanupBatchSize)
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

	// Metrics 接收入队与取消产生的指标事件；为 nil 时会在构造期被替换成
	// NopMetrics，因此实现内部的调用点不必各自判空。
	//
	// 取消没有队列标签（见 Metrics.TaskCanceled 的说明）；入队带的是**归一化后**
	// 的队列名，也就是任务实际落入的那个队列。
	Metrics Metrics
}

// WithDefaults 返回一个副本，其中所有零值字段被替换为默认值。
// 它不修改接收者，也不做合法性校验；校验请调用 Validate。
func (c ClientConfig) WithDefaults() ClientConfig {
	c.DefaultQueue = defaultIfZero(c.DefaultQueue, DefaultQueue)
	c.DefaultMaxAttempts = defaultIfZero(c.DefaultMaxAttempts, DefaultMaxAttempts)
	c.MaxPayloadSize = defaultIfZero(c.MaxPayloadSize, DefaultMaxPayloadSize)
	return c
}

// Validate 校验入队配置的合法性。返回的错误可用 errors.Is 匹配 ErrInvalidConfig；
// 存在多个问题时返回一个合并错误，逐条说明原因。
func (c ClientConfig) Validate() error {
	var errs []error

	if c.DefaultQueue == "" {
		errs = append(errs, errors.New("client default queue is required"))
	}
	checkPositive(&errs, "client default max attempts", c.DefaultMaxAttempts)
	checkPositive(&errs, "client max payload size", c.MaxPayloadSize)

	if len(errs) == 0 {
		return nil
	}
	return fmt.Errorf("%w: %w", ErrInvalidConfig, errors.Join(errs...))
}
