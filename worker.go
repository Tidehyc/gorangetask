package gorangetask

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"
)

// ClaimedTask 是 Worker 成功领取到的一条任务，连同本次领取的 fencing 凭据。
//
// 它是执行侧的输入：Claim 返回它，随后 Succeed / Retry / MarkDead 用它把结果
// 写回去。与 TaskInfo 的区别在于它面向 Worker 内部——带 Owner / Epoch 这两个
// 只对执行者有意义的 fencing 凭据（TaskInfo 刻意不含它们），因此不暴露给只做
// 查询的调用方。
type ClaimedTask struct {
	// ID 是任务标识。
	ID TaskID
	// Queue 是任务所属队列。
	Queue string
	// Type 是任务类型名，用于映射 Handler。
	Type string
	// Payload 是任务的 JSON 参数。
	Payload json.RawMessage
	// Attempt 是**包含本次**在内的尝试次数（领取时由存储 +1）。
	Attempt int
	// MaxAttempts 是最大尝试次数。Attempt >= MaxAttempts 表示次数已耗尽。
	MaxAttempts int
	// Timeout 是入队时指定的 Handler 超时；未设置时为 0，表示本次执行使用
	// Worker 配置的 HandlerTimeout。
	Timeout time.Duration

	// Wait 是这条任务的排队等待时长：从"最早可被领取"（created_at 与 run_at 的
	// 较晚者）到它实际被领取为止，由**数据库时钟**算出。
	//
	// 它只服务于可观测性（Metrics.TaskWaited）。刻意在存储层算好再带出来，而不是
	// 由 Worker 拿本地时钟减：那样各机器与数据库之间的时钟偏差会直接混进这个数，
	// 而它衡量的恰恰是以毫秒计的系统行为（技术设计 §3.2 的时钟口径同此）。
	Wait time.Duration

	// Owner 是本次领取的持有者标识；Epoch 是本次领取的租约代数。
	//
	// 两者构成 fencing 凭据：写回时存储必须同时匹配 owner 与 epoch，
	// 否则视为租约已易主，写回被拒绝（ErrLeaseLost）。这样即使一个已经
	// 失去租约的旧 Worker 恢复过来，也无法覆盖新持有者的结果。
	Owner string
	Epoch uint64
}

// ClaimSpec 是一次领取请求。字段全部来自 Worker 配置，由 Worker 构造。
type ClaimSpec struct {
	// Queue 是本次领取的目标队列。
	Queue string
	// Limit 是本次最多领取的任务数，必须大于 0。
	Limit int
	// Owner 是本次领取的持有者标识，不能为空。
	Owner string
	// LeaseDuration 是本次领取的租约时长，必须大于 0。
	LeaseDuration time.Duration
}

// ReapResult 是一次过期租约回收的结果计数。
//
// 两个计数都保留：回收是"进程崩溃之后任务还能不能继续"的唯一保障，而这种动作
// 平时是静默的——没有计数的话，出问题时几乎无从观察它到底有没有在工作。
type ReapResult struct {
	// ToPending 是被交还队列、等待重新领取的任务数。
	ToPending int
	// ToDead 是尝试次数已耗尽、被直接判为死信的任务数。
	ToDead int
}

// CleanupSpec 是一次终态清理的输入。
//
// 它是"每个终态各清一次"的粒度，而不是一次清掉所有终态：三种终态的保留期并不同
// （见 Config.SucceededRetention / DeadRetention），而且分状态执行才能让底层的
// 加锁读停在 (status, finished_at, id) 这个索引的**等值前缀**上——换成
// `status IN (...)`，索引无法再保证按 (finished_at, id) 有序，优化器会退化成
// 范围扫描 + filesort，加锁范围随之失控（技术设计附录 A 记的就是同类事故）。
type CleanupSpec struct {
	// Status 是要清理的终态，只允许 succeeded / dead / canceled。
	Status Status
	// Retention 是保留期，必须大于 0。
	//
	// 边界由实现用**数据库时钟**计算（只删除 finished_at 早于
	// `UTC_TIMESTAMP(6) - Retention` 的行），调用方不传绝对时刻：多机时钟不同步时，
	// 绝对时刻会让不同 Worker 删掉不同的行。这与租约到期判定的口径一致。
	Retention time.Duration
	// Limit 是本次最多删除的行数，必须大于 0。
	Limit int
}

// WorkerStore 是 Worker 执行闭环所依赖的存储能力。
//
// 根包只通过这个接口做领取与确认，不直接书写 SQL；方言实现（如
// store/mysql）负责把语义翻译成具体语句。所有方法都必须在 ctx 取消时尽快返回。
//
// 实现约定（技术设计 §5.2–§5.5）：
//   - Claim 必须在**事务提交之后**才把任务交给调用方执行；Handler 绝不能在
//     事务或行锁持有期间运行。
//   - Claim 应当只领取 status='pending' 且 run_at 已到期的任务，并原子地把它们
//     置为 running、递增 lease_epoch 与 attempt。
//   - Succeed / Retry / MarkDead / RenewLease 必须校验 (id, status='running',
//     owner, epoch, 租约未过期) 这组条件；影响 0 行时返回 ErrLeaseLost，
//     不得用无条件更新补救。
type WorkerStore interface {
	// Claim 原子领取一批到期任务。没有可领取的任务时返回空切片且 err 为 nil。
	Claim(ctx context.Context, spec ClaimSpec) ([]ClaimedTask, error)

	// RenewLease 把任务的租约延长 leaseDuration，从**数据库当前时间**起算。
	//
	// 它守卫的是"Handler 跑得比一次租约更长"这件事：只要续租一直成功，长任务
	// 就不会因为租约过期而被别人重领。因此它的 fencing 条件与写回完全相同。
	//
	// 返回 ErrLeaseLost 与返回驱动错误必须被区分：前者表示这次执行已经不再持有
	// 租约，调用方要立刻停手；后者只表示"这次没问成"，租约可能仍然有效，
	// 应当原样冒泡、稍后重试，不能被翻译成 ErrLeaseLost。
	RenewLease(ctx context.Context, task ClaimedTask, leaseDuration time.Duration) error

	// Succeed 把任务写为 succeeded 终态、清空租约并设置 finished_at。
	Succeed(ctx context.Context, task ClaimedTask) error

	// Retry 把任务写回 pending，在 delay 之后可再次被领取，并记录错误摘要。
	// delay 为 0 表示立即可重领。
	Retry(ctx context.Context, task ClaimedTask, delay time.Duration, lastError string) error

	// MarkDead 把任务写入死信终态（不再自动重试），并记录错误摘要。
	MarkDead(ctx context.Context, task ClaimedTask, lastError string) error

	// ReapExpiredLeases 回收一批"仍标记为 running、但租约已经过期"的任务：
	// 尝试次数未耗尽的交还队列（写回 pending），次数已耗尽的判为 dead。
	// limit 是本次最多处理的行数，必须大于 0。
	//
	// 调用者不是任何一条任务的租约持有者，因此它没有 owner/epoch 这类凭据可比。
	// 实现必须以"选行时读到的租约仍然未变"作为写回守卫；竞争导致影响 0 行时
	// **跳过该行继续，不算错误** —— ErrLeaseLost 是租约持有者的错误语义，
	// 这里借用它会让"有别的回收器同时在跑"看起来像故障。
	//
	// 回收只改状态，绝不执行 Handler。
	ReapExpiredLeases(ctx context.Context, limit int) (ReapResult, error)

	// CleanupFinished 删除一批"已进入终态、且超过保留期"的任务，返回真正删除的行数。
	//
	// 契约有四条，都是实现必须守住的：
	//
	//  1. **只处理 spec.Status 指定的那一个终态**。pending / running 一行都不能碰，
	//     它们是队列本身；被误删的任务既不会被执行，也不会留下任何痕迹。
	//  2. 只删除 finished_at 早于"数据库当前时间 - spec.Retention"的行。终态但
	//     finished_at 为 NULL 的行天然被排除（NULL 与任何值比较都不为真）——这是
	//     刻意的兜底方向：宁可少删，也不要把"结束时间不明"的行当成过期。
	//  3. 一批用**一个短事务**、且批内按主键删除，避免长事务与全表扫描；
	//     加锁范围必须受 spec.Limit 约束（实现口径见 store/mysql）。
	//  4. 删满 spec.Limit 行时返回的计数应当等于 Limit，调用方据此判断"可能还有
	//     剩余"，不需要额外的 COUNT。
	//
	// 注意副作用：任务一旦被删除，它的幂等键就不再占用唯一索引，同一个键可以被
	// 重新接受（技术设计 §3）。库保证的是"保留期内不重复入队"，不是"永不重复"；
	// 需要更强语义的调用方要自己在业务侧去重。
	CleanupFinished(ctx context.Context, spec CleanupSpec) (int64, error)
}

// Worker 从队列领取任务、调用 Handler、再把结果写回存储。
//
// 使用方式：
//
//	w, err := gorangetask.NewWorker(store, registry, cfg)
//	if err != nil { ... }
//	go w.Run(ctx)          // ctx 取消即优雅停机
//
// 本项（进度跟踪 §3「单 Worker claim/execute/ack」）已实现的语义：
//   - 领取：一次 claim 周期内先提交事务，再执行 Handler，顺序有保证。
//   - 并发：Run 按 Config.Concurrency 起同等数量的 goroutine，各自独立领取。
//   - 成功：写回 succeeded 终态。
//   - 失败分流（§4「重试退避与永久错误」）：永久失败（IsPermanent，如未知任务类型、
//     payload 无法解码）直接写 dead，不做无谓重试；其余失败按**有上限的指数退避 +
//     抖动**写回 pending，尝试次数耗尽后写 dead。
//   - 单任务 panic 兜底（§4「panic 处理」）：Handler panic 被收成一次普通失败，
//     只终结这一条任务。
//   - 优雅停机（§3「Worker graceful shutdown」）：见 Run 的说明。
//   - 租约续期与失租取消（§4「租约续期」「lease 丢失取消 context」）：见下方
//     「本次执行的三条取消路径」。
//   - 过期租约回收（§4「过期租约回收」）：见下方「过期租约回收器」。
//   - 终态清理（§4「终态清理」）：Run 还起一个清理协程，按保留期删除
//     succeeded / canceled / dead 三类终态任务，避免任务表随吞吐无限增长。
//
// # 本次执行的三条取消路径
//
// 传给 Handler 的 context 可能因为三种互不相同的原因被取消，Handler 可以用
// context.Cause 分辨它们：
//
//   - **停机**：Run 的 ctx 被取消，原因是 context.Canceled。Worker 会停止领取新任务、
//     不再为该任务续租，但**不释放租约**；任务保持 running 等租约自然过期。
//   - **逐任务超时**：原因是 context.DeadlineExceeded（Task.Timeout，未设置则用
//     Config.HandlerTimeout）。这算一次普通的失败，正常走重试/死信。
//   - **失去租约**：原因是 ErrLeaseLost。续租被拒说明任务已经归别人，本次执行的
//     任何结果都不再被接受，Worker 不会写回任何状态。
//
// 取消始终是**协作式**的：它不会回滚 Handler 已经造成的业务副作用，只是通知
// Handler 尽快收手。Handler 必须在长耗时操作中观察 ctx。
//
// # 过期租约回收器
//
// Run 还会起一个回收协程，按 Config.ReapInterval 把"持有者已经消失、租约也已
// 过期"的任务交还队列或判为死信。它是**进程崩溃、停机超时、Handler 不理会取消**
// 这三种情况的唯一出口：这三种情况都会留下一条永远 running 的任务，而租约是其中
// 唯一会自然过期的凭据。
//
// 它有两条刻意的性质：
//   - **不写回任何业务结果**：回收只把状态从 running 挪走，不执行 Handler，也
//     不替任何任务确认成功或失败。
//   - **失败不致命**：续租看护与回收器都遵循同一条规则——只有 ctx 取消才退出，
//     驱动错误一律上报后等下一个周期重试。数据库抖一下不该让回收能力停摆。
//
// 集群里每个 Worker 都会回收，并发安全由存储层的 SKIP LOCKED 与租约值守卫保证
// （见 WorkerStore.ReapExpiredLeases）。
//
// # 可观测性
//
// 配置了 Config.Logger 时，Worker 会把生命周期事件与基础设施错误记成结构化日志：
// 领取、成功、重试、死信、失去租约、回收与清理的每轮结果、以及停机过程。字段名
// 是稳定的 snake_case（task_id / task_type / queue / attempt / max_attempts /
// lease_epoch / worker_id），与需求 §8 对齐；**payload 永不进入日志**。
//
// 未配置 Logger 时全程静默——"要不要日志"是部署决定。内部字段 onError 只是本包
// 测试用的观察缝隙（记为 error 级日志的同时也会调用它），不是给调用方的出口。
type Worker struct {
	store    WorkerStore
	registry *Registry
	cfg      Config
	owner    string

	// onError 接收与单个任务无关的基础设施错误（领取失败、写回失败等）。
	//
	// 它是**测试用的内部观察缝隙**，不是公开出口：生产环境请用 Config.Logger。
	// report 在调用它的同时也记一条 error 级日志。
	onError func(error)

	// rr 用于在多个监听队列之间轮转，避免总是先打同一个队列。
	rr atomic.Uint64

	// active 是当前正在执行 Handler 的数量，供 Metrics.ConcurrencyInUse 报告。
	// 用原子量而不是普通计数：它被每个执行 goroutine 读写，且只在 process
	// 的首尾各动一次，不值得为它引入一把锁。
	active atomic.Int64
}

// NewWorker 构造一个 Worker。store 与 registry 不能为 nil，registry 至少要注册
// 一个 Handler，cfg 会先经 WithDefaults 填充再校验。
func NewWorker(store WorkerStore, registry *Registry, cfg Config) (*Worker, error) {
	if store == nil {
		return nil, fmt.Errorf("%w: nil worker store", ErrInvalidConfig)
	}
	if registry == nil {
		return nil, fmt.Errorf("%w: nil handler registry", ErrInvalidConfig)
	}
	// 一个 Handler 都没注册的 Worker 只会把所有任务判成"类型未知"，
	// 属于启动期就该发现的配置错误，不留给运行期。
	if registry.Len() == 0 {
		return nil, fmt.Errorf("%w: no handlers registered", ErrInvalidConfig)
	}
	cfg = cfg.WithDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	// nil 指标在这里换成空实现：库内部遍布打点，让每个调用点各自判空既啰嗦
	// 又容易漏掉一处。Logger 不做同样的替换，因为日志的单点出口（logf）已经
	// 只有一处判空。
	if cfg.Metrics == nil {
		cfg.Metrics = NopMetrics{}
	}
	return &Worker{
		store:    store,
		registry: registry,
		cfg:      cfg,
		owner:    newOwnerID(),
	}, nil
}

// Run 启动 Worker 并阻塞，直到 ctx 被取消。
//
// 它会启动 Config.Concurrency 个 goroutine，每个都反复执行
// "领取一批 → 逐条执行 → 写回结果"。队列为空（或领取失败）时按
// PollInterval 起、指数退避至 MaxPollInterval 并加抖动，避免空队列时的
// 高频轮询；一旦领到任务就把退避重置回 PollInterval。
//
// # 优雅停机
//
// ctx 取消即触发停机，按三步走：
//
//  1. **停止领取新任务**：各循环在下一轮开头发现 ctx 已取消就退出，不再 Claim。
//  2. **通知在途 Handler**：Handler 拿到的 context 派生自 ctx，取消会传到它手里。
//     这是协作式取消——Handler 必须自己观察 ctx 才会提前返回，库无法强杀它。
//  3. **在 ShutdownTimeout 内等待退出**：超时后 Run 不再等待，返回
//     ErrShutdownTimeout；没退回的 Handler 连同它们的任务一起被放弃。
//
// 回收协程也在同一步被等停。它不持有任何任务、随时可以放手，所以它几乎总是立刻
// 返回；把它一起纳管只是为了不留一条"Run 已经返回、却还有协程在改库"的尾巴。
//
// 停机期间**不写回失败**。Handler 因停机被取消而返回的错误不代表任务真的失败；
// 把它写回 pending 会让另一个 Worker 立刻重跑，而旧 Handler 的业务副作用可能
// 仍在途。因此这类任务保持 running，等租约自然过期后由回收器恢复
// （需求文档 §4.6「不提前把租约释放给其他 Worker」）。
//
// 已经**成功返回**的 Handler 仍然会尽力确认：它已明确结束，确认它不会造成并行
// 执行，还能省掉一次重复。确认时改用一个脱离停机取消、只保留 ShutdownTimeout
// 作为预算的 context，否则这条本来成功的结果会连带被取消写不回去。
//
// 单个任务执行失败不会中断循环：失败会被写回存储（pending 重试或 dead）。
// 正常停机（所有 Handler 都在 ShutdownTimeout 内退出）返回 nil。
func (w *Worker) Run(ctx context.Context) error {
	w.logf(ctx, LevelInfo, "worker started",
		"worker_id", w.owner,
		"queues", w.cfg.Queues,
		"concurrency", w.cfg.Concurrency,
		"reaper_enabled", w.reaperEnabled(),
		"cleanup_enabled", w.cleanupEnabled(),
	)

	var wg sync.WaitGroup
	for i := 0; i < w.cfg.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.loop(ctx)
		}()
	}

	// 所有循环都只在 ctx 取消后才退出（每个 loop 都在轮次开头检查 ctx），
	// 因此这里直接等停机信号。wg.Wait() 放在独立 goroutine 里，是为了让
	// 下面的有界等待能同时观察"循环已经全部结束"。
	loopsDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(loopsDone)
	}()

	// 回收器与清理器的存亡只由 ctx 与配置决定，与在途 Handler 无关，所以各自一个
	// 信号。关闭时直接关掉它，下面的等待逻辑不必再分情况。
	reapDone := make(chan struct{})
	if w.reaperEnabled() {
		go func() {
			defer close(reapDone)
			w.reapLoop(ctx)
		}()
	} else {
		close(reapDone)
	}

	cleanupDone := make(chan struct{})
	if w.cleanupEnabled() {
		go func() {
			defer close(cleanupDone)
			w.cleanupLoop(ctx)
		}()
	} else {
		close(cleanupDone)
	}

	<-ctx.Done()
	// 停机日志用脱离取消的 context：这行之后 ctx 已经是取消态，而"开始停机"这条
	// 消息恰恰是最后一个必须写出去的事件。日志实现若检查 ctx 也不会把它丢掉。
	w.logf(context.WithoutCancel(ctx), LevelInfo, "worker stopping",
		"worker_id", w.owner, "timeout", w.cfg.ShutdownTimeout)

	// 在途 Handler 拿到的 context 派生自 ctx，取消已经传到它们手里；这里只等
	// 它们在 ShutdownTimeout 内返回，不为不理会取消的 Handler 无限期阻塞。
	timer := time.NewTimer(w.cfg.ShutdownTimeout)
	defer timer.Stop()

	// 三部分分开观察，只为把超时原因说清楚：是 Handler 没退，还是回收器/清理器
	// 没停。置 nil 后该 case 永久阻塞，等同于"这一边已经不用再等了"。
	handlersStopped, reaperStopped, cleanupStopped := false, false, false
	for !handlersStopped || !reaperStopped || !cleanupStopped {
		select {
		case <-loopsDone:
			handlersStopped, loopsDone = true, nil
		case <-reapDone:
			reaperStopped, reapDone = true, nil
		case <-cleanupDone:
			cleanupStopped, cleanupDone = true, nil
		case <-timer.C:
			// 这句与下面返回的错误用同一个 reason，保证日志与调用方拿到的
			// 错误说的是同一件事（两侧各算一次容易改一处忘一处）。
			reason := stillRunning(handlersStopped, reaperStopped, cleanupStopped)
			w.logf(context.WithoutCancel(ctx), LevelError, "shutdown timed out",
				"worker_id", w.owner, "timeout", w.cfg.ShutdownTimeout, "still_running", reason)
			return fmt.Errorf("%w: %s elapsed with %s", ErrShutdownTimeout, w.cfg.ShutdownTimeout, reason)
		}
	}
	return nil
}

// stillRunning 描述停机超时时还有哪几部分没停下来。
//
// 三部分（执行循环 / 回收器 / 清理器）彼此独立，那句超时错误里唯一能定位问题的信息
// 就是这段文字，所以逐项列出，而不是笼统一句"超时了"。
func stillRunning(handlersStopped, reaperStopped, cleanupStopped bool) string {
	var pending []string
	if !handlersStopped {
		pending = append(pending, "handlers still running")
	}
	if !reaperStopped {
		pending = append(pending, "the reaper not stopped")
	}
	if !cleanupStopped {
		pending = append(pending, "the cleanup not stopped")
	}

	// 连成一句英文清单："a" / "a and b" / "a, b and c"。进入等待循环时至少有一边
	// 没停，所以 pending 不会是空的；空输入的返回（""）只是让这个函数保持全函数，
	// 不至于在某次改动后把超时消息变成一次切片越界。
	if len(pending) < 2 {
		return strings.Join(pending, "")
	}
	return strings.Join(pending[:len(pending)-1], ", ") + " and " + pending[len(pending)-1]
}

// loop 是一个 goroutine 的领取循环。
func (w *Worker) loop(ctx context.Context) {
	backoff := w.cfg.PollInterval
	for {
		if ctx.Err() != nil {
			return
		}

		claimed, err := w.runOnce(ctx)

		// 被取消时立刻退出，不再为已领取的任务做无谓的写回尝试
		// （那些任务会留给回收器，见 Worker 文档）。
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			w.report(ctx, err)
			backoff = w.sleep(ctx, backoff)
			continue
		}
		if claimed == 0 {
			backoff = w.sleep(ctx, backoff)
			continue
		}
		// 领到了任务说明有活干，退避重置。
		backoff = w.cfg.PollInterval
	}
}

// runOnce 从下一个队列领取一批任务并逐条处理，返回本次领取到的条数。
//
// 返回的 error 只表示"领取"本身失败（基础设施问题）；单条任务的执行失败
// 会被写回存储，不作为 error 返回。
func (w *Worker) runOnce(ctx context.Context) (int, error) {
	queue := w.pickQueue()
	claimStart := time.Now()
	claimed, err := w.store.Claim(ctx, ClaimSpec{
		Queue:         queue,
		Limit:         w.cfg.BatchSize,
		Owner:         w.owner,
		LeaseDuration: w.cfg.LeaseDuration,
	})
	// 失败的领取也要记耗时——恰恰是它变慢（锁竞争、连接池耗尽）时最需要看见。
	w.cfg.Metrics.ClaimLatency(time.Since(claimStart))
	if err != nil {
		return 0, fmt.Errorf("claim tasks: %w", err)
	}
	if len(claimed) > 0 {
		w.logf(ctx, LevelDebug, "claimed tasks",
			"worker_id", w.owner, logKeyQueue, queue, "count", len(claimed))
		for _, task := range claimed {
			w.cfg.Metrics.TaskClaimed(task.Queue, task.Type)
			w.cfg.Metrics.TaskWaited(task.Queue, task.Type, task.Wait)
		}
	}

	// 领取事务已经提交，此时任务在库里已是 running 且带着本次的 owner/epoch；
	// 从这里开始执行 Handler 不会持有任何数据库事务或行锁。
	for i, task := range claimed {
		// 停机已经开始就不再启动批次里剩下的 Handler：它们一启动就会拿到已
		// 取消的 context，等于把任务领出来又立刻放弃。保持 running 交给回收器，
		// 比"启动了再取消"更干净。
		if ctx.Err() != nil {
			w.report(ctx, fmt.Errorf("shutdown in progress; %d claimed task(s) left running for the reaper", len(claimed)-i))
			break
		}
		w.process(ctx, task)
	}
	return len(claimed), nil
}

// process 执行一条已领取的任务并写回结果。它不返回 error：任务的失败是
// 预期内的结果，写回存储即可；写回本身失败才通过 report 上报。
func (w *Worker) process(ctx context.Context, task ClaimedTask) {
	// 计时只覆盖 Handler 本身：start 在这里取，elapsed 在 execute 返回后立刻算，
	// 看护协程的启停与写回都不计入"处理耗时"。
	start := time.Now()

	// 并发池水位：进入就 +1、退出就 -1。用 defer 收尾是为了让所有提前返回的
	// 分支（失租、停机）都不会漏掉这一次减法——漏了水位就只会单调上涨。
	w.cfg.Metrics.ConcurrencyInUse(int(w.active.Add(1)))
	defer func() { w.cfg.Metrics.ConcurrencyInUse(int(w.active.Add(-1))) }()

	// 本次执行有两种彼此独立的提前终止原因：整个 Worker 停机（ctx）与本任务
	// 失去租约（下面的看护协程取消 taskCtx）。用 CancelCause 把原因分开，
	// 才能对"要不要写回"作不同判断——停机时不写回失败但成功仍确认，失租时
	// 什么都不能写。Handler 也能通过 context.Cause 分辨自己是被谁叫停的。
	taskCtx, cancelTask := context.WithCancelCause(ctx)
	defer cancelTask(nil)

	stop := make(chan struct{})
	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		w.watchLease(taskCtx, stop, task, cancelTask)
	}()

	err, timedOut := w.execute(taskCtx, task)
	// 耗时只计 Handler 本身（execute 的往返），不含看护停止与写回——
	// 指标里的"处理耗时"要回答的是"Handler 干了多久"。
	elapsed := time.Since(start)

	// panic 与超时都只是**普通的失败**（照常走重试或死信，也会计进 TaskFinished），
	// 这里额外记一次诊断计数：它们分别指向"Handler 有 bug"与"超时/容量需要调整"，
	// 混在失败总数里就看不出来了。
	var panicked *panicError
	if errors.As(err, &panicked) {
		w.cfg.Metrics.HandlerPanicked(task.Queue, task.Type)
	}
	if timedOut {
		w.cfg.Metrics.HandlerTimedOut(task.Queue, task.Type)
	}

	// 先停看护、等它退出，再写回。反过来的话，续租可能在任务已经写成终态之后
	// 才到达，因 status 不再是 running 而返回 ErrLeaseLost，凭空报出一条错误；
	// 更糟的是它会把一个已经正常结束的任务标成"失租"。
	close(stop)
	<-watchDone

	// 失租的判断必须排在停机前面：租约一旦易主，任务已归别的持有者，本次结果
	// 无论成功与否都不能再写（写也只会被 fencing 拒掉）。任务保持 running，
	// 交给回收器按新租约处理。
	if cause := context.Cause(taskCtx); errors.Is(cause, ErrLeaseLost) {
		w.cfg.Metrics.LeaseLost(task.Queue, task.Type)
		w.report(ctx, fmt.Errorf(
			"task %d lost its lease while running; leaving it to the reaper: %w", task.ID, cause))
		return
	}

	// 停机进行中：这次失败是被取消掉的，不代表任务真的失败，不能写回
	// （写回 pending 会让别的 Worker 在旧副作用可能仍在途时立刻重跑）。
	if ctx.Err() != nil {
		if err == nil {
			w.ack(ctx, task, elapsed)
			return
		}
		w.report(ctx, fmt.Errorf("shutdown in progress; task %d left running for the reaper: %w", task.ID, err))
		return
	}

	if err != nil {
		w.fail(ctx, task, err, elapsed)
		return
	}
	w.ack(ctx, task, elapsed)
}

// watchLease 为一条在途任务按 Config.HeartbeatInterval 续租，直到 stop 被关闭
// （Handler 已结束）、taskCtx 被取消（停机），或确认失租。
//
// 确认失租（RenewLease 返回 ErrLeaseLost）时调用 cancelTask 取消 Handler 的
// context：任务已经归别人了，本次执行的结果不再被接受，越早让 Handler 收手越好。
//
// **续租失败不等于失租**。只有 ErrLeaseLost 才说明租约没了；连接断开、语句超时
// 这类错误只说明这次没问成，租约可能仍然有效，因此只上报并等下一个周期重试，
// 绝不据此取消 Handler（需求文档 §9「数据库暂时不可用时记录错误、退避并继续」）。
// 如果数据库真的长时间不可用，租约会自然过期，那时下一次续租才会返回 ErrLeaseLost。
func (w *Worker) watchLease(ctx context.Context, stop <-chan struct{}, task ClaimedTask, cancelTask context.CancelCauseFunc) {
	ticker := time.NewTicker(w.cfg.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			return
		case <-ctx.Done():
			// 停机（或本任务已被取消）：停止续租，但**不释放租约**。
			// 租约保持到自然过期，之后再交给回收器——需求文档 §4.6 要求
			// 不提前把租约释放给其他 Worker。
			return
		case <-ticker.C:
		}

		// 单次续租自带一个心跳周期长度的预算：数据库卡住时看护协程不能无限期
		// 阻塞在续租上，否则 process 在 Handler 返回后的收尾也会一起被卡住。
		renewCtx, cancel := context.WithTimeout(ctx, w.cfg.HeartbeatInterval)
		err := w.store.RenewLease(renewCtx, task, w.cfg.LeaseDuration)
		cancel()

		if err == nil {
			continue
		}
		if errors.Is(err, ErrLeaseLost) {
			cancelTask(fmt.Errorf("renew lease: %w", err))
			return
		}
		// 停机打断的续租不是失租，也不值得上报：真驱动的续租语句会因为 ctx 已取消
		// 而失败，那看上去像一条基础设施错误，实际只是停机的附带结果。
		if ctx.Err() != nil {
			return
		}
		w.report(ctx, fmt.Errorf("renew lease for task %d: %w", task.ID, err))
	}
}

// reapBatchSize 是回收器每轮最多处理的行数。
//
// 刻意做成内部常量而不是配置字段：它调的是"回收事务有多长"，属于实现细节；
// 使用方真正关心的是多久捡一次（Config.ReapInterval）。真需要调整时再提上来。
const reapBatchSize = 100

// reaperEnabled 报告本 Worker 是否承担过期租约回收职责。
//
// 约定是"负值关闭，0 表示用默认值"：NewWorker 会先跑 WithDefaults，因此走到
// 这里 ReapInterval 不可能是 0（Validate 也拒绝 0），大于 0 即开启。
func (w *Worker) reaperEnabled() bool {
	return w.cfg.ReapInterval > 0
}

// reapLoop 按 Config.ReapInterval 周期回收过期租约，直到 ctx 取消。
//
// 与 watchLease 遵循同一条原则：**只有 ctx 取消才退出**。驱动错误一律上报后
// 等下一个周期重试——回收能力要是能被一次数据库抖动关掉，崩溃遗留的任务就会
// 一直挂在 running 上没人管，而它正是这个循环存在的理由。
func (w *Worker) reapLoop(ctx context.Context) {
	ticker := time.NewTicker(w.cfg.ReapInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		result, err := w.store.ReapExpiredLeases(ctx, reapBatchSize)
		if err != nil {
			// ctx 取消导致的失败是停机的附带结果，不是基础设施故障，不必上报。
			if ctx.Err() != nil {
				return
			}
			w.report(ctx, fmt.Errorf("reap expired leases: %w", err))
			continue
		}
		// 只在真有回收动作时记一次。这是"回收发生了"的事件语义，实现方直接按
		// 传入的条数递增即可，不必自己判零；日志用同一条件，免得"真的捡回了任务"
		// 那一次被零值噪音埋掉——而它正是运维需要看到"有 Worker 崩溃过"的唯一线索。
		if result.ToPending > 0 || result.ToDead > 0 {
			w.cfg.Metrics.TasksReaped(result.ToPending, result.ToDead)
			w.logf(ctx, LevelInfo, "expired leases reaped",
				logKeyWorkerID, w.owner,
				"to_pending", result.ToPending,
				"to_dead", result.ToDead,
			)
		}
	}
}

// cleanupMaxBatches 是一轮清理里、**每个终态**最多跑几批。
//
// 为什么要"一轮多批"：保留期清理要追上的是表的增长速度。若一轮只删一批，那么
// "每小时删 1000 行"这种配置在每天新增几万行时永远追不上，积压只会单调增长——
// 而积压恰恰是这个功能存在的理由。多批则让一轮有追赶能力，同时用一个明确的上限
// 保证一轮不会无限期跑下去（每批各自一个短事务，见 store 侧实现）。
//
// 10 × CleanupBatchSize（默认 1000）＝ 一轮每个终态最多 1 万行。
const cleanupMaxBatches = 10

// cleanupEnabled 报告本 Worker 是否承担终态清理职责。
//
// 与回收器同一套约定：负值关闭，0 表示用默认值（NewWorker 会先跑 WithDefaults，
// 因此走到这里 CleanupInterval 不可能是 0，Validate 也拒绝 0）。
func (w *Worker) cleanupEnabled() bool {
	return w.cfg.CleanupInterval > 0
}

// cleanupLoop 按 Config.CleanupInterval 周期删除超过保留期的终态任务，直到 ctx 取消。
//
// 与 reapLoop 遵循同一条原则：**只有 ctx 取消才退出**，驱动错误一律上报后等下一个
// 周期重试——清理能力要是能被一次数据库抖动关掉，表就会一直长下去，而没人会注意到。
//
// 每个 tick 跑一轮；一轮里的批数有上限（cleanupMaxBatches），到点就等下个周期。
func (w *Worker) cleanupLoop(ctx context.Context) {
	ticker := time.NewTicker(w.cfg.CleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		w.runCleanupRound(ctx)
	}
}

// runCleanupRound 对每个清理目标跑一轮：重复取批，直到某一批没删满（说明清干净了）
// 或达到批数上限。
//
// "没删满即停"是这里唯一判断"还有没有剩余"的手段：CleanupSpec 的契约保证删满 limit
// 说明后面很可能还有，因此不需要额外的 COUNT；反过来，没删满就继续问下去只会多一次
// 空查询。
//
// 单个目标失败**不影响其他目标**：三种终态的保留期彼此独立，让 succeeded 的一次
// 抖动顺带把 dead 的清理也停掉没有道理。
func (w *Worker) runCleanupRound(ctx context.Context) {
	for _, spec := range w.cleanupSpecs() {
		var total int64
		for i := 0; i < cleanupMaxBatches; i++ {
			deleted, err := w.store.CleanupFinished(ctx, spec)
			if err != nil {
				// ctx 取消导致的失败是停机的附带结果，不是基础设施故障，不必上报。
				if ctx.Err() != nil {
					return
				}
				w.report(ctx, fmt.Errorf("cleanup %s tasks: %w", spec.Status, err))
				break
			}
			total += deleted
			if deleted < int64(spec.Limit) {
				break
			}
		}
		// 清理是例行工作，记 debug：它每天都会发生，按 info 记会让日志满是
		// "删了 N 行"。真需要按量告警时用指标（同一条件），不必靠日志。
		if total > 0 {
			w.cfg.Metrics.TasksCleaned(spec.Status, total)
			w.logf(ctx, LevelDebug, "finished tasks cleaned",
				logKeyWorkerID, w.owner, "status", string(spec.Status), "deleted", total)
		}
	}
}

// cleanupSpecs 返回本 Worker 一轮清理要处理的目标。
//
// succeeded 与 canceled 共用 Config.SucceededRetention，dead 用 DeadRetention
// （技术设计 §5.7）。做成一次一个终态而不是一个 `status IN (...)`：等值前缀才能让
// 加锁读停在 (status, finished_at, id) 索引上、并保持 (finished_at, id) 有序
// ——换成 IN，优化器会退化成范围扫描 + filesort，加锁范围就不再受 LIMIT 约束。
// 理由与实测见 CleanupSpec 的注释与技术设计附录 A。
func (w *Worker) cleanupSpecs() []CleanupSpec {
	return []CleanupSpec{
		{Status: StatusSucceeded, Retention: w.cfg.SucceededRetention, Limit: w.cfg.CleanupBatchSize},
		{Status: StatusCanceled, Retention: w.cfg.SucceededRetention, Limit: w.cfg.CleanupBatchSize},
		{Status: StatusDead, Retention: w.cfg.DeadRetention, Limit: w.cfg.CleanupBatchSize},
	}
}

// ack 把一条执行成功的任务写回 succeeded 终态。
//
// 这次写回**总是**脱离 ctx 的取消，只保留 Config.ShutdownTimeout 作为预算。
// 理由有三条：
//
//  1. 一条已经结束的执行，确认下来不会造成并行执行，还能省掉一次重复，所以它值得
//     一个不受停机影响的预算；
//  2. **只在"进来时 ctx 已取消"时才脱离是不够的**——还有一个更窄的窗口：进来时
//     停机尚未开始、DB 往返途中才被取消。那种情况下用 ctx 会让这次写回直接失败，
//     于是一个本来成功的任务白跑一遍。这个窗口 2026-10-16 由示例的端到端用例实测
//     抓到（`ack success for task N: succeed task: context canceled`），见
//     TestWorkerAcksSuccessEvenIfShutdownStartsMidAck；
//  3. 顺带把"数据库卡住"的边界也收紧成有界等待，与续租的做法一致（见 watchLease）。
//
// 失租时不写回，由调用方（process）在进入本函数之前判掉。
func (w *Worker) ack(ctx context.Context, task ClaimedTask, elapsed time.Duration) {
	ackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), w.cfg.ShutdownTimeout)
	defer cancel()

	if err := w.store.Succeed(ackCtx, task); err != nil {
		// 写回失败通常是租约过期/易主（ErrLeaseLost）。按设计不做无条件更新补救，
		// 任务的最终命运交给回收器。
		w.noteLeaseLostOnWriteBack(task, err)
		w.report(ctx, fmt.Errorf("ack success for task %d: %w", task.ID, err))
		return
	}
	// 指标与日志都只在写回真的落地之后记：Succeed 被 fencing 拒掉时任务并没有
	// 成功，记一条 succeeded 会让观测数据与库里的状态互相矛盾。
	w.cfg.Metrics.TaskFinished(task.Queue, task.Type, OutcomeSucceeded, elapsed)
	w.logf(ctx, LevelInfo, "task succeeded",
		append(w.taskLogFields(task), "duration", elapsed)...)
}

// noteLeaseLostOnWriteBack 在写回因租约已易主被拒时补记一次失租。
//
// process 里那次失租检测的是**续租**被拒；这里是**写回**被拒。两者在同一次执行里
// 互斥——续租被拒后 process 直接返回、不再写回——所以一次失租只会记一次。
func (w *Worker) noteLeaseLostOnWriteBack(task ClaimedTask, err error) {
	if errors.Is(err, ErrLeaseLost) {
		w.cfg.Metrics.LeaseLost(task.Queue, task.Type)
	}
}

// execute 解析 Handler 并在超时 context 下运行它，返回它的错误以及
// "它是不是因为执行超时被取消的"。
//
// 超时取 task.Timeout（入队时持久化、领取时还原）；为 0 表示没设置，
// 回落到 Worker 配置的 HandlerTimeout。runCtx 派生自 ctx（即本次执行的
// taskCtx），因此停机、逐任务超时、失去租约这三种取消都能传到 Handler 手里
// ——它们是同一套协作式取消，谁先发生谁生效，Handler 可用 context.Cause
// 分辨自己是被谁叫停的。
//
// 第二个返回值**只**为可观测性而存在（Metrics.HandlerTimedOut）：超时与否以
// runCtx 的取消原因为准，而不是看 Handler 返回了什么错误——Handler 完全可能在
// 超时之后又返回一个不相干的错误，而我们要计数的是"它因超时被叫停"这件事本身。
func (w *Worker) execute(ctx context.Context, task ClaimedTask) (error, bool) {
	handler, err := w.registry.Resolve(task.Type)
	if err != nil {
		return err, false
	}

	timeout := task.Timeout
	if timeout <= 0 {
		timeout = w.cfg.HandlerTimeout
	}
	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	execErr := runHandler(handler, runCtx, task.Payload)
	return execErr, errors.Is(context.Cause(runCtx), context.DeadlineExceeded)
}

// panicStackLines 是 panic 堆栈写入 last_error 时的最大行数。
//
// 堆栈是判断"panic 出自 Handler 本身还是它调用的库"最有用的东西，但 `debug.Stack()`
// 动辄几十行，而 last_error 列只有 2048 字符、且定位是给人读的摘要——超出部分
// 反正会被 truncateRunes 截掉，不如自己先截，还能保住摘要开头那句关键信息。
const panicStackLines = 12

// runHandler 在**单任务边界**运行一次 Handler，把 panic 转成普通错误。
//
// 需求文档 §4.2：Handler panic 由 Worker 捕获并视为一次失败，不得使整个 Worker
// 进程意外退出。这里给出的错误**刻意不打永久标记**——需求说的只是"视为一次失败"，
// 而 panic 未必是确定性的（可能依赖外部状态或时序），直接判死会丢掉恢复的机会；
// 上限由 max_attempts 兜底，重试耗尽同样进 dead。
//
// 边界刻意收得很窄，**只包住 Handler 调用本身**：本库自己的领取、写回、回收逻辑
// 若 panic，属于本库的 bug，应当照常崩掉，不该被伪装成"某条任务失败"而藏起来
// （技术设计 §5.5「进程级不可恢复错误不应被吞掉」）。
//
// 局限：只能捕获**同一个 goroutine** 里的 panic。Handler 若在自己的子协程里 panic
// （`go func(){ ... }()`），本函数拦不到，Go 运行时会直接终止进程。这条写进文档，
// 因为"Worker 兜底"容易被误读成"随便怎么 panic 都安全"。
func runHandler(handler Handler, ctx context.Context, payload json.RawMessage) (err error) {
	defer func() {
		r := recover()
		if r == nil {
			return
		}
		err = &panicError{value: r, stack: panicStack()}
	}()
	return handler(ctx, payload)
}

// panicError 是 Handler panic 被兜底转换出的错误。
//
// 用**类型**而不是给消息加前缀，是因为指标与日志都需要判断"这次失败是不是
// panic"；靠匹配 "handler panicked: " 这个前缀会在哪天改了措辞时静默失效，
// 而那正是这段代码最该可靠的时候。错误文案与引入类型之前逐字一致，因此既有
// 断言（含 example 里那条）不受影响。
type panicError struct {
	value any
	stack string
}

func (e *panicError) Error() string {
	return fmt.Sprintf("handler panicked: %v\n%s", e.value, e.stack)
}

// panicStack 返回截断后的 panic 堆栈（不含首行的 goroutine 头）。
//
// 首行形如 `goroutine 42 [running]:`，只说明"现在在跑"，对定位没有价值；
// 真正有用的是紧随其后的 Handler 帧。
func panicStack() string {
	stack := debug.Stack()
	if idx := bytes.IndexByte(stack, '\n'); idx >= 0 {
		stack = stack[idx+1:]
	}

	lines := bytes.SplitN(stack, []byte("\n"), panicStackLines+1)
	if len(lines) > panicStackLines {
		lines = append(lines[:panicStackLines], []byte("..."))
	}
	return string(bytes.Join(lines, []byte("\n")))
}

// fail 把一次执行失败写回存储。
//
// 只有两种去向：不再重试（dead），或回到 pending 等下一次。
// "不再重试"有两个彼此独立的理由，合在一个判断里——
// 带永久标记（重试不会让它变好），或尝试次数已耗尽。
//
// 注意这里**不为两个理由写不同的 last_error**：摘要就是 cause.Error()，
// 永久失败的可辨识性来自 Permanent 包装器自带的前缀（"permanent failure: "），
// 而不是来自走的是哪个分支。所以"先判永久再判耗尽"与反过来在结果上完全等价，
// 不必（也不该）为顺序写断言。
//
// 找不到 Handler（Registry.Resolve 返回永久 ErrNoHandler）与 payload 解码失败
// （TypedHandler 返回永久 ErrInvalidPayload）都落在第一条上，因此它们不会白白
// 重试到次数耗尽（需求文档 §4.2）。
func (w *Worker) fail(ctx context.Context, task ClaimedTask, cause error, elapsed time.Duration) {
	summary := cause.Error()

	// Attempt 是"包含本次"的尝试次数，因此 Attempt >= MaxAttempts 即已耗尽。
	if IsPermanent(cause) || task.Attempt >= task.MaxAttempts {
		w.markDead(ctx, task, cause, elapsed)
		return
	}

	delay := w.retryDelay(task.Attempt)
	if err := w.store.Retry(ctx, task, delay, summary); err != nil {
		w.noteLeaseLostOnWriteBack(task, err)
		w.report(ctx, fmt.Errorf("retry task %d: %w", task.ID, err))
		return
	}
	w.cfg.Metrics.TaskFinished(task.Queue, task.Type, OutcomeRetried, elapsed)
	w.logf(ctx, LevelWarn, "task failed; retry scheduled",
		append(w.taskLogFields(task), "delay", delay, logKeyError, cause)...)
}

// markDead 把任务写回死信终态。写回失败通常是租约丢失，只上报。
//
// 只在写回真的落地后记日志：MarkDead 被 fencing 拒掉时任务并没有进死信，
// 此时记一条"已死信"会让日志与库里的状态互相矛盾。
func (w *Worker) markDead(ctx context.Context, task ClaimedTask, cause error, elapsed time.Duration) {
	if err := w.store.MarkDead(ctx, task, cause.Error()); err != nil {
		w.noteLeaseLostOnWriteBack(task, err)
		w.report(ctx, fmt.Errorf("mark task %d dead: %w", task.ID, err))
		return
	}
	w.cfg.Metrics.TaskFinished(task.Queue, task.Type, OutcomeDead, elapsed)
	w.logf(ctx, LevelWarn, "task marked dead",
		append(w.taskLogFields(task), "duration", elapsed, logKeyError, cause)...)
}

// retryBackoffCap 返回第 attempt 次尝试失败后、下一次重试延迟的上界：
// base × 2^(attempt-1)，以 max 封顶。
//
// 抽成纯函数是为了让"退避确实按指数增长、并且确实封顶"可以被**确定性**断言，
// 而不必去观察随机化之后的延迟——后者只能给出区间。
func retryBackoffCap(attempt int, base, max time.Duration) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	cap := base
	for i := 1; i < attempt; i++ {
		// 先判断再翻倍：MaxAttempts 是可配置的，attempt 可能很大，
		// 一路左移会溢出成负数（那会变成"没有退避"）。
		if cap > max/2 {
			return max
		}
		cap *= 2
	}
	if cap > max {
		return max
	}
	return cap
}

// retryDelay 计算一次重试的延迟。
//
// 分布取 [cap/2, cap]（equal jitter），**不是**技术设计 §5.5 最初写的
// full jitter [0, cap]——偏离是刻意的，理由有两条：
//
//  1. 这个延迟最终会落成 run_at，等于 0 就是"立刻可重试"。对已经故障的下游
//     没有任何好处，只是更快吃掉剩余尝试次数；cap/2 的下限保证每次重试都至少
//     等到与当前退避级别相称的一段时间。
//  2. 有下限才**可断言**：run_at 的增量能被区间断言卡住，而 [0, cap] 只能断言
//     上界，"退避根本没生效"这类实现错误检查不出来。
//
// 仍然满足需求文档 §4.5 的"有上限的指数退避 + 随机抖动"：上界按 2 的幂增长并封顶，
// 抖动把并发重试摊开在半个窗口上。全库只保留这一处抖动口径——空队列轮询用的
// 也是 jitter()，理由相同（轮询的延迟为 0 就是忙等）。
func (w *Worker) retryDelay(attempt int) time.Duration {
	return jitter(retryBackoffCap(attempt, w.cfg.RetryBaseDelay, w.cfg.RetryMaxDelay))
}

// pickQueue 在监听队列之间轮转。只有一个队列时直接返回它。
func (w *Worker) pickQueue() string {
	queues := w.cfg.Queues
	if len(queues) == 1 {
		return queues[0]
	}
	n := w.rr.Add(1) - 1
	return queues[n%uint64(len(queues))]
}

// sleep 睡一段带抖动的退避时间，并返回下一轮更大的退避值（封顶 MaxPollInterval）。
func (w *Worker) sleep(ctx context.Context, backoff time.Duration) time.Duration {
	d := backoff
	if d < w.cfg.PollInterval {
		d = w.cfg.PollInterval
	}
	next := d * 2
	if next > w.cfg.MaxPollInterval {
		next = w.cfg.MaxPollInterval
	}
	_ = sleepContext(ctx, jitter(d))
	return next
}

// report 上报一个与单个任务无关的基础设施错误，或一条任务被放弃的异常结果。
//
// 两个出口同时走：onError 是本包测试用的观察缝隙，日志才是给部署方的。日志一律
// 用**脱离取消**的 context——report 报的都是"已经发生的事实"，不该因为 ctx 恰好
// 被取消而丢掉（停机路径上报的批次遗留、写回失败都属于这一类）。
func (w *Worker) report(ctx context.Context, err error) {
	if w.onError != nil {
		w.onError(err)
	}
	w.logf(context.WithoutCancel(ctx), LevelError, "worker error",
		logKeyWorkerID, w.owner, logKeyError, err)
}

// logf 记一条结构化日志。未配置 Logger 时静默——"要不要日志"是部署决定，
// 不是配置错误，因此 nil 检查集中在这一处，调用点不必各自判空。
func (w *Worker) logf(ctx context.Context, level Level, msg string, kv ...any) {
	if w.cfg.Logger == nil {
		return
	}
	w.cfg.Logger.Log(ctx, level, msg, kv...)
}

// taskLogFields 返回一条任务的公共日志字段。
//
// 字段名是稳定的 snake_case，与需求 §8 列出的名字一一对应（task_id / task_type /
// queue / attempt / lease_epoch / worker_id，另加 max_attempts 便于判断是否已是
// 最后一次机会），便于接入方建索引或写告警查询。**刻意不含 payload**。
func (w *Worker) taskLogFields(task ClaimedTask) []any {
	return []any{
		logKeyTaskID, uint64(task.ID),
		logKeyTaskType, task.Type,
		logKeyQueue, task.Queue,
		logKeyAttempt, task.Attempt,
		logKeyMaxAttempts, task.MaxAttempts,
		logKeyLeaseEpoch, task.Epoch,
		logKeyWorkerID, w.owner,
	}
}

// sleepContext 睡 d，ctx 取消则提前返回。d <= 0 时立即返回。
func sleepContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// jitter 返回 [d/2, d] 之间的一个随机时长：空队列轮询时让多个 Worker 错开，
// 避免它们在同一时刻一起醒来抢锁。
func jitter(d time.Duration) time.Duration {
	if d <= 1 {
		return d
	}
	half := d / 2
	return half + time.Duration(rand.Int64N(int64(half)+1))
}

// newOwnerID 生成一个在集群内足够唯一的租约持有者标识。
//
// 形如 host-pid-16hex：主机名 + 进程号 + 随机数，既能一眼看出是哪个进程，
// 也能保证同一台机器上重启后的新进程不会与旧进程撞名。
func newOwnerID() string {
	// 拿不到主机名时 os.Hostname 返回空串，由 ownerID 回退到 "worker"，
	// 因此这里不必单独分支处理错误。
	host, _ := os.Hostname()
	return ownerID(host, os.Getpid(), rand.Uint64())
}

// ownerID 把持有者标识的各个部分拼成一个字符串。
//
// lease_owner 的列宽由方言决定（MySQL 为 VARCHAR(128)）。这里把主机名截到
// 64 字符，整串最长 64+1+7+1+16 = 89 字符，给列宽留足余量。
func ownerID(host string, pid int, nonce uint64) string {
	if host == "" {
		host = "worker"
	}
	host = truncateRunes(host, 64)

	return fmt.Sprintf("%s-%d-%016x", host, pid, nonce)
}

// truncateRunes 按字符（而非字节）截断 s 到最多 n 个字符。
func truncateRunes(s string, n int) string {
	if n <= 0 {
		return ""
	}
	if utf8.RuneCountInString(s) <= n {
		return s
	}
	return string([]rune(s)[:n])
}
