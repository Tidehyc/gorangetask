package gorangetask

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
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
//   - 失败：写回 pending 立即重试；若尝试次数已耗尽则写为 dead。
//   - 优雅停机（§3「Worker graceful shutdown」）：见 Run 的说明。
//   - 租约续期与失租取消（§4「租约续期」「lease 丢失取消 context」）：见下方
//     「本次执行的三条取消路径」。
//   - 过期租约回收（§4「过期租约回收」）：见下方「过期租约回收器」。
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
// 尚未实现的语义（后续工作项，文档已排期）：
//   - **指数退避与永久错误（§4）**：失败一律立即重试，不区分 Permanent，
//     也不做退避；因此永久失败的任务会尝试到次数耗尽才进入 dead。
//   - **panic 隔离（§4）**：Handler panic 会向上冒泡，尚未被单任务边界拦截。
//   - **终态清理（§4）**：succeeded / dead / canceled 目前不会被删除。
//   - **可观测性（§5）**：基础设施错误目前没有公开出口，只能通过内部 onError 观察；
//     ReapResult 的两个计数也还没有消费方。
type Worker struct {
	store    WorkerStore
	registry *Registry
	cfg      Config
	owner    string

	// onError 接收与单个任务无关的基础设施错误（领取失败、写回失败等）。
	// 目前没有公开的日志出口，这里是留给自己与测试的内部缝隙，默认丢弃；
	// §5「结构化日志」会把它换成真正的日志句柄。
	onError func(error)

	// rr 用于在多个监听队列之间轮转，避免总是先打同一个队列。
	rr atomic.Uint64
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

	// 回收器的存亡只由 ctx 与配置决定，与在途 Handler 无关，所以单独一个信号。
	// 关闭回收器时直接关掉它，下面的等待逻辑不必再分情况。
	reapDone := make(chan struct{})
	if w.reaperEnabled() {
		go func() {
			defer close(reapDone)
			w.reapLoop(ctx)
		}()
	} else {
		close(reapDone)
	}

	<-ctx.Done()

	// 在途 Handler 拿到的 context 派生自 ctx，取消已经传到它们手里；这里只等
	// 它们在 ShutdownTimeout 内返回，不为不理会取消的 Handler 无限期阻塞。
	timer := time.NewTimer(w.cfg.ShutdownTimeout)
	defer timer.Stop()

	// 两个信号分开观察，只为把超时原因说清楚：是 Handler 没退，还是回收器没停。
	// 置 nil 后该 case 永久阻塞，等同于"这一边已经不用再等了"。
	handlersStopped, reaperStopped := false, false
	for !handlersStopped || !reaperStopped {
		select {
		case <-loopsDone:
			handlersStopped, loopsDone = true, nil
		case <-reapDone:
			reaperStopped, reapDone = true, nil
		case <-timer.C:
			return fmt.Errorf("%w: %s elapsed with %s",
				ErrShutdownTimeout, w.cfg.ShutdownTimeout, stillRunning(handlersStopped, reaperStopped))
		}
	}
	return nil
}

// stillRunning 描述停机超时时还有哪一部分没停下来。
func stillRunning(handlersStopped, reaperStopped bool) string {
	switch {
	case !handlersStopped && !reaperStopped:
		return "handlers still running and the reaper not stopped"
	case !handlersStopped:
		return "handlers still running"
	default:
		return "the reaper not stopped"
	}
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
			w.report(err)
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
	claimed, err := w.store.Claim(ctx, ClaimSpec{
		Queue:         w.pickQueue(),
		Limit:         w.cfg.BatchSize,
		Owner:         w.owner,
		LeaseDuration: w.cfg.LeaseDuration,
	})
	if err != nil {
		return 0, fmt.Errorf("claim tasks: %w", err)
	}

	// 领取事务已经提交，此时任务在库里已是 running 且带着本次的 owner/epoch；
	// 从这里开始执行 Handler 不会持有任何数据库事务或行锁。
	for i, task := range claimed {
		// 停机已经开始就不再启动批次里剩下的 Handler：它们一启动就会拿到已
		// 取消的 context，等于把任务领出来又立刻放弃。保持 running 交给回收器，
		// 比"启动了再取消"更干净。
		if ctx.Err() != nil {
			w.report(fmt.Errorf("shutdown in progress; %d claimed task(s) left running for the reaper", len(claimed)-i))
			break
		}
		w.process(ctx, task)
	}
	return len(claimed), nil
}

// process 执行一条已领取的任务并写回结果。它不返回 error：任务的失败是
// 预期内的结果，写回存储即可；写回本身失败才通过 report 上报。
func (w *Worker) process(ctx context.Context, task ClaimedTask) {
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

	err := w.execute(taskCtx, task)

	// 先停看护、等它退出，再写回。反过来的话，续租可能在任务已经写成终态之后
	// 才到达，因 status 不再是 running 而返回 ErrLeaseLost，凭空报出一条错误；
	// 更糟的是它会把一个已经正常结束的任务标成"失租"。
	close(stop)
	<-watchDone

	// 失租的判断必须排在停机前面：租约一旦易主，任务已归别的持有者，本次结果
	// 无论成功与否都不能再写（写也只会被 fencing 拒掉）。任务保持 running，
	// 交给回收器按新租约处理。
	if cause := context.Cause(taskCtx); errors.Is(cause, ErrLeaseLost) {
		w.report(fmt.Errorf(
			"task %d lost its lease while running; leaving it to the reaper: %w", task.ID, cause))
		return
	}

	// 停机进行中：这次失败是被取消掉的，不代表任务真的失败，不能写回
	// （写回 pending 会让别的 Worker 在旧副作用可能仍在途时立刻重跑）。
	if ctx.Err() != nil {
		if err == nil {
			w.ack(ctx, task)
			return
		}
		w.report(fmt.Errorf("shutdown in progress; task %d left running for the reaper: %w", task.ID, err))
		return
	}

	if err != nil {
		w.fail(ctx, task, err)
		return
	}
	w.ack(ctx, task)
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
		w.report(fmt.Errorf("renew lease for task %d: %w", task.ID, err))
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
			w.report(fmt.Errorf("reap expired leases: %w", err))
			continue
		}
		// 计数暂时没有消费方：Worker 目前没有日志出口，§5「结构化日志」接进来
		// 时这两个数会成为最有价值的一条。真库测试会直接断言它们。
		_ = result
	}
}

// ack 把一条执行成功的任务写回 succeeded 终态。
//
// 正常运行时直接用 ctx。停机期间 ctx 已被取消，此时换用一个脱离该取消、
// 只保留 Config.ShutdownTimeout 作为预算的 context——否则这条已经成功的
// 结果会因为停机取消写不回去，白白多执行一次。
func (w *Worker) ack(ctx context.Context, task ClaimedTask) {
	ackCtx := ctx
	if ctx.Err() != nil {
		var cancel context.CancelFunc
		ackCtx, cancel = context.WithTimeout(context.WithoutCancel(ctx), w.cfg.ShutdownTimeout)
		defer cancel()
	}
	if err := w.store.Succeed(ackCtx, task); err != nil {
		// 写回失败通常是租约过期/易主（ErrLeaseLost）。按设计不做无条件更新补救，
		// 任务的最终命运交给回收器。
		w.report(fmt.Errorf("ack success for task %d: %w", task.ID, err))
	}
}

// execute 解析 Handler 并在超时 context 下运行它。
//
// 超时取 task.Timeout（入队时持久化、领取时还原）；为 0 表示没设置，
// 回落到 Worker 配置的 HandlerTimeout。runCtx 派生自 ctx（即本次执行的
// taskCtx），因此停机、逐任务超时、失去租约这三种取消都能传到 Handler 手里
// ——它们是同一套协作式取消，谁先发生谁生效，Handler 可用 context.Cause
// 分辨自己是被谁叫停的。
func (w *Worker) execute(ctx context.Context, task ClaimedTask) error {
	handler, err := w.registry.Resolve(task.Type)
	if err != nil {
		return err
	}

	timeout := task.Timeout
	if timeout <= 0 {
		timeout = w.cfg.HandlerTimeout
	}
	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	return handler(runCtx, task.Payload)
}

// fail 把一次执行失败写回存储。本项的策略刻意最简：
// 还有次数就立即回到 pending，次数耗尽就进 dead。
func (w *Worker) fail(ctx context.Context, task ClaimedTask, cause error) {
	summary := cause.Error()

	// Attempt 是"包含本次"的尝试次数，因此 Attempt >= MaxAttempts 即已耗尽。
	if task.Attempt >= task.MaxAttempts {
		if err := w.store.MarkDead(ctx, task, summary); err != nil {
			w.report(fmt.Errorf("mark task %d dead: %w", task.ID, err))
		}
		return
	}

	// delay 传 0：立即重试。指数退避与永久错误分类由 §4 补齐
	// （技术设计 §5.5 已给出 full-jitter 公式）。
	if err := w.store.Retry(ctx, task, 0, summary); err != nil {
		w.report(fmt.Errorf("retry task %d: %w", task.ID, err))
	}
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

// report 上报一个与单个任务无关的基础设施错误。
func (w *Worker) report(err error) {
	if w.onError != nil {
		w.onError(err)
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
