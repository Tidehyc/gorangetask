// Command demo 是 example/basic 的可运行入口：造几笔订单、起一个 Worker，
// 然后周期性地把订单与任务的状态打印出来。
//
//	go run ./example/basic/cmd/demo
//	go run ./example/basic/cmd/demo -orders 3 -watch 500ms
//	go run ./example/basic/cmd/demo -orders 0            # 只起 Worker，接手遗留任务
//
// 连接串从 GOTASK_EXAMPLE_DSN 读，缺省用 scripts/dev-mysql.sh 起的本地容器。
//
// Ctrl-C 是优雅停机：正在执行的 Handler 会收到取消，已经成功的照常写回，尚未完成
// 的任务保持 running，等租约自然过期后由回收器接手。README.md 里的「故障演示」
// 就是照着这条路径走的。
//
// 关于可观测性：库目前还没有公开的错误出口（§5「结构化日志」），因此这个 demo
// 不打印 Worker 侧的基础设施错误，而是**直接读库**把状态摊开给人看。任务卡住时
// 看得到 status 与 last_error，这正是现在唯一可靠的观测面。
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"text/tabwriter"
	"time"

	gorangetask "github.com/Tidehyc/gorangetask"
	"github.com/Tidehyc/gorangetask/example/basic"
)

// defaultDSN 与 scripts/dev-mysql.sh 起的容器一致，并带上驱动的读写超时——
// go-sql-driver 默认不给读写设 deadline，缺了它数据库一停顿就会无限期挂住。
const defaultDSN = "root:rootpw@tcp(127.0.0.1:3306)/gotask" +
	"?parseTime=true&loc=UTC&timeout=5s&readTimeout=30s&writeTimeout=30s"

// dsnEnv 是覆盖连接串的环境变量名。
const dsnEnv = "GOTASK_EXAMPLE_DSN"

// workerStopGrace 是收尾时等待 Worker 退出的上限，仅为兜底，防止 demo 卡住。
const workerStopGrace = 30 * time.Second

type options struct {
	dsn          string
	orders       int
	handlerDelay time.Duration
	watch        time.Duration
}

// trackedOrder 把 demo 关心的两侧 ID 配成一对：业务侧订单与它对应的任务。
type trackedOrder struct {
	orderID int64
	taskID  gorangetask.TaskID
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "demo: %v\n", err)
		os.Exit(1)
	}
}

func parseFlags() options {
	var opts options

	flag.StringVar(&opts.dsn, "dsn", "", "MySQL DSN（缺省读 "+dsnEnv+"，再缺省用本地开发容器）")
	flag.IntVar(&opts.orders, "orders", 5, "启动时创建的订单数；0 表示只起 Worker，接手之前遗留的任务")
	flag.DurationVar(&opts.handlerDelay, "handler-delay", 0,
		"导出 Handler 在写回前先等待多久（用于演示停机打断在途任务）")
	flag.DurationVar(&opts.watch, "watch", time.Second, "打印状态的间隔")
	flag.Parse()

	if opts.dsn == "" {
		opts.dsn = os.Getenv(dsnEnv)
	}
	if opts.dsn == "" {
		opts.dsn = defaultDSN
	}
	return opts
}

func run() error {
	opts := parseFlags()

	// Ctrl-C / SIGTERM 取消 ctx，随后走库的优雅停机路径。
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	app, err := basic.Open(ctx, opts.dsn, basic.Options{HandlerDelay: opts.handlerDelay})
	if err != nil {
		return err
	}
	defer func() { _ = app.Close() }()

	fmt.Printf("connected; queue=%q task type=%q\n", basic.Queue, basic.TaskTypeExportOrder)

	tracked, err := createOrders(ctx, app, opts.orders)
	if err != nil {
		return err
	}

	// workerDone 只由收尾的 stopWorker 读取。Worker 万一提前退出（基础设施问题），
	// 这里顺手取消 ctx 让打印循环也停下来，免得 demo 在那儿一直刷同样的状态。
	// workerExited 让打印循环能把"Worker 自己退了"与"用户按了 Ctrl-C"分开报，
	// 否则两种情况都会显示成 interrupted。
	var workerExited atomic.Bool
	workerDone := make(chan error, 1)
	go func() {
		err := app.Run(ctx)
		workerExited.Store(true)
		stop()
		workerDone <- err
	}()
	fmt.Printf("worker started; press Ctrl-C to stop (watching every %s)\n\n", opts.watch)

	reason := watch(ctx, stop, app, tracked, opts.watch, &workerExited)

	fmt.Printf("\nshutting down: %s\n", reason)
	return stopWorker(workerDone, app, tracked)
}

// createOrders 创建订单。每一次调用都是一笔独立事务：订单行与任务行一起提交，
// 或者一起不存在——这正是示例要展示的那个事务边界。
func createOrders(ctx context.Context, app *basic.App, n int) ([]trackedOrder, error) {
	tracked := make([]trackedOrder, 0, n)
	for i := 0; i < n; i++ {
		customer := fmt.Sprintf("customer-%02d", i+1)
		amount := int64((i + 1) * 1999)

		orderID, taskID, err := app.Store.CreateOrder(ctx, customer, amount)
		if err != nil {
			return nil, err
		}
		tracked = append(tracked, trackedOrder{orderID: orderID, taskID: taskID})
		fmt.Printf("created order %d (task %d) in one transaction\n", orderID, taskID)
	}
	return tracked, nil
}

// watch 按 watch 间隔打印进度，返回停机原因。
//
// 结束条件有三个：所有订单都导出完成（主动请求停机）、用户按了 Ctrl-C
// （signal.NotifyContext 取消 ctx），或 Worker 自己先退了（基础设施问题，
// 启动它的那个 goroutine 会顺手取消 ctx）。
//
// ctx.Done() 这三种来源都走同一条通道，得靠 workerExited 把它们分开报——
// 否则 Worker 自己挂掉也会显示成 "interrupted"，把人引到错误的方向。
func watch(ctx context.Context, stop context.CancelFunc, app *basic.App, tracked []trackedOrder, interval time.Duration, workerExited *atomic.Bool) string {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			if workerExited.Load() {
				return "worker stopped on its own"
			}
			return "interrupted"
		case <-ticker.C:
			allDone, err := printProgress(ctx, app, tracked)
			if err != nil {
				// 打印失败（比如连接断了）不该让整个 demo 崩掉：说明白然后走收尾，
				// 让停机路径把最终状态再报一次。
				fmt.Fprintf(os.Stderr, "demo: read progress: %v\n", err)
				stop()
				return "progress read failed"
			}
			if allDone {
				stop() // 已经没事可做了，请求优雅停机。
				return "all orders exported"
			}
		}
	}
}

// printProgress 打印一行一行的状态表，返回"是否所有订单都已导出完成"。
//
// 数据全部走公开 API：业务侧用 Store.GetOrder，任务侧用 Client.Get。
// demo 不需要（也不应该）自己写 SQL 去读库里的任务表。
func printProgress(ctx context.Context, app *basic.App, tracked []trackedOrder) (bool, error) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "order\tstatus\ttask\tstatus\tattempt\terror")

	allDone := true
	for _, item := range tracked {
		order, err := app.Store.GetOrder(ctx, item.orderID)
		if err != nil {
			return false, err
		}
		info, err := app.Client.Get(ctx, item.taskID)
		if err != nil {
			return false, err
		}
		if order.Status != basic.OrderExported || !info.Status.IsTerminal() {
			allDone = false
		}
		fmt.Fprintf(w, "%d\t%s\t%d\t%s\t%d\t%s\n",
			order.ID, order.Status, int64(info.ID), info.Status, info.Attempt, info.LastError)
	}
	if err := w.Flush(); err != nil {
		return false, err
	}
	return allDone, nil
}

// stopWorker 等 Worker 退出，并把停机那一刻的遗留状态说清楚。
//
// 还没进入终态的任务不一定是坏了：停机时正在执行的任务会**保持 running**，等租约
// 自然过期后由回收器接手。这里把这些任务点出来，正是为了让"至少一次"这件事可见，
// 而不是让人以为任务丢了。
func stopWorker(workerDone <-chan error, app *basic.App, tracked []trackedOrder) error {
	var workerErr error
	select {
	case workerErr = <-workerDone:
	case <-time.After(workerStopGrace):
		fmt.Fprintf(os.Stderr, "worker did not stop within %s; giving up on waiting\n", workerStopGrace)
	}

	// Worker 因 ctx 取消而返回 nil 是正常的；停机超时（ErrShutdownTimeout）说明
	// 有 Handler 不理会取消，要说出来，但不影响后面的状态汇总。
	if workerErr != nil && !errors.Is(workerErr, context.Canceled) {
		fmt.Fprintf(os.Stderr, "worker: %v\n", workerErr)
	}

	// 用一个新的 ctx 读最终状态：进来的 ctx 已经取消，读不了库。
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	leftover := 0
	for _, item := range tracked {
		info, err := app.Client.Get(ctx, item.taskID)
		if err != nil {
			return err
		}
		if !info.Status.IsTerminal() {
			leftover++
			fmt.Printf("task %d (order %d) is still %s\n", int64(info.ID), item.orderID, info.Status)
		}
	}
	if leftover > 0 {
		fmt.Printf("\n%d task(s) left unfinished. They stay as-is until the lease expires,\n"+
			"then the reaper hands them back to the queue. Restart with `-orders 0` to\n"+
			"watch a fresh worker pick them up (see README.md).\n", leftover)
	}
	return nil
}
