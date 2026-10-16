# GoRangeTask

GoRangeTask 是一个可嵌入 Go 服务的 **MySQL 持久化任务队列**，面向由本服务 Worker 执行的后台任务。

> **项目状态：早期开发中。**
>
> **已可用**：任务与配置类型、Handler 注册表与 payload 解码、公开错误、MySQL migration（版本管理 + 脏状态检测）、完整的 MySQL Client（`Enqueue` / `EnqueueTx` / `Get` / `Cancel`，含幂等键），以及 Worker 侧的领取与执行闭环（`Claim` / `Succeed` / `Retry` / `MarkDead`、并发 `Run` 轮询、**租约续期与失租取消**、**过期租约回收**、**失败退避与永久错误分流**、**Handler panic 兜底**、**终态任务清理**、优雅停机、逐任务超时）。单元测试与真库集成测试覆盖上述实现，CI 已接入 MySQL。另有一个**可独立运行的示例服务** `example/basic`（业务行与任务行同事务创建、幂等 Handler、`cmd/demo` 入口），见下文「安装与快速开始」。
>
> **尚未实现**：结构化日志与指标、GORM adapter。
>
> **已知边界**：清理是**物理删除**。任务行被删掉之后它的幂等键就不再占用唯一索引，同一个键可以被重新接受——库保证的是"保留期内不重复入队"，不是"永不重复"。需要更强语义时要在业务侧自行去重。终态清理的保留期默认 7 天（`succeeded` / `canceled`）与 30 天（`dead`）；把 `CleanupInterval` 设成负值会关闭清理器，终态表将随任务吞吐单调增长。
>
> 当前文档描述的是目标设计与验收计划，不代表所有功能已经可用。进度见[项目进度跟踪](docs/GoRangeTask-进度跟踪.md)。

## 项目定位

业务服务可以在**自己的数据库事务**中写入业务数据和 GoRangeTask 任务行。事务提交后，GoRangeTask Worker 从 MySQL 领取任务并调用本地注册的 Go Handler。

同一事务插入业务数据和任务行，直接 SQL 就可以实现。GoRangeTask 不会比 MySQL 事务提供更强的原子性保证。它的目标是把任务行写入之后的执行生命周期封装为可复用库：并发领取、租约续期、Worker 故障恢复、失败重试、旧租约写回保护和死信管理。

```mermaid
flowchart LR
    A[业务 Producer] -->|同一事务：业务数据 + 任务行| DB[(MySQL)]
    DB -->|领取任务 / 租约| W[GoRangeTask Worker]
    W --> H[本地 Go Handler]
    H -->|成功确认或失败重试| DB
```

GoRangeTask 适用于生产者与 Handler 共用数据库、任务由本服务执行且需要可靠恢复的场景。简单任务也可以直接用 SQL 自行实现；使用 GoRangeTask 的理由是复用并验证完整的并发和故障处理机制。

## 执行语义与边界

GoRangeTask 目标是让已提交任务在数据库和 Worker 可用、重试策略允许的情况下至少交给 Handler 尝试执行一次。Handler 可能重复运行，也可能因永久错误或达到最大尝试次数进入死信。

- **不保证 Handler 必定成功。** 长期故障、永久错误或重试次数耗尽时，需要告警或人工处理。
- **不保证 exactly-once（恰好一次）外部副作用。** Handler 应实现幂等；fencing token 保护的是 GoRangeTask 任务状态，不会撤销已经发生的外部操作。
- **恢复只在 Worker 进程内进行。** 租约过期的任务由运行中的 Worker 内置的回收器捡回（`Config.ReapInterval`，负值可关闭该职责），所以"至少有一个 Worker 在运行"是进程崩溃、停机超时之后任务能被重新执行的前提；全部 Worker 下线期间，任务只是停在 `running` 等着，不会自己回到队列。
- **不替代 RabbitMQ。** 需要跨服务/跨语言分发、广播、路由、Broker 缓冲或多个独立订阅者时，继续使用 Outbox + RabbitMQ 等事件架构。GoRangeTask 不充当 Outbox 转发器。
- **不追求默认高于 Broker 的吞吐。** MySQL 查询、锁等待、连接池和轮询成本需要在目标负载下实测。

## MVP 目标能力

以下是目标功能，尚未全部实现：

- `EnqueueTx`：在调用方现有数据库事务中写入任务，不接管事务的提交或回滚。
- 立即执行和延迟执行。
- MySQL InnoDB 多 Worker 并发领取，使用短事务和 `FOR UPDATE SKIP LOCKED`。
- 数据库时间基准的租约、续期、过期恢复。
- 递增 `lease_epoch`（fencing token），拒绝旧租约持有者写回任务状态。
- 可配置重试次数、有上限的指数退避 + 抖动、永久失败和死信。
- 可选幂等键、任务状态查询、取消尚未领取的任务。
- 优雅停机、结构化日志、基础指标和终态任务清理。
- 可独立运行的示例，演示事务入队、Worker 执行和故障恢复。

## 技术基线

| 项目 | 选择 |
|---|---|
| 公开项目名 | GoRangeTask |
| 仓库名 | `gorangetask` |
| Go module | `github.com/Tidehyc/gorangetask` |
| Go 版本 | 1.24+ |
| 数据库 | MySQL 8.0+ / InnoDB；MVP 只验证 MySQL |
| SQL 层 | 核心包使用 `database/sql`，不依赖 GORM；GORM adapter 为可选组件 |
| 许可证 | MIT |

数据库任务表承担本地任务持久化与 Worker 领取，不经过 RabbitMQ。未来支持其他数据库时，需要单独实现并验证该数据库的 migration、领取 SQL、租约 SQL 和并发语义。

## 本地 MySQL 环境

仓库提供 `scripts/dev-mysql.sh`，用容器化 MySQL 8.0 作为本地开发库：

```bash
./scripts/dev-mysql.sh up      # 启动 MySQL 容器
./scripts/dev-mysql.sh status  # 查看状态
./scripts/dev-mysql.sh shell   # 打开 mysql 客户端
./scripts/dev-mysql.sh down    # 停止容器，保留数据卷
./scripts/dev-mysql.sh reset   # 删除容器和数据卷
```

默认连接信息：`127.0.0.1:3306`，数据库 `gotask`，用户 `root`，密码 `rootpw`，UTC 时区，`READ-COMMITTED` 隔离级别。该环境用于本地开发；不要将示例密码用于共享或生产环境。

## 安装

```bash
go get github.com/Tidehyc/gorangetask
```

核心包只用标准库的 `database/sql`，唯一的直接依赖是 MySQL 驱动（由使用方自己引入并注册）。最低 Go 1.24。

接入的步骤顺序不能换：

1. **迁移任务表**：`mysqlmigrations.Migrate(ctx, db)` 建出 `gotask_tasks` 并管理版本；
2. **建业务表**：业务表属于使用方，库对它一无所知；
3. **构造 store**：`mysqlstore.New(db, clientCfg)`——它**只读表、不建表**，所以必须排在两步建表之后。

```go
ctx := context.Background()

db, err := sql.Open("mysql", dsn) // dsn 必须带库名；库不替调用方创建数据库
// ... ping、错误处理 ...

if err := mysqlmigrations.Migrate(ctx, db); err != nil {
	return err
}
client, err := mysqlstore.New(db, gorangetask.ClientConfig{})
// ... 错误处理 ...
```

## 快速开始：业务数据与任务同事务

这是整个库存在的理由。业务行与任务行写在**同一个 `*sql.Tx`** 上——`EnqueueTx` 不开、不提交、不回滚这个事务，它只是借用你的连接插入一行：

```go
tx, err := db.BeginTx(ctx, nil)
if err != nil {
	return err
}
defer func() { _ = tx.Rollback() }() // 提交成功后返回 sql.ErrTxDone，可忽略

res, err := tx.ExecContext(ctx,
	"INSERT INTO orders (customer, amount_cents) VALUES (?, ?)", customer, amountCents)
if err != nil {
	return err
}
orderID, err := res.LastInsertId()
if err != nil {
	return err
}

payload, err := json.Marshal(map[string]int64{"order_id": orderID})
if err != nil {
	return err
}

// 同一个 tx：提交则订单与任务一起可见，回滚则两者都不存在。
taskID, err := client.EnqueueTx(ctx, tx, gorangetask.Task{
	Queue:   "orders",
	Type:    "order.export",
	Payload: payload,
})
if err != nil {
	return err // 注意：此时 tx 还没提交，taskID 不具备"任务已存在"的含义
}

if err := tx.Commit(); err != nil {
	return err
}
```

Worker 侧注册 Handler 并运行。**Handler 必须幂等**——库的语义是「至少一次」，进程崩溃、租约过期、停机超时都会让同一条任务被重新领取：

```go
registry := gorangetask.NewRegistry()
if err := registry.Register("order.export", func(ctx context.Context, raw json.RawMessage) error {
	var p struct {
		OrderID int64 `json:"order_id"`
	}
	if err := json.Unmarshal(raw, &p); err != nil {
		// payload 是入队时写死的，重试不会让它变得可解析 ⇒ 带永久标记，直接进死信，
		// 而不是白试到次数耗尽。
		return gorangetask.Permanent(fmt.Errorf("decode payload %s: %w", raw, err))
	}

	// 幂等的来源：条件更新。第一次影响 1 行，第二次影响 0 行——0 行不是错误。
	res, err := db.ExecContext(ctx, `UPDATE orders SET status = 'exported'
        WHERE id = ? AND status = 'pending'`, p.OrderID)
	if err != nil {
		return err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		// 简化版：把 0 行当幂等命中。完整写法还要回读业务状态，把「已经导出过」
		// （不是错误）与「订单根本不存在」（应当返回 gorangetask.Permanent）
		// 分开——见 example/basic/handler.go。
		return nil
	}
	return nil
}); err != nil {
	return err
}

worker, err := gorangetask.NewWorker(client, registry, gorangetask.Config{Queues: []string{"orders"}})
if err != nil {
	return err
}

// 阻塞运行直到 ctx 被取消；取消触发优雅停机（Handler 在 ShutdownTimeout 内没退出
// 会返回 ErrShutdownTimeout，被放弃的任务保持 running，交给回收器）。
if err := worker.Run(ctx); err != nil {
	return err
}
```

一份完整、可直接运行的版本在 [`example/basic`](example/basic)（表结构、幂等 Handler 的完整写法、可运行入口都在里面）：

```bash
./scripts/dev-mysql.sh up
go run ./example/basic/cmd/demo              # 造 5 笔订单，起一个 Worker，打印进度
go run ./example/basic/cmd/demo -orders 0    # 只起 Worker，接手之前遗留的任务
go run ./example/basic/cmd/demo -h           # 连接串、订单数、Handler 延迟、打印间隔
```

## 文档

- [需求文档](docs/GoRangeTask-需求文档.md)：项目目标、MVP 范围、状态机、API 草案、可观测性和验收标准。
- [技术设计](docs/GoRangeTask-技术设计.md)：任务表 DDL、领取/续租/确认/回收 SQL、API 与验证计划；**附录 A** 是一次实测事故的完整留档——`SELECT ... FOR UPDATE LIMIT n` 的加锁范围由执行计划决定，计划一离开预期索引就会锁住整个积压、`SKIP LOCKED` 失效，附 `EXPLAIN`/`data_locks` 对照与固定手段。
- [迁移机制](docs/GoRangeTask-迁移机制.md)：MySQL DDL、dirty 状态、advisory lock、多语句处理及实测验证。
- [进度跟踪](docs/GoRangeTask-进度跟踪.md)：阶段状态、完成标准和可核验的实现证据。
- [STAR 说明](docs/GoRangeTask-STAR.md)：项目定位、与手写 SQL 及 Outbox/RabbitMQ 的区别、简历证据框架。

## 许可

本项目使用 [MIT License](LICENSE)。
