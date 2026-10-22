# GoRangeTask 需求文档

> 项目名：GoRangeTask（仓库名：`gorangetask`）。
> 文档版本：v0.4（草案稿）  
> 状态：已按本文完成 MVP 实现与验收（v0.1.0）；本文保留立项时的需求口径，不再随实现回填。  
> 用途：作为独立 GoRangeTask 项目搭建、MVP 实现与验收的主要依据。  
> 项目动机、STAR 叙述及 Outbox/RabbitMQ 边界见《GoRangeTask-STAR.md》。  
> 文中标为“建议”的技术决策与全部验收目标均已在 v0.1.0 落地（唯一例外是 §11 第 6 阶段的可选外部集成，尚未做也不阻塞）；逐项证据见《GoRangeTask-进度跟踪.md》§0–§6，实测数字见《GoRangeTask-性能基线-Ryzen7-8745H.md》（另有一份 4 核机器的历史基线《GoRangeTask-性能基线.md》）。

## 1. 项目目标

GoRangeTask 是一个 Go 事务型任务队列库。应用把任务持久化到关系型数据库，GoRangeTask Worker 从数据库领取任务并直接执行注册的 Go Handler。数据库任务表本身是持久化的本地投递通道，不依赖 RabbitMQ。

GoRangeTask 的首要用途是：为 Go 服务提供可嵌入的持久化任务队列，使调用方能够在同一数据库事务中原子写入业务数据与任务请求，再把已提交任务可靠交给 Worker。GoRangeTask 作为独立项目设计、实现和验收；不依赖 GCFeed 才能证明价值。它还提供延迟调度、多 Worker 安全领取、租约续期与恢复、失败重试、幂等键和任务状态查询。

### 1.1 目标

- 业务数据与任务在同一个数据库事务中提交或回滚。
- 提交的任务通过数据库队列最终交给注册的本地 Handler 尝试执行，无须先转发到另一个 Broker。
- 允许多个 API 实例或 Worker 实例并发运行。
- Worker 进程或主机故障后，任务最终能恢复执行。
- 旧 Worker 失去租约后不能更新任务行的当前状态。
- 明确支持至少一次执行，不承诺 Handler 的外部副作用恰好发生一次。
- 提供足够的日志、指标和测试结果，让使用者理解可靠性与数据库成本。

### 1.2 非目标

- 不实现通用事件总线，不替代 RabbitMQ 的主题路由、广播、跨服务传递和 Broker 削峰。
- 不做 Outbox 到 RabbitMQ 的转发器；若 Handler 再发布到 RabbitMQ，跨系统投递仍需 Outbox 等方案。
- MVP 不支持 Cron/DAG/工作流、任务优先级、Web 管理台、分布式共识、多数据库运行时切换。
- 不保证 exactly-once（恰好一次）外部副作用。
- 不追求与 Redis/RabbitMQ 队列相同的峰值吞吐；适用范围须由压测确定。

## 2. 关键决策建议

| 项目 | 建议 | 理由与待讨论点 |
|---|---|---|
| 数据库 | MVP 使用 MySQL 8.0+、InnoDB；设计允许后续扩展其他数据库 | GCFeed 使用 MySQL；首版集中验证 MySQL 的事务、行锁和 `SKIP LOCKED` 语义。通过存储接口隔离方言差异，但首版不宣称支持 PostgreSQL 等其他数据库。 |
| 项目形态 | 可嵌入 Go 服务的 Go module；同时提供最小 Worker 示例 | `EnqueueTx` 必须加入调用方正在使用的事务；独立 HTTP 队列服务无法直接加入业务事务。Worker 可单独部署进程，但仍使用该库。 |
| SQL 层 | 核心包不依赖 GORM；提供官方可选 GORM 适配包 | GCFeed 使用 GORM。适配包接收调用方已开启的 GORM 事务，并通过该事务插入 GoRangeTask 任务行；任务与业务数据共用同一事务。核心包仍使用明确的 SQL 执行接口，避免强依赖 ORM。 |
| 调度 | Worker 轮询数据库，空队列时退避等待 | MVP 架构简单，无额外 Broker；轮询周期和 DB 负载需压测后定默认值。 |
| 消费语义 | 至少一次 + Handler 幂等 | Worker 崩溃可能发生在副作用完成、状态写回之前，任务会被再次执行。 |

MySQL 的锁定读和 InnoDB 行锁适合队列式并发领取，但 `SKIP LOCKED` 返回的是跳过已锁行后的集合，不应当被当成一般一致性查询使用。领取事务必须短，最终 SQL 和索引要通过 MySQL 集成测试及 `EXPLAIN` 验证。[MySQL 锁定读说明](https://dev.mysql.com/doc/refman/8.0/en/innodb-locking-reads.html)

## 3. 用户与核心场景

### 3.1 使用者

- **Producer（生产者）**：提交任务的业务 API/服务。
- **Worker（工作进程）**：领取并执行任务的进程；可与 API 同进程，也可作为单独部署单元。
- **Handler（处理函数）**：针对一种稳定任务类型注册的 Go 函数。

### 3.2 使用场景

核心场景是：业务事务写入数据的同时创建一个本服务后台任务；事务提交后由 Worker 执行该任务，进程崩溃时可恢复。示例服务应自带一张业务表和任务表：创建订单/报告等业务记录时，通过同一个 MySQL 事务创建任务；示例 Handler 执行可重复的后台处理，并展示幂等效果。示例领域应保持简单，避免把 GoRangeTask 的正确性验证绑定到特定业务项目。

其他核心行为包括多个 Worker 共享队列、任务延迟执行、失败重试、租约到期恢复和旧执行者写回拒绝。上传处理、缓存重建或视频向量生成可作为后续集成示例。GCFeed 不是 MVP 场景，也不是验收前置条件；若未来接入，需单独确认它的数据库事务边界和消费者部署要求。

## 4. MVP 功能需求

### P0：首版必须完成

#### 4.1 任务入队

- 支持立即执行和指定时间后执行。
- 支持 `Enqueue(ctx, task)` 非事务入队。
- 支持 `EnqueueTx(ctx, tx, task)`：只在传入的事务连接上写任务，不自行开启、提交或回滚该事务。
- 事务回滚后任务不可见；事务提交后任务可被 Worker 领取。
- Worker 直接轮询任务表领取已提交任务；MVP 不增加“GoRangeTask 再发布到 RabbitMQ”的二段转发流程。
- 支持可选幂等键；同队列内相同非空幂等键最多成功创建一条有效任务。并发冲突返回稳定、可判断的重复任务结果。
- 任务参数使用 JSON；拒绝无效 JSON、空任务类型和超出配置上限的 payload。MVP 单任务 payload 上限定为 256 KiB，Worker 配置可调低但首版不支持无上限 payload。

#### 4.2 Handler 注册与任务执行

- 使用者按稳定 `task_type` 注册 Handler；任务 payload 需有明确 schema/version 管理约定。
- 找不到 Handler 或无法解码 payload 的任务不能无限快速重试；记录可诊断错误并进入失败策略。
- Handler 接收 `context.Context`，执行超时可配置。
- Handler panic 由 Worker 捕获并视为一次失败；不得使整个 Worker 进程意外退出。
- Handler 不应在 GoRangeTask 内部事务/行锁持续期间运行。

#### 4.3 并发领取

- Worker 只领取其配置监听的队列。
- 多 Worker 通过 MySQL InnoDB 行级锁，在短事务内原子领取到期 `pending` 任务。
- 领取成功时将任务设为 `running`，写入 `lease_owner`、`lease_until`，并递增 `lease_epoch`；提交后再启动 Handler。
- Worker 并发数可配置；每次领取批量大小有上限。MVP 默认批量大小建议为 1，避免任务在本地排队时租约已消耗。
- 不提供优先级，也不承诺多个队列之间的公平调度。

#### 4.4 租约、续期与恢复

- 每个运行任务有数据库时间基准的租约截止时间。
- 长任务由 Worker 定期续租；续租条件包含任务 ID、`lease_owner`、`lease_epoch` 和 `running` 状态。
- 续租影响行数为 0 表示 Worker 已失去租约；Worker 取消 Handler context，并禁止之后写回任务状态。
- Worker 定期回收租约已过期的任务；任务可再次领取，尝试次数按定义增加。
- 每次领取产生递增 `lease_epoch`。成功、失败、延期、续租等写回都必须条件匹配当前租约代次。过期 Worker 的更新必须影响 0 行。
- 租约丢失后的 context cancel 是协作式取消，不保证撤销已发生的外部副作用。

#### 4.5 失败、重试与死信

- Handler 成功则任务变成 `succeeded` 并记录完成时间。
- Handler 返回错误或 panic 则按最大尝试次数决定重试或进入 `dead`。
- 重试使用有上限的指数退避并加入随机抖动，避免失败任务同时重试形成尖峰。
- Handler 可返回永久失败标记，跳过后续重试直接进入 `dead`；未分类错误默认可重试。
- 死信保存错误摘要、尝试次数和时间戳。MVP 提供查询任务状态的方法；命令行死信重放可放入 P1。

#### 4.6 取消和关闭

- MVP 支持取消尚未领取的 `pending` 任务；已 `running` 的取消 API 可暂不提供，避免把“请求取消”和“Handler 已停止”混为一谈。
- 收到关闭信号后停止领取新任务，向运行任务的 Handler 发出 cancel，并在配置的 shutdown timeout 内等待退出。
- 未能确认 Handler 已停止时，不提前把租约释放给其他 Worker；租约自然过期后由其他 Worker 恢复，避免并行执行窗口。

### P1：MVP 后再评估

- 死信查询与人工重放 CLI。
- 完成任务保留期配置和安全清理命令。
- 基于真实负载优化批量领取和空队列轮询。
- 多队列并发配额或公平策略。
- Web 管理界面、Cron、DAG、任务优先级。

## 5. 任务状态与状态转换

MVP 状态：`pending`、`running`、`succeeded`、`dead`、`canceled`。失败等待重试仍为 `pending`，通过未来的 `run_at` 表示可执行时间。

| 当前状态 | 事件 | 新状态 | 说明 |
|---|---|---|---|
| `pending` | 到期且被领取 | `running` | 写新租约与递增代次 |
| `pending` | 用户取消 | `canceled` | 仅尚未领取任务 |
| `running` | Handler 成功且租约仍有效 | `succeeded` | 条件写回 |
| `running` | 可重试错误且未达上限 | `pending` | 写下一次 `run_at` |
| `running` | 永久错误或超过上限 | `dead` | 保存错误摘要 |
| `running` | 租约过期 | `pending` | 恢复器回收；旧 Worker 代次失效 |
| `dead` | 管理员重放（P1） | `pending` | 增加重放审计信息 |

状态写回必须检查 affected rows（受影响行数）；0 行表示当前执行者不再拥有任务，不能将该次结果覆盖到任务记录。

## 6. 数据存储需求

### 6.1 任务表字段

首版至少存储：

```text
id                  主键
queue_name          队列名
task_type           Handler 类型名
payload             JSON 参数
status              任务状态
run_at              最早可领取时间
attempt             已领取/执行尝试计数
max_attempts        最大尝试次数
lease_owner         当前 Worker 实例标识
lease_until         租约截止时间
lease_epoch         单调递增租约代次
idempotency_key     可选幂等键
last_error          最近一次错误摘要
created_at          创建时间
updated_at          更新时间
finished_at         终态时间
```

要求：

- 使用 InnoDB；所有时间以 UTC 存储和比较，租约到期使用数据库时间，避免 Worker 主机时钟漂移造成判断分歧。
- 唯一索引作用域为 `(queue_name, idempotency_key)`；允许 `idempotency_key` 为 NULL。
- 按队列、状态、到期时间建立领取所需复合索引；按状态和租约截止时间支持租约回收。最终索引由真实 SQL、`EXPLAIN` 和压测确定。
- 终态任务不无限期保留。MVP 需定义清理策略或至少提供运维 SQL 说明；具体默认保留期为待讨论项。
- payload 不存放视频文件等大对象，只保存引用或必要的业务 ID。

### 6.2 迁移

- 提供版本化 SQL migration，不在每次应用启动时自动修改生产 schema。
- Migration 可重复检测当前版本；升级失败需返回清晰错误，不静默继续启动 Worker。
- MVP 明确支持 MySQL 版本并验证 `SKIP LOCKED`、JSON 列、索引及隔离级别行为。

## 7. API 与配置草案

API 形状先作为需求，不冻结最终 Go 类型：

```go
type Handler func(context.Context, json.RawMessage) error

type Task struct {
    Queue          string
    Type           string
    Payload        json.RawMessage
    RunAt          time.Time
    MaxAttempts    int
    Timeout        time.Duration
    IdempotencyKey string
}

type EnqueueTx interface {
    ExecContext(context.Context, string, ...any) (sql.Result, error)
}

type Client interface {
    Enqueue(context.Context, Task) (TaskID, error)
    EnqueueTx(context.Context, EnqueueTx, Task) (TaskID, error)
    Get(context.Context, TaskID) (TaskInfo, error)
    Cancel(context.Context, TaskID) error
}
```

`EnqueueTx` 接收一个能在现有事务内执行 SQL 的事务执行器。标准 `*sql.Tx` 可直接适配；官方可选 GORM adapter 提供面向 `*gorm.DB` 的入口，要求传入正在运行的事务对象，并通过该对象执行任务插入，不能另开事务或从连接池取新连接。GORM adapter 放在独立子包，只有使用 GORM 的应用才引入该依赖；核心包不直接依赖 GORM。示例需验证“业务写入 + 任务入队”一同提交、一起回滚。

`Task.Timeout` 为逐任务超时（对应 §4.2「Handler 执行超时可配置」）：零值表示使用 Worker 级默认 Handler 超时，非零值覆盖本次执行。该超时通过传给 Handler 的 `context.Context` deadline 实现，与租约丢失、shutdown 复用同一套协作式取消机制；超时不保证撤销已发生的外部副作用。

Worker 配置至少包括：数据库连接、监听队列、并发数、poll interval、lease duration、heartbeat interval、shutdown timeout、默认 Handler 超时、重试策略、最大 payload 和日志/指标钩子。默认数值在基准测试后确定，并允许覆盖。

## 8. 可观测性要求

日志包含 `task_id`、`task_type`、`queue_name`、`attempt`、`lease_epoch`、`worker_id` 和结果；不默认打印完整 payload 或敏感字段。

基础指标至少包括：

- 按队列和状态的任务数量/积压量。
- 入队、领取、成功、失败、重试、死信和取消计数。
- 排队延迟、处理耗时、领取 SQL 耗时的直方图。
- 租约过期、旧代次写回拒绝、Handler 超时和 panic 计数。
- Worker 活跃并发数和数据库连接池占用（如由宿主应用提供）。

指标标签不得包含 task ID、用户 ID 或其他高基数值。

## 9. 可靠性与安全要求

- 事务入队只保证同一数据库实例/事务中的业务变更和任务创建原子提交；不保证不同数据库之间原子性。
- Handler 可能重复执行；文档和示例必须展示幂等写法。
- fencing token 保护 GoRangeTask 任务行的状态更新，不自动保护外部 API、Redis 或文件系统副作用。
- 数据库暂时不可用时，Worker 记录错误、退避并继续恢复；不可在忙循环中持续重试。
- payload 和错误信息可能含敏感数据；支持日志脱敏，不把密钥放入明文任务参数。
- 任务表应有清理方案，防止成功记录无限增长影响索引和扫描性能。

## 10. 验收标准

### 10.1 正确性

1. 事务回滚后任务行不存在；事务提交后任务可执行。
2. 多 Worker 同时领取时，同一租约期内一条任务只有一个有效租约。
3. Handler 成功、可重试错误、永久错误、panic 分别进入定义的状态。
4. Worker 在领取后退出，租约过期后任务可重新领取。
5. 旧租约代次不能续租、成功写回或失败写回。
6. 幂等键并发提交时，同队列只创建一个有效任务。
7. 关闭 Worker 时停止领取；未确认退出的任务不会被提前释放。

### 10.2 集成与故障测试

- 使用真实 MySQL 8 + InnoDB 集成测试，不以 mock 替代事务、锁和索引语义验证。
- 测试 10,000 次业务事务，随机包含提交/回滚和 API/Worker 进程重启；数据库核对提交的业务记录与任务对应、回滚记录没有任务。目标完整率 100%。
- 并发测试中明确区分“同租约同时只有一个有效持有者”和“租约过期后允许重复执行”。
- 注入旧 Worker 延迟苏醒，验证 fencing 条件写回拒绝。
- 使用 `go test -race` 检查 Worker 内部并发安全。

### 10.3 独立示例端到端验收

示例服务在一笔 MySQL 事务中写入业务记录和 GoRangeTask 任务。集成测试断言提交时两者同时存在、回滚时两者同时不存在；Worker 运行后将业务记录推进到完成状态。通过停止 Worker、等待租约过期、重启 Worker，验证任务最终完成。该示例用于演示 GoRangeTask API，不将其当作独立业务产品。

### 10.4 性能测试

使用固定数据库资源、固定 payload 和记录在案的 Worker/连接池配置，测试不同 Worker 并发和任务大小下的：

- 入队延迟与示例 Producer API p50/p95/p99。
- tasks/second、领取耗时、数据库锁等待和连接池使用。
- 人为积压后的排空时间。
- 空队列轮询带来的数据库 QPS。

不在文档阶段承诺预设吞吐。首版报告任务处理速率、MySQL CPU/锁等待/连接占用、示例 Producer API p95 和任务延迟。若后续与 RabbitMQ 对照，需采用 Outbox 可靠基线并匹配工作负载；若 GoRangeTask 未在目标指标上获益，则如实记录，不宣称生产优化。

## 11. 交付阶段

1. **规格确认**：确认数据库版本、事务适配方式、MVP API 和取消语义。
2. **持久化闭环**：migration、入队、事务入队、状态读取与单 Worker 执行。
3. **并发与恢复**：多 Worker 原子领取、租约、心跳、过期回收、fencing。
4. **失败处理**：panic/error 处理、退避重试、dead 状态、幂等键。
5. **验证与观测**：集成/故障/性能测试、指标和运维说明。
6. **可选外部集成**：MVP 完成后，可评估 GCFeed 向量生成等真实业务集成；它不阻塞 GoRangeTask 发布，也不作为核心验收指标。

## 12. 已确定的范围与实现决策

以下决策按当前项目目标收敛，作为首版需求基线；后续如有新证据再调整：

1. **数据库范围：** MVP 只支持 MySQL 8.0+、InnoDB。代码按存储接口与方言实现分层，方便未来新增数据库，但在相应方言、迁移和并发测试完成前，不宣称支持其他数据库。
2. **项目形态：** GoRangeTask 是可嵌入 Go 服务的库，并提供可独立运行的 Worker 示例。Worker 可与 API 同进程或单独部署；无论部署方式如何，事务入队必须使用业务服务传入的数据库事务。
3. **事务接入：** 核心包提供事务执行器接口，支持 `database/sql`；提供官方可选 GORM adapter，便于 GCFeed 在 GORM 事务中原子插入任务。adapter 是薄封装，不是另一套队列实现，也不负责开启、提交或回滚业务事务。
4. **验证方式：** 以 GoRangeTask 自带的独立示例服务、MySQL 集成测试、并发与故障注入测试作为首版验证。GCFeed 等外部集成仅为可选后续案例。
5. **任务保留：** 成功任务保留 7 天，死信保留 30 天作为首版建议默认值；保留期可配置，清理按小批次执行并避开领取热路径。实现前通过存储增长估算和清理测试校准默认值。
6. **取消语义：** MVP 只允许取消尚未领取的任务；运行中任务通过传给 Handler 的 context 协作取消，暂不提供“强制终止并立即释放租约”的 API。
7. **空队列调度：** Worker 使用可配置轮询间隔和有上限的退避；首版先采用简单轮询，基准测试后调参，不引入 Redis 通知或额外调度服务。

## 12.1 开发环境与工程约定（2026-10-11 确认）

1. **Go 版本：** 最低支持 Go 1.24；本机开发使用 go1.27.0 linux/amd64。README、`go.mod` 与 CI 须使用一致版本。
2. **许可证：** MIT。
3. **模块路径：** `github.com/Tidehyc/gorangetask`；已通过本机 `gh auth status` 复核账号 `Tidehyc` 有效（scopes 含 `repo`）。
4. **本地数据库：** 本地开发与集成测试统一使用容器化 MySQL 8.0.46（`mysql:8.0` 镜像）；宿主机不安装、不运行 mysql-server，避免污染本机环境。容器由仓库内 `scripts/dev-mysql.sh {up|down|reset|status|shell}` 管理。
5. **容器环境：** 宿主机 Docker 已可用；Testcontainers-Go 可作为集成测试的可选方案，与本地 `scripts/dev-mysql.sh` 二选一。
6. **提交约定：** 以需求为粒度提交；每次开工前先与项目所有者确认。
7. **Handler 超时：** 采用逐任务可覆盖的超时，即 `Task.Timeout` 非零时覆盖 Worker 默认 Handler 超时，零值回退到 Worker 配置；由 `context.Context` deadline 实现。
8. **包结构：** 根包 `gorangetask` 只承载公开契约（Task/TaskInfo/Status/Handler/Registry/Client/Tx/配置/错误）；所有 SQL 与 MySQL 方言实现放 `store/`（`store/mysql`）；GORM adapter 隔离在 `adapter/gorm`；migration 用 `embed` 打包。目录随对应阶段创建，不提交空占位目录。详见技术设计 §7。

## 13. 参考实现与借鉴范围

这些项目用于参考设计，不复制其实现代码：

- [River](https://github.com/riverqueue/river)：重点参考事务内入队、任务类型与 Worker 注册，以及数据库任务队列的产品边界。River 主线是 PostgreSQL，本项目建议先以 MySQL 为目标。
- [Asynq](https://github.com/hibiken/asynq)：参考任务重试、超时、Worker 并发与崩溃恢复的用户体验；它以 Redis 为后端，架构目标不同。
- [pgq](https://github.com/btubbs/pgq)：参考精简的 SQL 队列、延迟任务与重试配置；它基于 PostgreSQL。
- [MySQL 8.0 锁定读文档](https://dev.mysql.com/doc/refman/8.0/en/innodb-locking-reads.html)：核实 MySQL 的 `FOR UPDATE`、`SKIP LOCKED` 语义与使用边界。

GoRangeTask 的实现应以本需求文档明确的 MySQL 事务、租约和 fencing 要求为准，不把某个参考项目的数据库语义直接照搬。
