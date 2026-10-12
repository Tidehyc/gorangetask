# GoRangeTask 技术设计

> 项目名：GoRangeTask（仓库名：`gorangetask`）。
> Go module 路径：`github.com/Tidehyc/gorangetask`（已由本机 `gh auth status` 复核：账号 `Tidehyc`，token 有效，scopes 含 `repo`）。
> 版本：v0.1
> 状态：实现前技术基线；SQL 与默认参数需在 MySQL 集成测试中验证。
> 范围：独立 Go 任务队列 MVP。GCFeed 仅作为未来可选集成。
> 上位需求：[GoRangeTask-需求文档.md](./GoRangeTask-需求文档.md)

## 1. 设计目标与保证边界

GoRangeTask 是一个可嵌入 Go 服务的 MySQL 持久化任务队列。业务代码可在自己的数据库事务中写入业务数据和 GoRangeTask 任务行。Worker 从任务表并发领取任务，直接执行已注册的 Go Handler。

首版提供以下保证：

- 同一 MySQL 事务内，业务数据和任务记录一起提交或一起回滚。
- 多个 Worker 可并发领取；一个有效租约期内，任务只有一个有效租约持有者。
- Worker 崩溃或租约到期后，任务可重新尝试。
- 旧租约持有者不能续租、确认成功或确认失败。
- 执行语义为至少一次：Handler 可能重复运行，业务副作用需要幂等。

GoRangeTask 不保证外部副作用恰好一次。`lease_epoch`（租约代次）只保护 GoRangeTask 任务行；对 Handler 写业务表、调用外部 API、写 Redis 等副作用的保护仍由应用实现。

## 2. 首版技术范围

- 数据库：MySQL 8.0+、InnoDB；所有队列 SQL 与 migration 首版只验证 MySQL。本机已装 MySQL 8.0.46（Ubuntu 24.04 包），可直接作为本地开发库。
- SQL 驱动：`database/sql` 接口；具体驱动由宿主应用选择。示例采用 `go-sql-driver/mysql`。
- 核心包：不依赖 GORM。
- GORM：官方可选 adapter 子包，在调用方已经开启的 GORM 事务中插入任务；adapter 不创建或提交业务事务。
- 时间：库写入 UTC 时间；所有到期和租约判断使用数据库 `UTC_TIMESTAMP(6)`。
- 队列 Worker 与业务 Producer 可以同进程或分进程部署，但必须连接同一 MySQL 数据库实例/库，事务入队使用 Producer 的原事务。

未来新增数据库时，为新方言单独实现 migration、领取 SQL、租约 SQL 和并发集成测试；在此之前不标记为支持。

## 3. 核心数据模型

### 3.1 Migration DDL 草案

```sql
CREATE TABLE gotask_tasks (
    id                BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    queue_name        VARCHAR(64) NOT NULL,
    task_type         VARCHAR(128) NOT NULL,
    payload           JSON NOT NULL,
    status            VARCHAR(16) NOT NULL,
    run_at            DATETIME(6) NOT NULL,
    attempt           INT UNSIGNED NOT NULL DEFAULT 0,
    max_attempts      INT UNSIGNED NOT NULL DEFAULT 10,
    lease_owner       VARCHAR(128) NULL,
    lease_until       DATETIME(6) NULL,
    lease_epoch       BIGINT UNSIGNED NOT NULL DEFAULT 0,
    idempotency_key   VARCHAR(191) NULL,
    last_error        VARCHAR(2048) NULL,
    created_at        DATETIME(6) NOT NULL,
    updated_at        DATETIME(6) NOT NULL,
    finished_at       DATETIME(6) NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uq_gotask_queue_idempotency (queue_name, idempotency_key),
    KEY ix_gotask_claim (queue_name, status, run_at, id),
    KEY ix_gotask_lease (status, lease_until, id),
    KEY ix_gotask_cleanup (status, finished_at, id)
) ENGINE=InnoDB;
```

约定：

- `status` 由应用校验为 `pending`、`running`、`succeeded`、`dead`、`canceled`。首版不用 MySQL `ENUM`，减少后续迁移摩擦。
- `run_at` 存储 UTC；立即执行任务写入当前数据库 UTC 时间。延迟重试仍使用 `pending`，将 `run_at` 设为未来时刻。
- `attempt` 在任务被 Worker 成功领取时递增。因此 `attempt=1` 表示第一次 Handler 尝试；数据库死锁导致领取事务回滚时不增加次数。
- `max_attempts` 必须大于 0。默认值 10，可由任务覆盖。
- `lease_epoch` 在每次领取时递增，永不由任务重置；数据库无符号 BIGINT 用尽在实际生命周期内不作为运行假设。
- 可选 `idempotency_key` 在任务记录保留期间由唯一索引约束。清理终态任务后，该 key 可能再次被接受；API 文档须明确该作用域。
- 成功任务默认保留 7 天，死信默认保留 30 天；值可配置。首版 cleanup 批量删除终态任务，不删除 `pending` / `running`。
- 任务 payload 默认上限 256 KiB；不存文件、视频或其他大对象，只存引用 ID。

### 3.2 数据库时间

租约时间与 `run_at` 的比较均在 SQL 中使用 `UTC_TIMESTAMP(6)`。Worker 本机时间只用于本地 sleep/timeout，不参与决定任务是否到期或租约是否有效。新写入的 created/updated 字段可由 Go 使用 UTC 或 SQL 赋值，但所有参与互斥/时序判断的值以数据库为准。

## 4. 状态与尝试次数

| 事件 | 状态迁移 | attempt | 备注 |
|---|---|---:|---|
| 创建立即/延迟任务 | → `pending` | 0 | `run_at` 控制何时可领取 |
| Worker 领取 | `pending` → `running` | +1 | 同时设置 owner、deadline、epoch+1 |
| Handler 成功且租约有效 | `running` → `succeeded` | 不变 | 设置 `finished_at`，清空 lease |
| 可重试错误，尚有次数 | `running` → `pending` | 不变 | 写下一次 `run_at` 和错误摘要，清空 lease |
| 永久错误或次数耗尽 | `running` → `dead` | 不变 | 设置 `finished_at`，记录错误，清空 lease |
| 租约过期且次数未耗尽 | `running` → `pending` | 不变 | Reaper 回收，立即可重领并在下次领取时 +1 |
| 租约过期且次数已耗尽 | `running` → `dead` | 不变 | 错误摘要标记租约到期/Worker 无确认 |
| 取消未领取任务 | `pending` → `canceled` | 不变 | 设置 `finished_at`；已到期但尚未领取仍可取消 |

一次尝试从成功领取开始，到成功确认、错误确认或租约到期结束。若 Handler 副作用完成但 Worker 在成功确认前崩溃，任务可能重复执行；Handler 必须幂等。

## 5. SQL 操作与事务边界

以下 SQL 是实现基线，需在 MySQL 8.0+ 集成测试中验证，并根据驱动占位符调整。

### 5.1 普通入队

`Enqueue` 由 GoRangeTask 自行开启短事务或单条 INSERT；失败则不返回任务 ID。`EnqueueTx` 只在传入的事务执行器上 INSERT，不提交、不回滚、不另取连接。

```sql
INSERT INTO gotask_tasks (
    queue_name, task_type, payload, status, run_at,
    attempt, max_attempts, lease_epoch, idempotency_key,
    created_at, updated_at
) VALUES (?, ?, CAST(? AS JSON), 'pending', ?, 0, ?, 0, ?, UTC_TIMESTAMP(6), UTC_TIMESTAMP(6));
```

幂等冲突首版返回稳定的 `ErrDuplicateTask`，不静默覆盖旧任务，也不自动返回旧 ID。业务方需要旧 ID 时可根据 `(queue_name, idempotency_key)` 查询。

### 5.2 原子领取

每次领取默认 1 条，可配置上限。一个 Worker claim 周期按以下顺序运行：

1. 开启事务。
2. 用锁定读选出到期 `pending` 行。
3. 对选中 ID 设置 `running`、租约 owner/deadline，并递增 attempt 与 epoch。
4. 事务提交。
5. 再调用 Handler；绝不在事务或行锁持有期间执行 Handler。

```sql
SELECT id
FROM gotask_tasks
WHERE queue_name = ?
  AND status = 'pending'
  AND run_at <= UTC_TIMESTAMP(6)
ORDER BY run_at, id
LIMIT ?
FOR UPDATE SKIP LOCKED;
```

对选中的每个 ID，在同一事务内执行：

```sql
UPDATE gotask_tasks
SET status = 'running',
    lease_owner = ?,
    lease_until = TIMESTAMPADD(MICROSECOND, ?, UTC_TIMESTAMP(6)),
    lease_epoch = lease_epoch + 1,
    attempt = attempt + 1,
    updated_at = UTC_TIMESTAMP(6)
WHERE id = ? AND status = 'pending';
```

每行必须恰好更新 1 行，否则回滚整个 claim 事务并记录异常。事务提交后按 ID 读取任务数据供 Worker 执行。Claim 使用的索引由 `EXPLAIN` 验证；`SKIP LOCKED` 仅用于队列领取，不能作为一般一致性查询。锁定读可能跳过被锁记录，因此该查询不提供严格 FIFO 或多队列公平性。

### 5.3 租约续期

heartbeat 周期建议为 lease duration 的 1/3；默认 lease duration 30 秒、heartbeat 10 秒。调用方可配置，校验 heartbeat 小于 lease duration。

```sql
UPDATE gotask_tasks
SET lease_until = TIMESTAMPADD(MICROSECOND, ?, UTC_TIMESTAMP(6)),
    updated_at = UTC_TIMESTAMP(6)
WHERE id = ?
  AND status = 'running'
  AND lease_owner = ?
  AND lease_epoch = ?
  AND lease_until > UTC_TIMESTAMP(6);
```

RowsAffected 为 0 表示租约已丢失或到期。Worker 必须立即 cancel Handler context，禁止该次执行进行状态确认。context cancel 是协作式通知，不会回滚已经发生的业务副作用。

### 5.4 成功确认

```sql
UPDATE gotask_tasks
SET status = 'succeeded', finished_at = UTC_TIMESTAMP(6),
    lease_owner = NULL, lease_until = NULL,
    last_error = NULL, updated_at = UTC_TIMESTAMP(6)
WHERE id = ? AND status = 'running'
  AND lease_owner = ? AND lease_epoch = ?
  AND lease_until > UTC_TIMESTAMP(6);
```

RowsAffected 必须为 1。为 0 时记 `lease_lost_writeback_total`，不尝试用无条件更新补救。

### 5.5 失败确认与重试

对可重试错误，重试延迟按 full jitter 指数退避计算：`delay = random(0, min(max_delay, base_delay × 2^(attempt-1)))`。默认 base 1 秒、最大 1 小时。延迟由 Worker 计算成 duration，SQL 用数据库时钟设置 `run_at`。

可重试且 `attempt < max_attempts`：

```sql
UPDATE gotask_tasks
SET status = 'pending', run_at = TIMESTAMPADD(MICROSECOND, ?, UTC_TIMESTAMP(6)),
    lease_owner = NULL, lease_until = NULL,
    last_error = ?, updated_at = UTC_TIMESTAMP(6)
WHERE id = ? AND status = 'running'
  AND lease_owner = ? AND lease_epoch = ?
  AND lease_until > UTC_TIMESTAMP(6);
```

永久错误，或 `attempt >= max_attempts`：更新为 `dead`，写 `finished_at` 和 `last_error`，并清空 lease。上述更新同样必须匹配 owner、epoch 且 lease 未过期，RowsAffected 为 0 视为租约丢失。

错误摘要截断至列长度，不保存完整堆栈或敏感 payload。Worker panic 由单任务边界 recover，转成可重试错误；进程级不可恢复错误不应被吞掉。

### 5.6 过期租约回收

Reaper 按小批次处理 `running AND lease_until <= UTC_TIMESTAMP(6)`：事务内 `SELECT id, attempt, max_attempts ... FOR UPDATE SKIP LOCKED`，逐行条件更新。

- `attempt < max_attempts`：回到 `pending`，设置 `run_at = UTC_TIMESTAMP(6)`，清空 lease，保留错误说明“lease expired”。
- `attempt >= max_attempts`：改为 `dead`，设置 `finished_at`，清空 lease，错误说明“lease expired at final attempt”。

更新 WHERE 条件同时带上 `status='running'` 和被选中的 `lease_until` 值，且检查 RowsAffected。reaper 只在短事务中更新行；它不执行 Handler。若另一 Worker 已锁定行，`SKIP LOCKED` 使本批次跳过，后续轮次再处理。

### 5.7 清理终态任务

Cleanup 以小批次选取 `succeeded` 且 `finished_at < UTC_TIMESTAMP(6) - 7 DAY`，以及 `dead` 且超过 30 天的行；按主键删除，避免长事务。保留期可配置。`canceled` 默认按成功任务保留期处理。任务被删除后，其幂等键不再占用唯一键。

### 5.8 状态查询与取消

状态查询只读需要的列，不 `SELECT *`；`lease_owner` / `lease_until` / `lease_epoch` 属于 Worker 内部状态，不进 `TaskInfo`，免得以后加列时无意中暴露内部字段。返回的时间字段一律换算为 UTC，不依赖调用方 DSN 里的 `loc` 设置。

```sql
SELECT queue_name, task_type, payload, status, run_at,
       attempt, max_attempts, idempotency_key, last_error,
       created_at, updated_at, finished_at
FROM gotask_tasks WHERE id = ?;
```

取消用一条条件更新，而不是“先查后改”：

```sql
UPDATE gotask_tasks
   SET status = 'canceled',
       finished_at = UTC_TIMESTAMP(6),
       updated_at  = UTC_TIMESTAMP(6)
 WHERE id = ? AND status = 'pending';
```

- `WHERE status = 'pending'` 让判断与写入在同一条语句里完成，不会出现“查到是 pending、改之前被 Worker 领走”的漏判。
- **不校验 `run_at`**：已到期但尚未被领取的任务仍是 `pending`，按设计仍可取消（§3 状态迁移表）。
- 不触碰租约字段——`pending` 任务的租约字段本就是 NULL。
- 只有影响 0 行时才再读一次 `SELECT status`，用来区分“任务不存在”（`ErrNotFound`）和“状态不是 pending”（`ErrNotPending`）。这次补读不包事务：它只是给调用方判断原因用的快照，不承诺更强的一致性。
- 重复取消同一个任务返回 `ErrNotPending`：`canceled` 是终态，同样不满足“只允许取消 pending”。

## 6. Go API 草案

```go
type Handler func(ctx context.Context, payload json.RawMessage) error

type Task struct {
    Queue          string
    Type           string
    Payload        json.RawMessage
    RunAt          time.Time     // 零值表示立即运行；非零值由应用传入 UTC instant
    MaxAttempts    int           // 零值使用默认值
    Timeout        time.Duration // 零值使用 Worker 默认 Handler 超时
    IdempotencyKey string
}

type TaskID uint64

type Tx interface {
    ExecContext(context.Context, string, ...any) (sql.Result, error)
}

type Client interface {
    Enqueue(ctx context.Context, task Task) (TaskID, error)
    EnqueueTx(ctx context.Context, tx Tx, task Task) (TaskID, error)
    Get(ctx context.Context, id TaskID) (TaskInfo, error)
    Cancel(ctx context.Context, id TaskID) error
}

// ClientConfig 是入队侧配置（与 Worker 的 Config 分开）。零值合法：
// 缺省字段由 WithDefaults 填充，由 Validate 校验。
type ClientConfig struct {
    DefaultQueue       string // 任务未指定 Queue 时使用；默认 "default"
    DefaultMaxAttempts int    // 任务未指定 MaxAttempts 时使用；默认 10
    MaxPayloadSize     int    // 单任务 payload 字节上限；默认 256 KiB
}
```

`Task.Timeout` 为逐任务超时（对应需求文档 §7 草案与 §4.2「Handler 执行超时可配置」）：零值表示使用 Worker 级默认 Handler 超时；非零值覆盖本次执行的 Handler 超时。该超时通过传给 Handler 的 `context.Context` 的 deadline 实现，与租约、shutdown 的取消是同一套协作式取消机制。

API 约定：

- `Enqueue` / `EnqueueTx` 共用同一套校验与同一条 INSERT，差别只在“在谁的连接上执行”。校验（必填字段、JSON 合法性、payload 上限、列宽）全部在落库前完成，失败返回可判断的 `ErrInvalidTask` / `ErrInvalidPayload`。
- `Enqueue` 不额外开启事务：单条 INSERT 本身即原子。立即执行任务的 `run_at` 取 `UTC_TIMESTAMP(6)`，延迟任务由调用方传入 UTC 时刻。
- 幂等冲突**依赖唯一索引**而不是“先查后插”：后者在并发下必然漏判。空幂等键写入 NULL 而非空串——MySQL 唯一索引不约束 NULL，但会把空串当成真实取值，从而把“没有幂等键”误判成“幂等键重复”。
- `ErrDuplicateTask` 不返回既有任务 ID：调用方若需要旧任务，应自行按幂等键查询（需求文档未要求该能力，故不作为默认行为）。
- `EnqueueTx` 的 `Tx` 只要求执行 INSERT 所需的方法，不暴露 Commit/Rollback，避免库误操作调用方事务。
- GORM adapter 暴露独立入口（具体包名待仓库结构确认），接收 `*gorm.DB` 事务并执行同一 INSERT；adapter 通过传入对象执行，不开启新事务。
- `Get` 返回任务当前状态的快照，不含 Worker 内部字段（`lease_owner` / `lease_until` / `lease_epoch`）；时间字段一律 UTC；任务不存在返回 `ErrNotFound`。
- `Cancel` 只成功取消当前 `pending` 任务。任务不存在返回 `ErrNotFound`；已领取（`running`）或已处于终态（`succeeded` / `dead` / `canceled`）返回 `ErrNotPending`，错误信息里带上实际状态，便于从日志直接看出原因。重复取消同一个任务因此也返回 `ErrNotPending`（`canceled` 是终态）。
- 状态判断不提供更强的“结果”抽象：`Cancel` 只返回错误，不回传被取消任务的快照。调用方需要详情时再调 `Get`。
- `Handler` 注册时由 `task_type` 映射；同一个 type 在同一个 Worker 注册表中不能重复注册。
- Worker 启动时校验所监听队列和 Handler 注册；未知 task type 的任务进入永久失败路径，避免快速无限重试。

Worker 默认配置：并发 4、poll interval 250 ms（空闲时指数退避至 2 秒并加抖动）、batch size 1、lease duration 30 秒、heartbeat 10 秒、shutdown timeout 30 秒。所有配置可覆盖；这些默认值是起步值，压测后再调整。

## 7. 项目包结构

```text
.
├── doc.go                # 包级文档
├── client.go             # Client/Tx 接口与构造器
├── task.go               # Task、TaskInfo、TaskID、Status
├── errors.go             # 公开可判断错误
├── handler.go            # Handler 类型与 Registry 注册表
├── config.go             # Worker 与 Client 配置、默认值与校验
├── worker.go             # Worker 生命周期、并发和 shutdown
├── store/                # MySQL store：Client（入队/查询）、claim/lease/retry/reaper SQL
│   └── mysql/            # 方言实现（package mysql，导入路径 .../store/mysql）
├── migrations/mysql/     # 版本化 DDL + runner（package mysqlmigrations，独立包）
├── adapter/gorm/         # 可选官方 GORM adapter
├── example/basic/        # 业务事务 + EnqueueTx + Handler 示例
├── integration/          # 真 MySQL 测试
└── docs/                 # API、语义、运维与基准说明
```

结构决策（2026-10-11 确认）：

- **根包 `gorangetask` 保留公开契约**：`Task`/`TaskInfo`/`Status`/`Handler`/`Registry`/`Client`/`Tx`/配置/错误。使用者的导入面只有根包，语义一目了然。
- **`store/` 承载 SQL 与方言**：领取、续租、成功/失败确认、回收、清理等所有语句实现放在 `store/`（MySQL 方言在 `store/mysql`）。根包通过接口依赖 store，不直接写 SQL。
- **`migrations/mysql` 保持独立包，不并入 `store/mysql`**（2026-10-12 确认）：迁移 DDL 用 `//go:embed` 打包，而 `embed` 不能跨目录向上取文件，若并入 store 就必须把 .sql 搬进 `store/mysql` 目录下。迁移是“建表”这件事的完整闭环（DDL + 版本管理 + runner + 注释剥离），自成一体、已有独立测试；store 只读表、不建表，两者职责不重叠。因此调用方分两步用：先 `mysqlmigrations.Migrate`，再 `mysqlstore.New`。
- **可选依赖隔离在子包**：GORM adapter 放 `adapter/gorm`，只有使用 GORM 的应用才引入；`migrations` 通过 `embed` 打包，避免运行时依赖外部文件路径。
- **首版不必一次性建满**：根包在 API 类型阶段已经成形；`config.go`/`worker.go`/`store/` 随对应实现阶段加入，`example`/`integration`/`adapter` 在验证与适配阶段加入。目录只在有真实内容时创建，不提交空占位目录。

## 8. 验证计划

### 8.1 MySQL 集成正确性

使用 Testcontainers-Go 启动真实 MySQL 8.0+，验证：

1. 业务行与任务行同事务提交/回滚。
2. N 个 goroutine/Worker 并发领取 M 条任务，同一时刻每条任务至多有一个有效 owner+epoch。
3. attempt 只在成功 claim commit 后增加。
4. lease 延期、过期回收、最终次数耗尽进入 dead。
5. 旧 epoch 的 renew/succeed/fail 更新 RowsAffected=0。
6. 同一队列幂等键并发插入只有一条成功，其他返回 `ErrDuplicateTask`。
7. cancel 只作用于 pending；cleanup 按保留期限删除终态任务。

### 8.2 故障注入

独立示例提供明确的故障开关或测试钩子，在以下时点终止 Worker：

- claim commit 后、Handler 开始前；
- Handler 副作用完成后、成功状态写回前；
- lease 过期并被新 Worker 领取后，让旧 Worker 恢复并尝试写回。

期望结果：前两种情况下任务最终可恢复，副作用可能重复但幂等；最后一种情况下旧 Worker 状态写回被 fencing 条件拒绝。

### 8.3 性能基准

记录 MySQL 版本/资源、连接池、payload 大小、Worker 数、任务耗时和配置，报告：入队 p50/p95/p99、提交到开始延迟、tasks/s、积压排空时间、claim SQL `EXPLAIN`、锁等待、连接池占用和空队列轮询 QPS。吞吐目标先不拍脑袋设定；首轮建立可复现基线，再以功能正确为先优化。

## 9. 实施顺序与完成门槛

1. 创建 Go module、migration 和配置/错误/API 类型。
2. 实现单 Worker 入队、事务入队、claim、Handler 成功/失败确认，跑通 MySQL 示例。
3. 加入多 Worker、租约续期、reaper 和 fencing 测试。
4. 加入 panic/error、退避、死信、取消、graceful shutdown 和 cleanup。
5. 完成集成测试、故障注入、race 检查、基准与 README。
6. 最后提供可选 GORM adapter，并验证同事务提交/回滚；若这不阻塞核心可并行独立完成，但公开前需集成验证。

MVP 完成门槛：真实 MySQL 集成测试通过；事务提交/回滚和并发领取语义有自动化覆盖；至少三种故障注入可复现；README 清楚解释至少一次、幂等、租约与数据库成本；没有把 GCFeed 集成列作前置条件。

## 10. 参考

- MySQL 8.0 官方锁定读文档：[`SELECT ... FOR UPDATE`、`NOWAIT` 与 `SKIP LOCKED`](https://dev.mysql.com/doc/refman/8.0/en/innodb-locking-reads.html)。MySQL 说明 `SKIP LOCKED` 会排除已锁定行，适用于多个 session 访问队列表的场景；由于读取结果可能不完整，它只用于队列领取，不用于一般一致性读取。
- MySQL InnoDB 锁与语句扫描说明：[`Locks Set by Different SQL Statements`](https://dev.mysql.com/doc/refman/8.0/en/innodb-locks-set.html)。实际锁范围取决于扫描的索引记录，因此必须检查查询计划和索引。
