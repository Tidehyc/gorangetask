# GoRangeTask 技术设计

> 项目名：GoRangeTask（仓库名：`gorangetask`）。
> Go module 路径：`github.com/Tidehyc/gorangetask`（已由本机 `gh auth status` 复核：账号 `Tidehyc`，token 有效，scopes 含 `repo`）。
> 文档版本：v0.1
> 状态：设计基线（已落地）。文中 SQL 与默认参数已在 MySQL 8.0.46 集成测试中验证，逐项证据见[进度跟踪](./GoRangeTask-进度跟踪.md)。
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
    timeout_ns        BIGINT UNSIGNED NULL,   -- 0002 追加：逐任务 Handler 超时（纳秒）
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
- `timeout_ns` 为可选的逐任务 Handler 超时，`NULL` 表示未设置（用 Worker 配置的默认值）。单位取纳秒而不是微秒：`time.Duration` 本身就是纳秒计数，按微秒存会把不足 1µs 的超时截断成 0，读取方就会把它误当成"未设置"。该列由 0002 追加在表末——MySQL 8.0.12+ 只在"加为最后一列"时支持 `ALGORITHM=INSTANT`，插在中间会退化成重建整表。

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
    attempt, max_attempts, lease_epoch, idempotency_key, timeout_ns,
    created_at, updated_at
) VALUES (?, ?, CAST(? AS JSON), 'pending', ?, 0, ?, 0, ?, ?, UTC_TIMESTAMP(6), UTC_TIMESTAMP(6));
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

⚠️ 上面这条锁定读**刻意只取 `id`**，目的是让它停在覆盖索引 `ix_gotask_claim (queue_name, status, run_at, id)` 上。**这是承载语义的约束，不是风格偏好**：一旦把 payload 之类的列也取出来，覆盖索引失效，`LIMIT` 就不再限制加锁范围——回收器踩过完全相同的坑，实测数据与固定手段见**附录 A**。

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

**新的 `lease_until` 从数据库当前时间起算，不在旧的 `lease_until` 上累加**：续租表达的是"从我续租的这一刻起，再保证这么长时间"；累加会让租约随续租次数越推越远，一次失败的续租就能把任务锁住远超预期的时间。续租同样要匹配 `status = 'running'`——否则一个已经失去租约的旧 Worker 可以靠续租把自己"续"回有效状态，fencing 就形同虚设；终态任务也不能被续租复活。

### 5.3.1 本次执行的三种取消原因（2026-10-15 定）

Worker 传给 Handler 的 context 会因三种互不相同的原因被取消。Handler 可用 `context.Cause` 分辨自己是被谁叫停的，Worker 则据原因决定"要不要写回"：

| 原因 | `context.Cause` | Worker 的处理 |
|---|---|---|
| 停机（`Run` 的 ctx 被取消） | `context.Canceled` | 停止续租但**不释放租约**；失败不写回，成功仍确认 |
| 逐任务超时（`Task.Timeout`，未设置则用 `HandlerTimeout`） | `context.DeadlineExceeded` | 普通任务失败，走重试/死信 |
| 失去租约（续租影响 0 行） | `ErrLeaseLost` | **任何状态都不写回**，任务保持 `running` 交给回收器 |

**续租失败不等于失租。** 只有 `ErrLeaseLost` 才说明租约没了；连接断开、语句超时这类驱动错误只说明"这次没问成"，租约可能仍然有效，因此只上报并在下一个心跳周期重试，**绝不据此取消 Handler**（需求文档 §9「数据库暂时不可用时记录错误、退避并继续」）。若数据库长时间不可用，租约会自然过期，届时下一次续租才会返回 `ErrLeaseLost`。

顺序上，Worker 必须**先停掉这条任务的租约看护、等它退出，再写回状态**。反过来的话，续租会在任务已经写成终态之后到达，因 `status` 不再是 `running` 而返回 `ErrLeaseLost`，把一个已经正常结束的任务凭空报成"失租"。

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

对可重试错误，重试延迟按 **equal jitter** 指数退避计算：先取上界 `cap = min(max_delay, base_delay × 2^(attempt-1))`，再取 `delay = cap/2 + random(0, cap/2)`。默认 base 1 秒、最大 1 小时。延迟由 Worker 计算成 duration，SQL 用数据库时钟设置 `run_at`。

> **抖动口径偏离说明（2026-10-15）**：本节最初写的是 full jitter `delay = random(0, cap)`，实现时改成了 equal jitter。两条理由：
>
> 1. **下限有意义**。full jitter 允许延迟取到 0，即"立刻可重试"——对已经故障的下游没有任何好处，只是更快吃掉剩余尝试次数；`cap/2` 的下限保证每次重试至少等到与当前退避级别相称的一段时间。
> 2. **下限可断言**。有下界时 `run_at` 的增量能用区间断言卡住；`[0, cap]` 只能断言上界，"退避根本没生效"（延迟恒为 0）这类实现错误检查不出来。
>
> 需求文档 §4 的措辞是"有上限的指数退避并加入随机抖动"，两者都满足，因此按实现口径更正本文档。上界按 2 的幂增长并封顶这一点与原文一致；`cap` 的计算抽成纯函数 `retryBackoffCap` 单独测，避免"溢出成负数等价于没有退避"这类错误藏在抖动之后。

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

recover 的具体口径（2026-10-15 实现时定）：**边界只包住 Handler 调用本身**，本库自己的领取、写回、回收逻辑若 panic 仍然照常崩——那是本库的 bug，不该被伪装成"某条任务失败"藏起来。转出的错误形如 `handler panicked: <值>`，并附一段**按行数截断**的堆栈（丢掉只说明"现在在跑"的首行 `goroutine N [running]:`，取前若干行，超出以 `...` 收尾）：堆栈不写全，但保住"是谁 panic 的"这部分最有用的信息。**不打永久标记**——需求只说"视为一次失败"，而 panic 未必是确定性的，去留交给 `max_attempts`。**局限**：只能捕获同一个 goroutine 里的 panic；Handler 若在自己起的子协程里 panic，Go 运行时会直接终止进程，这不是库能拦的。

### 5.6 过期租约回收

Reaper 按小批次处理 `running AND lease_until <= UTC_TIMESTAMP(6)`：事务内 `SELECT id, attempt, max_attempts ... FOR UPDATE SKIP LOCKED`，逐行条件更新。

- `attempt < max_attempts`：回到 `pending`，设置 `run_at = UTC_TIMESTAMP(6)`，清空 lease，保留错误说明“lease expired”。
- `attempt >= max_attempts`：改为 `dead`，设置 `finished_at`，清空 lease，错误说明“lease expired at final attempt”。

更新 WHERE 条件同时带上 `status='running'` 和被选中的 `lease_until` 值，且检查 RowsAffected。reaper 只在短事务中更新行；它不执行 Handler。若另一 Worker 已锁定行，`SKIP LOCKED` 使本批次跳过，后续轮次再处理。

实现注记（2026-10-15）：

- **加锁读必须显式指定索引**。回收要连带取 `attempt` / `max_attempts`，这两列不在 `ix_gotask_lease` 里，因此它不是覆盖索引；实测优化器会改选全表扫描 + filesort，而 `FOR UPDATE` 在扫描过程中就把**整个积压**锁住——`LIMIT` 不再限制加锁范围，`SKIP LOCKED` 也失去意义（12 条过期任务、4 个回收器并发，只有 1 个能推进，合计只回收 3 条）。所以这条读写成 `FORCE INDEX (ix_gotask_lease)`。对照：领取的加锁读只取 `id`，对 `ix_gotask_claim` 天然覆盖，无需提示。**完整的事故经过、`EXPLAIN` 与 `data_locks` 实测对照、备选方案取舍见附录 A。**
- **不动 `attempt` / `lease_epoch`**：这次领取已经用掉一次机会，回收只是给下一次让路；`epoch` 属于“领取”这个动作。
- **守卫用读到的 `lease_until` 原值，而不是 owner/epoch**：回收器不是租约持有者，没有凭据可比。守卫不成立（影响 0 行）只跳过该行，**不返回 `ErrLeaseLost`**——那是持有者的语义，借用它会让“集群里有别的回收器在干活”看起来像故障。`lease_until` 用 `DATE_FORMAT(..., '%Y-%m-%d %H:%i:%s.%f')` 取成字符串原样带回去，避开驱动按 DSN 的 `loc` 重新格式化时间。

### 5.7 清理终态任务

Cleanup 以小批次选取 `succeeded` 且 `finished_at < UTC_TIMESTAMP(6) - 7 DAY`，以及 `dead` 且超过 30 天的行；按主键删除，避免长事务。保留期可配置。`canceled` 默认按成功任务保留期处理。任务被删除后，其幂等键不再占用唯一键。

**实现口径（2026-10-15 落地时补齐）**

- **一次调用只清一个终态**（`CleanupSpec{Status, Retention, Limit}`），保留期随状态一起传入，而不是写死在 SQL 里。这样做不是为了接口好看：`status` 必须是**等值**谓词，索引 `ix_gotask_cleanup (status, finished_at, id)` 的等值前缀才能让后两列继续有序，`ORDER BY finished_at, id` 与 `LIMIT` 才吃得上索引顺序（没有 filesort）。一旦写成 `status IN ('succeeded','canceled')`，各段之间无法保序，优化器退化为范围扫描 + filesort，`FOR UPDATE` 的加锁范围就不再受 `LIMIT` 约束——这正是附录 A 记的那类事故。
- **这条加锁读只 `SELECT id`**，三列全在 `ix_gotask_cleanup` 里，是天然的覆盖索引，因此**不加 `FORCE INDEX`**。这是它与 §5.6 回收器那条读的关键差别：回收器要连带取 `attempt` / `max_attempts`，覆盖索引不成立，只能显式把计划钉在 lease 索引上。两条读都各有一条 `EXPLAIN` 回归用例。
- 边界用 `finished_at <= UTC_TIMESTAMP(6) - Retention`（原稿写 `<`，实现在微秒精度上用 `<=`，二者只在恰好等于边界的那个微秒上有差异）。写成 `<=` 顺带排除了 `finished_at IS NULL` 的行（NULL 与任何值比较都不为真）——终态行不该出现 NULL，但兜底方向刻意取"宁可少删"。
- 删除按主键（先选 id 再 `DELETE ... WHERE id IN (...)`），一个短事务一批；返回的是 `RowsAffected`（**真正删了几行**，不是选出了几行），调用方据此判断"是否删满、可能还有剩余"，不需要额外的 `COUNT`。
- **一轮里每个终态最多跑 10 批**（`cleanupMaxBatches`）。一轮只删一批的话，"每小时删 1000 行"这种配置在每天新增几万行时永远追不上，积压单调增长——而那正是这个功能存在的理由；上限则保证一轮有确定的时长，不会把 Worker 一直占住。
- **单个终态失败不影响其余两个**：三种保留期彼此独立，`succeeded` 的一次抖动没有理由顺带把 `dead` 的清理也停掉。
- 运行间隔 `CleanupInterval`（默认 1h）与回收器同一套约定：**负值关闭清理器**，0 由 `WithDefaults` 填成默认值、`Validate` 拒绝。
- 入参校验（状态必须终态、保留期 > 0、批大小 ≥ 1）**先于任何读与写**：非终态校验是这条语句唯一的防误删屏障，一旦放行 `pending` / `running`，被删掉的任务既不会被执行也不会留下痕迹。

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

> **已落库（2026-10-13）**：迁移 0002 增加了 `timeout_ns` 列，`Task.Timeout` 入队时持久化、`ClaimedTask` 领取时还原（`NULL` → 0 → 用 Worker 默认值），逐任务超时已实际生效。早先"没有对应列、只使用 Worker 默认超时"的缺口已消除。

API 约定：

- `Enqueue` / `EnqueueTx` 共用同一套校验与同一条 INSERT，差别只在“在谁的连接上执行”。校验（必填字段、JSON 合法性、payload 上限、列宽）全部在落库前完成，失败返回可判断的 `ErrInvalidTask` / `ErrInvalidPayload`。
- `Enqueue` 不额外开启事务：单条 INSERT 本身即原子。立即执行任务的 `run_at` 取 `UTC_TIMESTAMP(6)`，延迟任务由调用方传入 UTC 时刻。
- 幂等冲突**依赖唯一索引**而不是“先查后插”：后者在并发下必然漏判。空幂等键写入 NULL 而非空串——MySQL 唯一索引不约束 NULL，但会把空串当成真实取值，从而把“没有幂等键”误判成“幂等键重复”。
- `ErrDuplicateTask` 不返回既有任务 ID：调用方若需要旧任务，应自行按幂等键查询（需求文档未要求该能力，故不作为默认行为）。
- `EnqueueTx` 的 `Tx` 只要求执行 INSERT 所需的方法，不暴露 Commit/Rollback，避免库误操作调用方事务。
- GORM adapter 是可选子包 `adapter/gorm`（包名 `gormadapter`），入口为 `(*Adapter).EnqueueTx(ctx, tx *gorm.DB, task)`，执行与 `EnqueueTx` 完全相同的那条 INSERT。它**不能**把 `*gorm.DB` 直接当作 `Tx`——GORM 没有 `ExecContext`，且它的 `Exec` 会丢掉 `sql.Result`，拿不到新插入行的 ID。实现口径见 §6.2。
- `Get` 返回任务当前状态的快照，不含 Worker 内部字段（`lease_owner` / `lease_until` / `lease_epoch`）；时间字段一律 UTC；任务不存在返回 `ErrNotFound`。
- `Cancel` 只成功取消当前 `pending` 任务。任务不存在返回 `ErrNotFound`；已领取（`running`）或已处于终态（`succeeded` / `dead` / `canceled`）返回 `ErrNotPending`，错误信息里带上实际状态，便于从日志直接看出原因。重复取消同一个任务因此也返回 `ErrNotPending`（`canceled` 是终态）。
- 状态判断不提供更强的“结果”抽象：`Cancel` 只返回错误，不回传被取消任务的快照。调用方需要详情时再调 `Get`。
- `Handler` 注册时由 `task_type` 映射；同一个 type 在同一个 Worker 注册表中不能重复注册。
- Worker 启动时校验所监听队列和 Handler 注册；未知 task type 的任务进入永久失败路径，避免快速无限重试。

Worker 默认配置：并发 4、poll interval 250 ms（空闲时指数退避至 2 秒并加抖动）、batch size 1、lease duration 30 秒、heartbeat 10 秒、shutdown timeout 30 秒。所有配置可覆盖；这些默认值是起步值，压测后再调整。

### 6.1 可观测性（2026-10-16 落地）

两件事刻意分开：**日志面向人**（带 task ID 与错误原文，用来追溯"这一条任务发生了什么"），**指标面向聚合**（不带任何高基数维度）。把它们混成一个接口，是监控系统基数爆掉的常见开端。

日志是 `Config.Logger`：

```go
type Level int // LevelDebug / LevelInfo / LevelWarn / LevelError

type Logger interface {
	Log(ctx context.Context, level Level, msg string, kv ...any)
}
```

- 只有一个方法、一条消息加一串键值对，**不绑定任何日志库**（接 `log/slog` 是十几行适配）。
- `nil` 是合法且默认的取值——要不要日志、往哪写是部署决定，不是配置错误。因此 `WithDefaults` 不填它、`Validate` 也不校验它。
- 字段名是稳定契约，与需求 §8 对齐：`task_id` / `task_type` / `queue` / `attempt` / `max_attempts` / `lease_epoch` / `worker_id`，另有 `duration` / `delay` / `error`。**payload 永不写入日志**（有一条用哨兵值守的用例）。
- 内部缝隙 `onError` 保留为**测试用观察点**（12 处既有用例依赖它），不是公开出口。

指标是 `Config.Metrics`（Worker 侧）与 `ClientConfig.Metrics`（入队与取消）：

```go
type Metrics interface {
	TaskEnqueued(queue string)
	TaskCanceled()
	TaskClaimed(queue, taskType string)
	TaskFinished(queue, taskType string, outcome Outcome, d time.Duration)
	TaskWaited(queue, taskType string, d time.Duration)
	LeaseLost(queue, taskType string)
	HandlerPanicked(queue, taskType string)
	HandlerTimedOut(queue, taskType string)
	TasksReaped(toPending, toDead int)
	TasksCleaned(status Status, deleted int64)
	ClaimLatency(d time.Duration)
	ConcurrencyInUse(n int)
}
```

- **具名事件，不是通用的 `Counter(name, labels)` 原语**：指标名与标签集合由库固定，接入方无法（也无法不小心）把 task ID 这类高基数维度塞进标签——需求 §8 明确禁止这件事，而通用原语恰好把犯错的机会留给每一个使用方。接 Prometheus 时嵌入 `NopMetrics`、只覆盖关心的事件即可。
- `nil` 在构造期（`NewWorker` / `mysqlstore.New`）被替换成 `NopMetrics`：打点散布在库内部十来处，让每个调用点各自判空既啰嗦又容易漏，而漏一处就是 nil 解引用 panic。
- **两条需求条目刻意不做**：按队列与状态的积压量（要库主动轮询，改由宿主查 `SELECT queue_name, status, COUNT(*) FROM gotask_tasks GROUP BY 1, 2`）、数据库连接池占用（属于宿主，用 `db.Stats()`）。
- **排队延迟由存储层算好带出来**：`ClaimedTask.Wait` 来自
  `TIMESTAMPDIFF(MICROSECOND, GREATEST(created_at, run_at), UTC_TIMESTAMP(6))`，即"从最早可被领取到实际被领取"。取 `GREATEST` 而不是 `created_at`——延迟任务在 `run_at` 之前本就不该被领取，把它刻意安排的等待算进"排队延迟"会污染这个指标；用数据库时钟而不是 Worker 本地时钟，理由与 §3.2 同。
- `TaskFinished` 只在写回**真的落地之后**记：被 fencing 拒绝时任务并没有成功、也没有进死信，记了会让观测数据与库里的状态互相矛盾。回收与清理同理，只在真有动作时上报——它们每轮都在跑，零值也上报会把"确实捡回了任务"那一次埋在噪音里。
- **已知局限**：`TaskEnqueued` 在 `EnqueueTx` 上也会计数，**包括事务最终回滚的那一次**——库无从知道调用方的事务是否提交（`Tx` 刻意不暴露 Commit）。这条写进了接口注释，也在集成测试里做成显式断言，而不是含糊带过。

### 6.2 GORM adapter 的执行口径（2026-10-16 落地）

`*gorm.DB` 满足不了 `Tx`，两条都是读 gorm v1.31.2 源码确认的：

1. 它**没有** `ExecContext`——执行入口是 `Exec(sql string, values ...interface{}) *DB`，返回的是链式句柄，错误与结果都藏在会话状态里。
2. 它的 `Exec` **丢弃 `sql.Result`**。整个 gorm v1.31.2 里只有 `callbacks/create.go:142` 读过 `LastInsertId`，那是 `Create` 流程内部的事；裸 SQL 走 `Exec` 拿不到新插入行的 ID，而 `EnqueueTx` 必须返回任务 ID。

因此 adapter 不碰 GORM 的执行入口，而是取出 **GORM 正在使用的连接**（gorm 定义的 `ConnPool` 接口）：

```go
tx.Statement.ConnPool
```

`gorm.Open` 之后它是 `*sql.DB`（autocommit）；`db.Begin()` / `db.Transaction` 之后它被替换成 `*sql.Tx`（开启 `PrepareStmt` 时是 gorm 的 `*PreparedStmtTX` 包装）。adapter 把它交给 `store/mysql.EnqueueTx`，于是校验、幂等键冲突识别、时间口径与普通入队**逐字复用**，没有第二份 INSERT。

三条设计要点：

- **编译期断言** `var _ gorangetask.Tx = gormio.ConnPool(nil)`：ConnPool 的 `ExecContext` 与 `Tx` 的签名一致（ctx、查询、变参、`(sql.Result, error)`）。GORM 哪天改了它，这里是**编译失败**，而不是运行期把任务写进错误的连接。
- **拒绝非事务句柄**，判据取"**是不是 `*sql.DB`**"而**不是**"是不是 `*sql.Tx`"：`PrepareStmt` 模式下事务里的连接池是 gorm 自己的 `*PreparedStmtTX`，并不是 `*sql.Tx`，那种正当用法不能被误伤。这是最需要挡的误用——非事务的 `*gorm.DB` 会让任务行走自己的连接，与业务写入不在同一个事务里，而它编译通过、运行也不报错，**只在回滚时暴露**（业务行没了、任务行还在）。
- **包名 `gormadapter` 与目录名 `adapter/gorm` 不同**：使用方几乎总要同时 import `gorm.io/gorm`，包名若也叫 gorm 就得处处起别名。

验证的判别点在**回滚**而不是提交：把 adapter 换成非事务入队后，"提交则两行都可见"的正向用例**照常通过**，只有回滚用例报出 `task rows = 1 after rollback`（见 §8.2）。

## 7. 项目包结构

```text
.
├── doc.go                # 包级文档
├── client.go             # Client/Tx 接口与构造器
├── task.go               # Task、TaskInfo、TaskID、Status
├── errors.go             # 公开可判断错误
├── handler.go            # Handler 类型与 Registry 注册表
├── config.go             # Worker 与 Client 配置、默认值与校验
├── observability.go      # Logger / Level 与 Metrics / Outcome / NopMetrics
├── worker.go             # Worker 生命周期、并发、回收、清理与 shutdown
├── store/                # MySQL store：Client（入队/查询）、claim/lease/retry/reaper/cleanup SQL
│   └── mysql/            # 方言实现（package mysql，导入路径 .../store/mysql）
├── migrations/mysql/     # 版本化 DDL + runner（package mysqlmigrations，独立包）
├── adapter/gorm/         # 可选官方 GORM adapter（package gormadapter）
├── example/basic/        # 业务事务 + EnqueueTx + Handler 示例（含 cmd/demo 入口）
└── docs/                 # 需求、技术设计、迁移机制、进度跟踪与 STAR 说明
```

> **2026-10-16 修正**：上面这份目录树原先还列了一个 `integration/`，但那个目录**从未建过**——真库测试一直与各自包放在同一目录（`store/mysql`、`migrations/mysql`、`adapter/gorm`、`example/basic` 各有自己的 `*_integration_test.go`），用 DSN 环境变量开关、不设就跳过。各包另有独立的派生库名以避免并发跑包时互相删表。同时补上后加的 `observability.go` 与 `cmd/demo`。目录只在有真实内容时创建，这一条仍然守着。

结构决策（2026-10-11 确认）：

- **根包 `gorangetask` 保留公开契约**：`Task`/`TaskInfo`/`Status`/`Handler`/`Registry`/`Client`/`Tx`/配置/错误。使用者的导入面只有根包，语义一目了然。
- **`store/` 承载 SQL 与方言**：领取、续租、成功/失败确认、回收、清理等所有语句实现放在 `store/`（MySQL 方言在 `store/mysql`）。根包通过接口依赖 store，不直接写 SQL。
- **`migrations/mysql` 保持独立包，不并入 `store/mysql`**（2026-10-12 确认）：迁移 DDL 用 `//go:embed` 打包，而 `embed` 不能跨目录向上取文件，若并入 store 就必须把 .sql 搬进 `store/mysql` 目录下。迁移是“建表”这件事的完整闭环（DDL + 版本管理 + runner + 注释剥离），自成一体、已有独立测试；store 只读表、不建表，两者职责不重叠。因此调用方分两步用：先 `mysqlmigrations.Migrate`，再 `mysqlstore.New`。
- **可选依赖隔离在子包**（2026-10-16 补实情）：GORM adapter 放 `adapter/gorm`，核心包不 import 它，因此不使用 GORM 的项目不会把它编译进产物。但它与核心在**同一个 module** 里，所以 `go.mod` 会列出 `gorm.io/gorm` 与 `gorm.io/driver/mysql`——Go 的模块图是全仓库共享的，这一点无法靠"放在子包"绕开（要彻底隔离就得给 adapter 单开一个 module 并单独打 tag，代价与收益不成比例）。它与 §1 的"核心不依赖 GORM"并不矛盾：那条说的是 **import** 层面的隔离。`migrations` 通过 `embed` 打包，避免运行时依赖外部文件路径。
- **首版不必一次性建满**：根包在 API 类型阶段已经成形；`config.go`/`worker.go`/`store/` 随对应实现阶段加入，`example`/`adapter` 在验证与适配阶段加入（原计划里写着 `integration`，实际没建——真库测试与各自包同目录，见上方 2026-10-16 修正）。目录只在有真实内容时创建，不提交空占位目录。

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
8. 每条**加锁读**（领取、回收、以及将来新增的清理）的 `EXPLAIN` 都纳入测试：断言 `key` 是预期索引，并用"一个事务持锁时另一个仍取到不相交的 `limit` 行"复核加锁范围确实受 `LIMIT` 约束。理由见附录 A——加锁范围由执行计划决定，而这在没有断言时是静默失效的。

### 8.2 故障注入

独立示例提供明确的故障开关或测试钩子，在以下时点终止 Worker：

- claim commit 后、Handler 开始前；
- Handler 副作用完成后、成功状态写回前；
- lease 过期并被新 Worker 领取后，让旧 Worker 恢复并尝试写回。

期望结果：前两种情况下任务最终可恢复，副作用可能重复但幂等；最后一种情况下旧 Worker 状态写回被 fencing 条件拒绝。

**实现口径（2026-10-16，示例落地后回填）**：示例 `example/basic` 只提供一个故障开关 `Options.HandlerDelay`——Handler 在写回之前先等这么久，用 `timer + select{<-timer.C, <-ctx.Done()}` 实现（停机时必须能立刻收手，用 `time.Sleep` 会让 Worker 白等满 `ShutdownTimeout`）。三个时点各自的落地方式：

- **claim commit 后、Handler 开始前**：不需要开关。把 `ShutdownTimeout` 设得小于 Handler 耗时即可——停机超时后 `Run` 返回 `ErrShutdownTimeout`，在途任务保持 `running`。已由 `TestWorkerReapsTaskAbandonedByShutdown` 覆盖。
- **副作用完成后、成功写回前**：由"Handler 在副作用之后返回一个普通错误"制造，而不是真的 kill 进程。两者在库侧**完全等价**：库对"没拿到成功写回"只有一条处理路径（按失败重试），它并不区分 Handler 是返回了错误、panic 了还是进程没了。已由 `TestExampleExportSideEffectHappensOnceAcrossAFailedAck` 覆盖（断言业务时间戳在重试前后不变）。
- **旧 Worker 复苏后尝试写回**：已由 `TestRevivedWorkerCannotWriteBackAfterTakeover`（`store/mysql/revival_integration_test.go`）端到端覆盖。做法是起一个**真的子进程**当旧 Worker，让它在 Handler 跑完、写回之前停下并发信号，然后 `SIGSTOP` 冻结整个进程——连续租一起停住，这是"进程没了"唯一无法用开关伪造的部分；等租约自然过期、另一个 Worker 接手并改写 `lease_owner` / `lease_epoch`，再 `SIGCONT` 放行那条陈旧写回：写回按 §6 的 fencing 条件被拒（错误里带 `ErrLeaseLost`），任务行逐字段与解冻前一致，旧 Worker 的日志里也没有成功记录。

刻意**不**加"在某处直接 kill 进程"的钩子：那会把进程控制塞进库或示例的公开面，代价大于收益；上面第一条已经真实地产生了"进程没了"的效果——回收器接手那条链路是真的走了一遍。

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

---

## 附录 A. 实测记录：加锁读的加锁范围由执行计划决定（2026-10-15）

**这是事故记录，不是设计预案。** 上面 §10 的参考链接里早就引用了 MySQL 的说明——"实际锁范围取决于扫描的索引记录，因此必须检查查询计划和索引"——回收器（§5.6）实现时仍然踩了进去。本节把从现象到固定的全过程与实测数字留档，供后续任何新增的加锁读（回收器、清理器、新的领取路径）对照。全部数据来自本机 **MySQL 8.0.46**。

### A.1 现象

回收器与它的并发用例一起跑时：**12 条过期任务、4 个回收器并发，合计只回收 3 条**，库侧终态是 `pending=3 / running=9`，且 `lease_epoch` 全为 1（说明没有任务被重新领取过，不是重复处理）。

第一次误判成用例设计问题：以为 `limit` 取小了，一个回收器把允许的行抢光，其余空手而归。实际不是分配不均——**有查询在明明还有 9 条未锁行的情况下选中了 0 行**。

### A.2 分层定位（每一步只回答一个问题）

1. **进程侧打点**：把回收器内部逐步骤打印（`BEGIN` / `SELECT` 结果 / 每条 `UPDATE` 的影响行数 / `COMMIT`）。结果是 1 个回收器拿到 `[1,2,3]`，另外 3 个的 `SELECT` **返回空**、`COMMIT` 也报 0 行。⇒ 问题在加锁读这一层，不在写回、不在计数、不在事务边界。
2. **SQL 层受控复现**：两个手写事务，tx1 跑完加锁读后**不提交**（锁保留），tx2 再跑同一条读。tx1 → `[1,2,3]`，tx2 → **`[]`**。⇒ 与 goroutine 调度无关，是**确定性**的锁行为（前提已核对：库里确实有 12 条 `running` 且 `lease_until` 已过期）。
3. **锁清单**：`performance_schema.data_locks` 按连接过滤（事务内先 `SELECT CONNECTION_ID()` 取连接号）。tx1 在 `PRIMARY` 上持有**全部 12 行**的 `X,REC_NOT_GAP` 记录锁；tx2 虽然返回 0 行，却也在 `ix_gotask_lease` 上锁了 12 条索引项。⇒ `LIMIT 3` 完全没有约束住加锁范围。
4. **执行计划**：`EXPLAIN` 给出 `type=ALL, key=NULL, Extra=Using where; Using filesort`。⇒ 这是**全表扫描 + 排序**：`FOR UPDATE` 在扫描过程中锁住每一行，`LIMIT` 只在扫描**之后**才生效。

### A.3 根因

回收的加锁读要连带取 `attempt` 与 `max_attempts` 来做分流（§5.6），而 `ix_gotask_lease` 是 `(status, lease_until, id)`——这两列不在索引里，因此**这条读不是覆盖索引**。优化器在"索引区间 + 回表"与"全表扫描 + 排序"之间选了后者，于是：

- 加锁范围 = **扫描过的行**（整个积压），而不是 `LIMIT` 行；
- `SKIP LOCKED` 失效：并发消费者的加锁集合几乎完全重叠，一轮下来只有一个能推进；
- 代价随积压规模增长——**积压越大越容易卡住**，而积压大正是回收器最该起作用的时候。

### A.4 证据对照

12 行过期任务，索引 `ix_gotask_lease (status, lease_until, id)`，`LIMIT 3`：

| 写法 | `EXPLAIN` | 实际加锁条数 |
|---|---|---|
| 未指定索引 | `type=ALL, key=NULL, Extra=Using where; Using filesort` | **13**（`PRIMARY` 上全部 12 行 + 表级 `IX`） |
| 不加提示、只取索引内列（覆盖） | `type=ref, key=ix_gotask_lease, Extra=Using where; Using index` | 7 |
| `FORCE INDEX` + 只取 `id` | `type=range, key=ix_gotask_lease, Extra=Using where; Using index` | 7 |
| **`FORCE INDEX` + 取 4 列（最终采用）** | `type=range, key=ix_gotask_lease, Extra=Using index condition` | **7** |

最终采用的写法，7 条锁的分解（逐条列出确认过）：`ix_gotask_lease` 上 3 条索引项 + `PRIMARY` 上 3 条记录锁 + 1 条表级 `IX`——**正好等于 `LIMIT` 的行数，一行不多**。

行为层只对**修复后的产品 SQL** 复测过三次，结果稳定：

```text
修复前：tx1 → [1 2 3]   tx2 → []        （tx2 返回 0 行）
修复后：tx1 → [1 2 3]   tx2 → [4 5 6]   （3 次全一致）
```

### A.5 对照：领取路径为什么没中招

领取的加锁读（§5.2）**刻意只取 `id`**，对 `ix_gotask_claim (queue_name, status, run_at, id)` 天然覆盖：实测 `key=ix_gotask_claim, Extra=Using where; Using index`，`LIMIT 3` 只锁 7 条。

两条查询的差别不在写法风格，只在**"有没有多取索引外的列"**。同一份设计文档里，一条遵守了覆盖索引的约束，另一条没有——这就是全部原因。

### A.6 修复与取舍

采用 `FORCE INDEX (ix_gotask_lease)`。备选方案与放弃理由：

| 方案 | 取舍 |
|---|---|
| 只取 `id` + 事务内按主键补读 `attempt`/`max_attempts` | 可行，但要多一次往返，并把分流判断拆成两段 |
| 给 `ix_gotask_lease` 补上 `attempt`/`max_attempts` 两列 | 这两列每次领取/回收都会变，挂到二级索引上等于给每次写入加一笔索引维护成本，而收益只是省掉一个提示 |
| **`FORCE INDEX`（采用）** | 最小改动、计划确定；代价是 `ix_gotask_lease` 从此是**硬依赖**（索引被删/改名会报 `Error 1176`），因此要在 SQL 常量上写明"这是必需的，不是优化" |
| 停机/回收时把任务写回 `pending` | **不做**。那是改语义（§5.3.1 已明确"停机不释放租约、失败不写回"），而这里只是把计划钉回原位 |

### A.7 固定手段

1. **计划层 + 行为层两层断言**，合在 `TestReapLockingReadStaysOnLeaseIndex`：
   - 计划层：`EXPLAIN` 的 `key` 必须是 `ix_gotask_lease`（去掉 `FORCE INDEX` 立刻失败）；
   - 行为层：tx1 跑完加锁读**不提交**，tx2 跑同一条读，断言它仍取到 `limit` 行、且与 tx1 **不相交**（去掉 `FORCE INDEX` 立刻返回 0 行）。
   用两个手写事务而不是两个 Worker：要的是**确定性**，不把成败压在 goroutine 调度上。
2. `TestReapIsSafeWithConcurrentReapers` 留作并发层验收（4 个回收器各 3 条、合计 12）。它现在也具备确定性：任一时刻最多 9 行被锁，第 4 个回收器总能拿到 3 行，所以"合计恰好 12"不是靠运气。
3. **证伪检查**：去掉 `FORCE INDEX` → 上面两条用例都失败（分别报 `key=""` 与"合计只回收 3 条"）→ 还原后与备份 `diff` 逐字节一致。没有这一步，"用例通过"什么也不说明。

### A.8 可推广的规则

- **加锁读的加锁范围等于它执行计划扫过的行，与 `LIMIT` 无关。** 想限制加锁范围，就必须限制扫描范围。
- 加锁读**优先只取索引内的列**；一旦不得不取索引外的列，就不再是覆盖索引，**必须显式指定索引**。不能把加锁范围交给优化器——**表越小它越容易选全表扫描，而这段代码要跑在大表上**。
- `EXPLAIN` 里出现 `Using filesort`，基本可以直接判定"计划离开了预期的那条索引"（`ORDER BY` 与索引顺序一致时不该排序）。
- 结论要用两层断言固定（计划层的 `key` + 行为层的并发不相交读取）。只做一层，将来表大小或统计信息一变，就可能"因为错误的原因通过"。
- 新增任何加锁读时，照 §8.1 的做法把它的 `EXPLAIN` 结果纳入集成测试，而不是只在本地 `EXPLAIN` 看一眼。

### A.9 溯源

- 发现与修复：提交 `75c2e04`（回收器实现 + `FORCE INDEX` + 回归用例 `TestReapLockingReadStaysOnLeaseIndex`）。
- 文档回填：提交 `af56a2f`。本附录为 2026-10-15 的独立留档（补充 A.4 的锁分解与 A.6 的备选方案取舍）。
- 复现所需环境：MySQL 8.0.46，12 行过期任务，`LIMIT 3`，两个手写事务（步骤见 A.2 第 2 步）。
- 相关用例：`store/mysql/worker_integration_test.go` 的 `TestReapLockingReadStaysOnLeaseIndex`、`TestReapIsSafeWithConcurrentReapers`。
