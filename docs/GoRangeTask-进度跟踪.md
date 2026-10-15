# GoRangeTask（gorangetask）项目进度跟踪

> 用途：对照需求和技术设计追踪项目完成情况；每项完成后补充提交、测试日志、基准结果或文档链接作为证据。
> 当前状态：构建阶段
> 更新日期：2026-10-15
> 核心依据：[GoRangeTask-需求文档.md](./GoRangeTask-需求文档.md)、[GoRangeTask-技术设计.md](./GoRangeTask-技术设计.md)、[GoRangeTask-STAR.md](./GoRangeTask-STAR.md)

## 项目身份

- 公开项目名：`GoRangeTask`
- GitHub 仓库名：`gorangetask`
- Go module 路径：`github.com/Tidehyc/gorangetask`（已通过本机 `gh auth status` 复核：账号 `Tidehyc` 已登录，token 有效，scopes = `admin:public_key, gist, read:org, repo`）
- MVP 数据库：MySQL 8.0+ / InnoDB
- 本机开发环境：go1.27.0 linux/amd64；宿主机未安装 mysql-server（无 `/var/lib/mysql`、无自启服务，3306 未被占用）；Docker 守护进程已运行（29.7.2）
- 本地数据库：**容器化 MySQL 8.0.46**，容器名 `gotask-mysql`，数据卷 `gotask-mysql-data`，映射 `127.0.0.1:3306`，库 `gotask`，root 密码 `rootpw`，UTC 时区、READ-COMMITTED；由 `scripts/dev-mysql.sh {up|down|reset|status|shell}` 管理
- 项目定位：独立 Go 持久化任务队列；GCFeed 是未来可选集成，不是核心依赖或验收前置条件。

## 状态说明

- `[ ]` 未开始
- `[-]` 进行中
- `[x]` 完成，需在“证据/备注”中附可核验依据
- `[!]` 阻塞或设计需要调整

完成状态不能只根据代码已写来勾选；对应测试或运行证据通过后才标记完成。

## 0. 项目准备

| 状态 | 工作项 | 完成标准 | 证据/备注 |
|---|---|---|---|
| [x] | 确定项目定位 | 独立轮子；GCFeed 仅可选集成 | 需求文档 v0.4 |
| [x] | 确定首版数据库范围 | MySQL 8.0+ / InnoDB；架构留扩展点 | 需求文档 §2、§12 |
| [x] | 明确 GORM 支持方式 | 核心不依赖 GORM，提供可选官方 adapter | 需求文档 §7、技术设计 §2、§6 |
| [x] | 编写实现前技术设计 | DDL、领取/续租/确认/回收 SQL、API 和测试设计齐备 | 技术设计 v0.1 |
| [x] | 确认 module 完整路径可用并重新认证 gh | 写定 `github.com/Tidehyc/gorangetask` | 2026-10-11 本机 `gh auth status` 已确认账号 Tidehyc 已登录且 token 有效（scopes 含 `repo`）；模块路径已定 |
| [x] | 定最低 Go 版本与许可证 | README/go.mod/CI 使用一致版本和许可证 | 2026-10-11 定：最低 Go **1.24**；许可证 **MIT** |
| [x] | 创建仓库并初始化 Go module | 本地 `git init`、README、LICENSE、go.mod、.gitignore 可运行 | 2026-10-11 `git init -b main`；首个提交 `3bb5009`（10 files, +1074）；module 最低 Go 1.24 |
| [x] | 创建远程仓库并首次推送 | GitHub 上 `Tidehyc/gorangetask` 存在且 main 已推送 | 2026-10-11 远程 `Tidehyc/gorangetask`（public，默认分支 main）已建；`main` 推送至 `958d120`，跟踪 origin/main |

## 1. 项目骨架与开发约定

| 状态 | 工作项 | 完成标准 | 证据/备注 |
|---|---|---|---|
| [x] | 初始化模块与包结构 | core、MySQL store、migration、example 结构建立 | 2026-10-11 包结构决策已定（技术设计 §7）：根包只放公开契约，SQL 放 `store/`，GORM 放 `adapter/gorm`，migration 用 embed；目录随阶段创建。根包 `gorangetask` 已建（doc/task/errors/handler/client） |
| [x] | 定义公开错误与 API 类型 | Task、TaskID、TaskInfo、Handler、Client、配置、错误语义有文档 | 2026-10-11 提交 `57be84f`：Task/TaskInfo/TaskID/Status、7 个公开错误、Handler+Registry、Tx/Client 接口；10 个单测通过、`-race` 通过、覆盖率 100% |
| [x] | 定 Handler 超时语义 | 逐任务超时与 Worker 默认超时的优先级明确 | 2026-10-11 定：`Task.Timeout` 非零覆盖、零值回退 Worker 默认；由 ctx deadline 实现（需求 §12.1-7、技术设计 §6） |
| [x] | 定义 Worker 配置类型与校验 | 配置字段齐备且有默认值与合法性校验 | 2026-10-11 提交 `921e230`：`Config` 16 字段 + 全部默认值常量；`WithDefaults`/`Validate`（包裹 `ErrInvalidConfig`）；`Task.Timeout` 落地；单测覆盖 19 类非法配置，覆盖率 100% |
| [x] | 建立本地 MySQL 开发环境 | 一条命令启动 MySQL 8，migration 可应用/回滚 | 2026-10-11 容器化 MySQL 8.0.46（`gotask-mysql`）已跑通；`scripts/dev-mysql.sh up` 启停；宿主机未装 mysql-server，环境不被污染 |
| [x] | 建立 CI 基线 | format、vet、unit test 可在 CI 执行（MySQL integration test 待有集成测试后补） | 2026-10-11 提交 `44f468f`：`.github/workflows/ci.yml`，push/PR 到 main 触发 gofmt→vet→build→test -race；首次运行 `38136967246` **success（29s）**；Go 版本由 go.mod 驱动（CI 实跑 1.24） |
| [x] | CI 接入 MySQL 集成测试 | 集成测试在 CI 真跑；缺 DSN 静默跳过时 CI 必须失败而不是全绿 | 2026-10-12 提交 `a515fbc`：`.github/workflows/` 加 `mysql:8.0` service container（`mysqladmin ping` 健康检查），job 级 env 设 `GOTASK_TEST_MYSQL_DSN`；测试步骤 `-v` + `tee go-test.log`，新增守门步骤——出现任何 `--- SKIP` 即判失败（因为集成测试缺 DSN 时只会静默 skip，会把"没验证"伪装成"通过"）。本地按同一流程复现：漏配 DSN 时 **33 个用例被跳过**（`go test` 自身退出码仍是 0）、守门步骤能抓住；配好 DSN 时 **0 SKIP**、日志留下 `MySQL server version: 8.0.46`、`go test -race -count=1 ./...` 三个包全过。**CI 实跑** `38228795804`（2026-10-12T09:16Z push，运行 58s）**success**：6 个步骤全绿，含 `Initialize containers`（MySQL service 起得来）、`Test with race detector`、`Fail if any test was skipped`、`Show MySQL server version`。其中守门步骤通过即等价于「日志里 0 个 SKIP」，也就是集成测试确实在 CI 跑了，而不是静默跳过。⚠️ 日志正文需要 admin 权限（沙箱 403），因此**服务端版本串与用例数尚未回填**，只记已核实到的部分 |

## 2. 持久化与事务入队

| 状态 | 工作项 | 完成标准 | 证据/备注 |
|---|---|---|---|
| [x] | 确认迁移机制方案 | 自研/第三方取舍与脏状态策略明确 | 2026-10-12 定：**自研**；含脏状态检测、`GET_LOCK` 并发保护、执行器切分多语句、不做 down 迁移、不自动执行。详见《迁移机制》文档 |
| [x] | 编写版本化 migration | 任务表、索引和版本管理完成；重复运行行为明确 | 2026-10-12 提交 `4693518`：`migrations/mysql/`（embed DDL + runner）；含脏状态检测、`GET_LOCK` 单连接并发保护、注释剥离与语句切分；**单元测试 + 6 个真 MySQL 集成测试全过**（幂等、索引、脏状态、失败留痕与人工恢复、并发串行、空库版本查询） |
| [x] | 实现普通 `Enqueue` | 立即/延迟任务能持久化，参数校验和 payload 上限生效 | 2026-10-12 提交 `f560e45`：新增 `store/mysql`（package `mysql`）的 `New`/`Enqueue`。校验全部先于落库（type 必填、payload 非空且是合法 JSON、payload 上限、列宽按**字符数**），失败返回 `ErrInvalidTask`/`ErrInvalidPayload` 而非 MySQL 1406；`run_at` 以固定 UTC 字符串绑定（避免 DSN 的 `loc` 造成时区偏差），零值走 `UTC_TIMESTAMP(6)`；单条 INSERT 不开事务 |
| [x] | 实现 `EnqueueTx` | 调用方事务提交任务可见、回滚无任务；库不接管事务生命周期 | 2026-10-12 提交 `f560e45`：与 `Enqueue` 共用同一条 INSERT 与同一套校验，只换执行器（内部接口同时被 `*sql.DB` 与 `gorangetask.Tx` 满足）。集成测试验证：提交前事务外可见 0 行、提交后 1 行；回滚后业务行与任务行均为 0；事务内可见 1 行；事务内命中幂等键时把 `ErrDuplicateTask` 交给调用方决定，库自身不回滚不重试 |
| [x] | 实现幂等键 | 并发同 key 仅一条任务创建，重复返回 `ErrDuplicateTask` | 2026-10-12 提交 `f560e45`：依赖 `uq_gotask_queue_idempotency` 唯一索引，1062 → `ErrDuplicateTask`，不做「先查后插」。**8 协程并发同键连跑 10 轮**：每轮恰好 1 成功 + 7 个 `ErrDuplicateTask`、库里恰好 1 行，未出现 1213 死锁。空幂等键绑 **NULL 而非空串**（绑空串会让同队列第二条无幂等键任务被误判为重复，有专门集成测试守着） |
| [x] | 实现状态读取和 pending 取消 | 查询结果稳定；只允许取消 pending | 2026-10-12 提交 `5be5957`：`store/mysql` 的 `Get`/`Cancel` 落地，至此**完整实现根包 `Client` 接口**并加上编译期断言 `var _ gorangetask.Client = (*Client)(nil)`。`Get` 只查需要的列（不含 Worker 内部字段 `lease_owner`/`lease_until`/`lease_epoch`）、时间字段一律换算 UTC、无行返回 `ErrNotFound`。`Cancel` 用一条条件更新（`WHERE id=? AND status='pending'`）而非"先查后改"，避免漏判；刻意不校验 `run_at`，所以**已到期但尚未领取仍可取消**；影响 0 行时才补读状态来区分 `ErrNotFound` 与 `ErrNotPending`（错误信息带实际状态，如 `task 42 is running`）；重复取消返回 `ErrNotPending`。新增 14 个真库集成用例逐个覆盖 `running` 与三种终态 |

测试口径（2026-10-12）：`store/mysql` 单测 35 个子用例（校验/归一化/错误分类，不连库）+ **26 个真 MySQL 8.0.46 集成测试**；语句覆盖率 97.0%（未覆盖分支是 `LastInsertId()` 与 `RowsAffected()` 的报错，实际无法构造）。`go test -race -count=1 ./...` 三个包全绿；CI 已配 MySQL service，且出现任何 SKIP 即判失败。

测试口径（2026-10-13）：根包 **27 个 Worker 单测**（内存假 store，不连库；含「Handler 内栅栏」用例证明并发池确有 N 个同时运行、4 个优雅停机用例，以及逐任务超时覆盖/回落两个子用例，根包覆盖率 100%）与 `store/mysql` **26 个 Worker 真库集成用例**（到期/队列/limit/顺序/不重复领取、epoch 与 attempt 递增、claim 走 `ix_gotask_claim` 且 `Using index`、三种写回的正常与 fencing 拒绝、延迟与 `last_error` 截断、三个端到端、**并发领取**三件套：8 领取者抢 40 条、跨会话 `SKIP LOCKED` 跳过被锁行、4 Worker 每条恰好执行一次、**停机后任务留在 running** 的库侧状态断言，以及**领取还原逐任务超时**的往返）。全量 `go test -race -count=1 ./...`：**254 条用例记录（128 顶层 + 126 子用例）、0 FAIL、0 SKIP、无 data race**；`store/mysql` 语句覆盖率 93.1%（未覆盖的是驱动层报错分支，如 `BeginTx`/`Exec`/`RowsAffected` 自身失败，无法在真实驱动上构造）。

测试口径（2026-10-15）：根包新增 **5 个租约用例 + 1 个停机续租守卫用例**，并给 2 个既有用例补上取消原因断言；`store/mysql` 新增 **3 个真库用例（共 8 个子用例）+ 1 个入参校验单测**。全量 `go test -race -count=1 ./...`：**272 条记录（136 顶层 + 136 子用例）、0 FAIL、0 SKIP、无 data race**；三个包全绿，根包语句覆盖率 **100%**、`store/mysql` 93.1%。

> **新断言的证伪检查（2026-10-15 做的 5 条）**：短路失租分支、失租后不取消 Handler、把续租的驱动错误当失租、写回前不等看护退出、停掉续租——五处都确认"故意破坏实现时用例立即失败"，再还原并 `diff` 校验与备份逐字节一致。凡断言依赖某个非显然的实现选择，都照这个流程走一遍。

> 集成测试的库隔离：`go test ./...` 会并发运行不同的包，而集成测试的 setup 是破坏性的（`DROP TABLE` + 重新迁移）。因此 `store/mysql` 使用派生库 `<DSN 库名>_store` 并自动创建，避免与 `migrations/mysql` 的集成测试互相删表而报出与产品无关的假失败（`ErrDirty`、`Error 1050 Table already exists`）。新增集成测试包时应取一个独立后缀。
>
> 测试库口径（2026-10-15 调整）：**不再区分「开发库」与「测试库」，统一用 `gotask` 一个库**。DSN 写成 `.../gotask?...`；`scripts/dev-mysql.sh up` 已经会创建它，CI 由 service container 的 `MYSQL_DATABASE` 预设，两条路径都不需要任何手工步骤。`migrations/mysql` 直接使用该库（它自己不建库，只对表做 DDL），`store/mysql` 使用自动派生的 `gotask_store`。原先的 `gotask_test` 已废弃删除。
>
> ⚠️ 顺带记下这个坑的成因：`gotask_test` 当年只是某次手工建好后才一直留在数据卷 `gotask-mysql-data` 里，卷被删掉后没有任何环节会重建它，于是 `migrations/mysql` 整包以 `Error 1049 Unknown database 'gotask_test'` 失败（2026-10-15 实际踩到）。该报错来自夹具的 `PingContext`、**不是迁移失败**——`sql.Open` 是惰性的，此时一条迁移语句都没跑，所以别把这类报错当成迁移逻辑的问题。

## 3. Worker 执行闭环

| 状态 | 工作项 | 完成标准 | 证据/备注 |
|---|---|---|---|
| [x] | Handler 注册与 payload 解码 | task type 映射、未知类型处理、schema 错误路径明确 | 2026-10-12 提交 `e2c4147`：`Registry.Resolve` 对未注册类型返回带永久标记的 `ErrNoHandler`（task_type 是入队时定死的值，重试不会让它变得可处理 ⇒ 直接进 dead）；`TypedHandler[T]` 把 `json.Unmarshal` 收进库，解码失败返回带永久标记的 `ErrInvalidPayload`（覆盖"无法解码 payload 不能无限快速重试"），nil 函数构造期 panic；`Permanent`/`IsPermanent` 用私有类型打标记，nil 安全、可重复、`errors.Is`/`As` 仍可穿透。根包覆盖率 100% |
| [x] | 单 Worker claim/execute/ack | handler 在 claim 事务提交后运行；成功写回终态 | 2026-10-13 提交 `4abcf9b`：根包新增执行侧契约 `WorkerStore`（`Claim`/`Succeed`/`Retry`/`MarkDead`，根包只依赖接口不写 SQL）与 `Worker`；`Run` 按 `Config.Concurrency` 起等量 goroutine，各自「领取一批 → 逐条执行 → 写回」，空队列时指数退避至 `MaxPollInterval` 并加抖动、领到任务即重置；Handler 的超时用 `cfg.HandlerTimeout` 作为 ctx deadline。**claim 两段式**（store/mysql）：事务内只 `SELECT id ... FOR UPDATE SKIP LOCKED`（只为停在覆盖索引 `ix_gotask_claim` 上——把 payload 一起取出来覆盖索引就没了）并逐条条件更新为 `running`（置 owner/`lease_until`、`lease_epoch+1`、`attempt+1`），**提交之后**才按主键回读整行，因此 Handler 绝不在事务/行锁内运行。三种写回共用 `(id, status='running', owner, epoch, lease_until 未过期)` 四重 fencing，影响 0 行 → `ErrLeaseLost`；`run_at`/`lease_until` 一律用数据库时钟（`TIMESTAMPADD` + `UTC_TIMESTAMP(6)`）。端到端用例里 Handler 用**另一条连接**读库，断言它看到的已是 `running`——直接证明 claim 事务先提交。失败策略本项**刻意最简**（还有次数立即回 pending、次数耗尽 dead），退避与永久错误分类留给 §4 |
| [x] | 多 Worker 并发领取 | MySQL `SKIP LOCKED` 集成测试证明并发无双重有效租约 | 2026-10-13 提交 `454ccac`：**无需改动生产代码**——claim 从设计上就是并发安全的（`FOR UPDATE SKIP LOCKED` + 单条 `WHERE status='pending'` 条件更新），本项的工作是把该性质钉在真库上。三个用例：① **8 个领取者**在同一 barrier 后抢 40 条任务、各用不同 `Limit`（1..5，顺带覆盖批量领取路径），断言两两不重复且零丢失，并**独立复核库侧** `lease_epoch`/`attempt` 全为 1（epoch 每次领取 +1 ⇒ 1 即「只被领取过一次」，这一层不依赖进程侧记录）；实测 8 个 owner 全部分到任务（2/4/6/8/8/2/4/6），确认确实在并发抢。② 另一会话用普通 `FOR UPDATE` 锁住「本该第一个被领走」的行，Claim 必须**跳过它**拿走其余两条且**不阻塞**（<3s），锁释放后那条仍可领取——直接验证 `SKIP LOCKED` 语义。③ 4 个 Worker 实例（各 2 并发）跑 24 条任务，断言每条 Handler **恰好执行 1 次** |
| [x] | Worker graceful shutdown | 停止领取、通知运行 handler cancel、在超时内等待退出 | 2026-10-13 提交 `6fc14d7`：按需求文档 §4.6 实现三步——① **停止领取**（循环在下一轮开头发现 ctx 已取消即退出，不再 Claim）；② **通知在途 Handler**（Handler 拿到的 ctx 派生自 `Run` 的 ctx，取消会传到它手里；这是协作式取消，库无法强杀 Handler）；③ **有界等待**：`Run` 在 ctx 取消后转为等待循环结束，超过 `Config.ShutdownTimeout` 仍未退出就返回新增的 `ErrShutdownTimeout`——此前是裸 `wg.Wait()`，一个不理会 ctx 的 Handler 能把 `Run` 永久卡死。关键取舍是**停机期间不写回失败**：Handler 因停机被取消返回的错误不代表任务失败，写回 pending 会让另一个 Worker 在旧副作用可能仍在途时立刻重跑（§4.6「不提前把租约释放给其他 Worker」），因此这类任务保持 `running`、等租约自然过期后交给回收器；而**已经成功返回**的 Handler 仍然确认——它已明确结束，确认不会造成并行执行，确认时改用 `context.WithoutCancel` + `ShutdownTimeout` 预算的 context，否则这条本来成功的结果会连带被取消写不回去。批内停机时剩余任务不再启动 Handler，整批不写回。测试：根包 4 个用例（取消确实传到 Handler、超时放弃并返回 `ErrShutdownTimeout`、停机后成功仍被确认、批内停机不再启动），内存 store 新增 `honourCtx` 开关模拟真实驱动拒绝已取消的 ctx——**已实测证伪**（把确认改回用已取消的 ctx，该用例立即失败；还原后文件与备份逐字节一致）；真库 1 个用例断言停机后任务仍是 `running`、租约与 `lease_epoch` 未变、`attempt` 未增加、`last_error` 仍为 NULL |
| [x] | 持久化逐任务超时 | `Task.Timeout` 落库，领取后能还原该任务的 Handler 超时 | 2026-10-13 提交 `5837ae6`：迁移 **0002** 给 `gotask_tasks` 加 `timeout_ns BIGINT UNSIGNED NULL`（NULL = 未设置，用 Worker 默认 `HandlerTimeout`）。**单位选纳秒而不是微秒**：`time.Duration` 本身就是纳秒计数，按微秒存会把不足 1µs 的超时截断成 0，而读取方又规定 0 就是"未设置"——一个显式设置过的超时会被静默改写成另一种语义。**列追加在表末、不写 `AFTER`**：MySQL 8.0.12+ 只在"加为最后一列"时支持 `ALGORITHM=INSTANT`，8.0.29 之前插到中间会退化成重建整表；本机 8.0.46 实测对已有数据的表追加后 `information_schema.INNODB_TABLES.TOTAL_ROW_VERSIONS` 由 0 变 1（走行版本化而非重建），存量行读新列为 NULL。链路：`prepare` 负值 → `ErrInvalidTask`、零值 → NULL、非零 → 纳秒；`Get` 写入 `TaskInfo.Timeout`、`Claim` 回填 `ClaimedTask.Timeout`；`Worker.execute` 在 `task.Timeout > 0` 时覆盖 `cfg.HandlerTimeout`，否则回落（仍派生自 `Run` 的 ctx，停机取消照旧生效）。测试：根包 `TestWorkerUsesPerTaskTimeout` 两个子用例（任务超时覆盖默认值 / 零值回落），Handler 内等 `ctx.Done` 回传错误与耗时、不靠墙钟睡眠；真库 `TestEnqueuePersistsAndReturnsTaskTimeout`（库侧直查纳秒数与 **NULL 而非 0**、再走 `Get` 往返）与 `TestClaimRestoresPerTaskTimeout`（领取后原样还原）；迁移 `TestMigrateIntegrationAddsTaskTimeoutColumn` 断言列类型与可空性；`TestPrepare` 增补 500ns 不被截断、负值被拒两个用例。迁移测试的版本期望抽成常量 `latestMigrationVersion`——新增迁移必须同步上调，否则测试报错 |

## 4. 租约、恢复与失败处理

| 状态 | 工作项 | 完成标准 | 证据/备注 |
|---|---|---|---|
| [x] | 租约续期 | DB 时间为准；有效 owner/epoch 可续期；失租 RowsAffected=0 | 2026-10-15 提交 `7d09e27`：`store/mysql` 新增 `renewLeaseSQL` + `RenewLease`（`lease_until = TIMESTAMPADD(MICROSECOND, ?, UTC_TIMESTAMP(6))`，**从数据库当前时间起算、不在旧值上累加**——累加会让租约随续租次数越推越远，一次失败的续租就能把任务锁住远超预期；`WHERE` 与三条写回完全一致，含 `status='running'`，否则失去租约的旧 Worker 能靠续租把自己"续"回有效状态）；`WorkerStore` 接口新增该方法；根包 `Worker` 为每条在途任务起一个租约看护协程，按 `Config.HeartbeatInterval`（默认 10s = 租约的 1/3）续租，单次续租自带一个心跳周期的超时预算，避免数据库卡住时看护协程无限期阻塞。真库用例 `TestRenewLeaseExtendsLeaseUntilFromDatabaseClock`：区间断言把"在旧 `lease_until` 上累加"的实现排除掉（两种算法结果相差整整一个原始租约长度），并断言 epoch/attempt/owner/status 一律不变；端到端 `TestWorkerRenewsLeaseSoHandlerCanOutliveOneLease`：Handler 轮询数据库直到**亲眼看到**租约被延到比初始更晚、且数据库时钟已越过初始到期时间才返回，最终写回成功、`attempt`/`lease_epoch` 均为 1。**证伪检查**：停掉续租后该用例 25s 后失败，任务停在 `running`——正是 §3 遗留的缺口 |
| [x] | lease 丢失取消 context | heartbeat 发现失租后取消 handler context | 2026-10-15 提交 `7d09e27`：`process` 改用 `context.WithCancelCause` 把**三种取消原因**分开——停机（`context.Canceled`）、逐任务超时（`context.DeadlineExceeded`）、失去租约（`ErrLeaseLost`）；Handler 可用 `context.Cause` 分辨自己是被谁叫停的，Worker 据原因决定要不要写回（详见技术设计 §5.3.1）。看护确认失租即 `cancelTask` 取消 Handler，任务保持 `running`。**续租失败 ≠ 失租**：只有 `ErrLeaseLost` 才办这件事，连接断开/语句超时一类驱动错误只上报并在下个心跳周期重试（需求文档 §9），否则一次数据库抖动会掐掉所有在途 Handler。用例：`TestWorkerCancelsHandlerWhenLeaseIsLost`（Handler 看到的原因包着 `ErrLeaseLost`，且 succeeded/retried/dead **零写回**）、`TestWorkerKeepsRunningWhenRenewFailsWithInfrastructureError`（连续两次驱动错误后 Handler 仍正常跑完并被成功确认）、`TestWorkerDoesNotReportRenewalsCutShortByShutdown`（停机打断的续租既不算失租也不上报成续租失败）。既有停机用例补上原因断言：`context.Cause` 是 `context.Canceled` 且**不是** `ErrLeaseLost` |
| [x] | 过期租约回收 | 未耗尽次数回 pending；耗尽次数进入 dead | 2026-10-15 提交 `75c2e04`：**形态**——能力挂在既有 `WorkerStore` 接口上（`ReapExpiredLeases(ctx, limit)`），不另起可选接口；`Config` 只加 `ReapInterval`（默认 `DefaultReapInterval = 30s`，**负值关闭回收器**，0 由 `WithDefaults` 填成默认值、`Validate` 拒绝），批大小是内部常量 `reapBatchSize = 100` 不进配置（它调的是"回收事务有多长"，属实现细节）。**根包**：`Run` 里与执行循环并列跑一个 ticker 驱动的回收循环，驱动错误只上报、下个周期重试——回收能力要是能被一次数据库抖动关掉，崩溃遗留的任务就会一直没人管；`ctx` 取消即退出，且停机造成的失败不上报。停机等待同时观察执行侧与回收侧，超时消息能区分是哪一侧没停。**store**：一个短事务完成"选出 → 分流 → 写回 → 提交"——`attempt >= max_attempts` 写 `dead`（`last_error='lease expired at final attempt'`、`finished_at`），否则回 `pending`（`run_at = UTC_TIMESTAMP(6)`、清空租约、`last_error='lease expired'`）。写回守卫是**读到的 `lease_until` 原值** + `status='running'`，而不是 owner/epoch：回收器不是租约持有者，没有凭据可比；影响 0 行只跳过该行，**刻意不返回 `ErrLeaseLost`**（那是持有者的语义，借用它会让"集群里有别的回收器在干活"看起来像故障）。`attempt` 与 `lease_epoch` 都不动——这次领取已经用掉一次机会，`epoch` 属于"领取"这个动作。**顺手抓到并修掉一个真 bug**：这条加锁读要连带取 `attempt` / `max_attempts`，两列不在 `ix_gotask_lease` 里，所以它不是覆盖索引；实测优化器改选 `type=ALL, key=NULL, Using filesort`（全表扫描 + 排序），`FOR UPDATE` 在扫描过程中把**整个积压**锁住——`LIMIT` 不再限制加锁范围、`SKIP LOCKED` 失效，12 条过期任务 4 个回收器并发只有一个能推进、合计只回收 3 条（`data_locks` 13 条 vs 修复后 7 条）。已改成 `FORCE INDEX (ix_gotask_lease)`（**完整的事故经过、`EXPLAIN`/`data_locks` 实测对照与备选方案取舍见技术设计「附录 A」**）。对照：领取的加锁读只取 `id`，对 `ix_gotask_claim` 天然覆盖故无需提示（`EXPLAIN` 复核实测 `key=ix_gotask_claim, Using index`）。**测试**：根包 6 个用例（按间隔回收、基础设施错误后继续、负值可关闭、停机超时能归因到回收器/两侧、停机造成的回收失败不上报）；真库 9 个用例（回 pending / 耗尽进 dead / 未过期的租约不动 / 非 running 不动 / 迟到租约值被守卫拒绝 / `limit` 生效 / **并发回收安全** / **加锁读的计划与锁范围** / **端到端**见 §6）。其中 `TestReapLockingReadStaysOnLeaseIndex` 抽出来单独钉住"加锁读必须停在 lease 索引上"：计划层断言 `EXPLAIN` 的 `key`，行为层断言一个事务持锁时另一个仍取到不相交的 `limit` 行——用两个手写事务而非两个 Worker，图的是**确定性**（不把成败压在 goroutine 调度上）。**证伪检查**：去掉 `FORCE INDEX` 后该用例（`key=""`、并发读返回 0 行）与并发回收用例（合计只回收 3 条）都立刻失败，还原后与备份逐字节一致 |
| [x] | fencing 状态写回 | 旧 epoch 无法续租、成功确认或失败确认 | 本行拆成两半，两侧均有真库证据。**成功/失败确认**在 §3 已完成：`succeedTaskSQL`/`retryTaskSQL`/`markDeadTaskSQL` 共用 `(id, status='running', owner, epoch, lease_until 未过期)` 四重条件，§3 已有正常路径与拒绝路径的真库用例。**续租一侧**由 2026-10-15 提交 `7d09e27` 补齐：`TestRenewLeaseRejectsStaleCredentialsAndExpiredLease` 五个子用例覆盖旧 owner、旧 epoch、已过期租约、已是终态（不能被续租复活）、任务不存在，全部返回 `ErrLeaseLost` 且**库里零痕迹**（`lease_until` 未被改动）。另有 `TestRenewLeaseRejectsInvalidInput` 覆盖入参校验（零 ID、空 owner、零/负租约时长） |
| [x] | 重试退避与永久错误 | 有上限的指数退避 + 抖动、上限、永久错误及次数耗尽状态正确 | 2026-10-15：`fail` 从 §3 刻意留的"还有次数就立即回 `pending`"升级为**两次分流**——`IsPermanent(cause) || task.Attempt >= task.MaxAttempts` 走 `MarkDead`，否则 `Retry(ctx, task, retryDelay(attempt), summary)`。两个"不再重试"的理由合在**一个**判断里：永久失败的可辨识性来自 `Permanent` 包装器自带的前缀（`"permanent failure: "`），不来自走哪个分支，因此先判永久还是先判耗尽在结果上完全等价——**这条是证伪检查当场改出来的**（见下）。退避抽成纯函数 `retryBackoffCap(attempt, base, max)`＝`base × 2^(attempt-1)` 并以 `max` 封顶，**先判再翻倍**，避免可配置的 `attempt` 很大时左移溢出成负数（那等价于"没有退避"）。抖动取 **equal jitter** `[cap/2, cap]`：技术设计 §5.5 原文写的是 full jitter，这里刻意偏离并已回填该节，理由有两条——full jitter 允许延迟取 0（等于立刻重试，只是更快吃掉剩余尝试次数），且只有上界可断言（"退避没生效"检查不出来）。`RetryBaseDelay`/`RetryMaxDelay`（默认 1s / 1h）此前已声明并校验但无人使用，本项接上。直接后果：未知 task type（`ErrNoHandler`）与 payload 解码失败（`ErrInvalidPayload`）此前"带永久标记但仍被重试到次数耗尽"，现在第一次就落 `dead`。**根包用例**：`TestWorkerUnknownTypeGoesStraightToDead` / `TestWorkerInvalidPayloadGoesStraightToDead`（除终态外断言 `Retry` **零调用**——只看 `dead` 分辨不出"先重试一次再 dead"；后者还断言解码失败时 Handler 函数体没进）、`TestWorkerPermanentFailureSkipsRetries`、`TestWorkerFailureOutcomeMatrix`（判定规则表）、`TestWorkerRetrybackoffCapIsExponentialAndCapped`（纯函数，含 `1<<20` 溢出、`base > max`、`max = 0` 边界）、`TestWorkerRetryDelayStaysWithinItsBackoffCap`（100 次抽样都落在 `[cap/2, cap]`，一次排除"没有退避"与"没有上限"两类错误实现）。**真库用例**：`TestWorkerPermanentFailureEndsDeadInOneAttempt`（关键断言是 `attempt == 1`——库侧尝试次数是"到底重试过没有"的独立证据，只看 `dead` 终态区分不出"重试几次再 dead"；并断言 `last_error` 带永久前缀、`finished_at` 有值、租约被清空）、`TestWorkerRetryBackoffLandsInRunAt`（断言 `run_at - t0` 落在 `[base/2 - slack, base + slack]`，证明延迟确实走到了 SQL 的 `TIMESTAMPADD`，而不是只在 Go 侧算了个值就丢掉）。**证伪检查两次**：① 短路永久分支 ⇒ 用例照常通过，暴露出"为分支顺序写断言"的用例只是装饰，据此改成只断言可观察结果的 `TestWorkerFailureOutcomeMatrix`；② 去掉退避（让 `retryDelay` 直接返回 0）⇒ 区间断言立刻失败。两次还原后均与备份逐字节一致 |
| [x] | panic 处理 | 单任务 panic 转失败；Worker 不因可恢复 panic 崩溃 | 2026-10-15：新增 `runHandler` 作为**单任务边界的 recover**，`execute` 改为经由它调用 Handler。边界刻意收窄到"只包住 Handler 调用本身"——本库自己的领取/写回/回收逻辑若 panic 属于本库 bug，应当照常崩掉，不该被伪装成"某条任务失败"藏起来（技术设计 §5.5「进程级不可恢复错误不应被吞掉」）。转出的错误形如 `handler panicked: <值>\n<堆栈>`，**刻意不打永久标记**：需求只说"视为一次失败"，而 panic 未必是确定性的（可能依赖外部状态或时序），直接判死会丢掉恢复机会，去留交给 `max_attempts` 兜底。堆栈由 `panicStack()` 截断——丢掉没有诊断价值的 `goroutine N [running]:` 首行、取前 12 行、超出以 `...` 收尾；`last_error` 列只有 2048 字符且定位是给人读的摘要，先自己截还能保住开头那句关键信息。**明确的局限**：只能捕获同一个 goroutine 里的 panic，Handler 在自己起的子协程里 panic 拦不住，Go 会直接终止进程；这条同时写进代码注释与文档，免得把"Worker 兜底"误读成"随便怎么 panic 都安全"。**根包用例**：`TestRunHandlerConvertsPanicToError`（正常返回不改写、普通错误原样穿透且不带 panic 措辞、panic 转出的错误含 panic 值 + 指回 `worker_test.go` 的堆栈 + 不带永久标记）、`TestWorkerRetriesHandlerPanic`（走的就是普通失败那条路：Retry 一次、dead 零次、摘要含值与措辞、延迟落在同一套退避区间）、`TestWorkerHandlerPanicGoesDeadWhenAttemptsExhausted`（次数耗尽同样进死信，且不带永久前缀）、`TestWorkerKeepsProcessingBatchAfterHandlerPanic`（批里第一条 panic，第二条照常执行并成功——守住"兜底是单任务边界"）。**真库用例**：`TestWorkerRetriesAfterHandlerPanicAndThenSucceeds`——第一次执行 panic、退避后被重新领取并成功；关键断言是 **handler 被调用 2 次** 与 **attempt == 2**，只看终态 `succeeded` 区分不出"panic 被当成了成功"这类错误实现；同时断言成功写回把第一次的 panic 摘要清成 NULL。**证伪检查**：拆掉 recover 后上述用例立刻失败（panic 直接打穿测试进程，退出码 1，堆栈指回 `worker_test.go`），还原后与备份逐字节一致（md5 相同）。根包覆盖率仍 100% |
| [ ] | 终态清理 | succeeded/dead/canceled 按保留期小批量清理 | 待实现 |

## 5. 示例、适配器与可观测性

| 状态 | 工作项 | 完成标准 | 证据/备注 |
|---|---|---|---|
| [ ] | 独立示例服务 | 示例业务行与任务行同事务创建；worker 可执行 handler | 待实现 |
| [ ] | 故障演示 | 文档化停止 Worker、等待租约回收、重启后任务完成 | 待实现——**机制侧已可运行**：`TestWorkerReapsTaskAbandonedByShutdown` 已把"停机遗留 running → 租约自然过期 → 被回收 → 跑完"整条链路跑通，缺的只是把它写成可照着复现的文档/示例（依赖示例服务与可独立运行的入口）。 |
| [ ] | 官方 GORM adapter | 通过调用方 GORM 事务执行任务 INSERT；核心模块无 GORM 依赖 | 待实现 |
| [ ] | 结构化日志 | 有 task id/type/queue/attempt/epoch/worker，不泄漏 payload | 待实现 |
| [ ] | Metrics 接口 | 任务计数、延迟、处理时间、租约失效等指标，无高基数标签 | 待实现 |

## 6. 验收与简历证据

| 状态 | 工作项 | 完成标准 | 证据/备注 |
|---|---|---|---|
| [x] | MySQL 集成测试 | 事务、锁、租约和索引语义用真实 MySQL 验证 | 四类语义都已在 MySQL 8.0.46 上验证过（细节见 §2/§3/§4 各行证据）：**事务**——`EnqueueTx` 的提交可见/回滚无痕；**锁**——`FOR UPDATE SKIP LOCKED` 跨会话跳过被锁行（`TestClaimSkipsRowsLockedByAnotherSession`，另会话持有行锁时 Claim 不阻塞且跳过它），且加锁范围**受 `LIMIT` 约束**（`TestReapLockingReadStaysOnLeaseIndex`）；**租约**——claim 置租约与 epoch、续租按数据库时钟延长、失租/过期/易主一律 `ErrLeaseLost`、停机保持 running、过期后由回收器接手；**索引**——两条加锁读走哪条索引都用 `EXPLAIN` 钉住：claim 走 `ix_gotask_claim` 且 `Using index`（覆盖），回收的加锁读走 `ix_gotask_lease`（该读非要取 `attempt`/`max_attempts`，"覆盖索引"这条办法不成立，只能显式 `FORCE INDEX`）。集成测试**不以 mock 替代**：内存假 store 只用于根包的分支覆盖。 |
| [x] | 并发正确性测试 | N 个 worker 并发处理 M 个任务，同租约期每项至多一位有效持有者 | §3「多 Worker 并发领取」三件套已覆盖（提交 `454ccac`）：8 领取者同一栅栏后抢 40 条（两层断言：进程侧 id 不重不漏 + **库侧 `lease_epoch`/`attempt` 全为 1** 的独立复核）、跨会话 `SKIP LOCKED` 跳过被锁行、4 Worker（各 2 并发）跑 24 条且每条 Handler **恰好执行 1 次**。真库上还顺带排除"租约过期被重领"这一混淆因素（领取用 `LeaseDuration = time.Minute`）。 |
| [x] | 故障注入：claim 后退出 | 租约过期后任务能重新执行并最终完成 | 2026-10-15 提交 `75c2e04`：`TestWorkerReapsTaskAbandonedByShutdown` 端到端覆盖——Worker A 的 Handler **无视 ctx**，停机只能超时放弃（`Run` 返回 `ErrShutdownTimeout`），任务留在 `running`；A 的续租看护随停机一起停掉，租约按配置的 `LeaseDuration` **自然过期**（不手工改库）；另一个 Worker 的回收器把它捡回 `pending`，随后被领出来跑完。整个链路只走产品路径，且不依赖任何手工构造的过期租约。**这条用例的写法有个坑值得记下**：必须等 Handler **真的进去了**（用例里用 channel 由 Handler 自己发信号）再取消 A——只等库里的 `status` 变成 `running` 是不够的，claim 提交与 Handler 启动之间有个窗口，在那里停机会让 `runOnce` 按设计跳过整批（剩余任务不启动 Handler），于是 `Run` 干净返回 `nil`——那是正确行为，却让用例测不到它想测的东西。 |
| [ ] | 故障注入：副作用后 ack 前退出 | 允许重复执行，幂等示例最终效果正确 | 待实现（依赖示例服务） |
| [ ] | 故障注入：旧 worker 复苏 | fencing 拒绝过期 worker 写回 | 待实现——机制侧已有等价证据：§3 三条写回与 2026-10-15 的续租都用**陈旧 owner/epoch、已过期租约**直接验证了 `ErrLeaseLost` 且库里零痕迹。缺的是"让一个旧 Worker 延迟苏醒再真的尝试写回"的端到端场景，等示例服务再补。 |
| [x] | Race 检查 | `go test -race` 覆盖 worker 内部并发路径 | 全量 `go test -race -count=1 ./...` 三个包全绿、日志中 **0 处 `DATA RACE`**（2026-10-15 复跑：154 个顶层用例 / 290 条 PASS 记录、0 FAIL、0 SKIP；`store/mysql` 97.9s）。worker 内部并发路径（并发领取池、每条任务一个租约看护协程、**回收循环**、停机与失租同时取消）都在 `-race` 下跑过。根包语句覆盖 100.0%，`store/mysql` 91.4%。 |
| [ ] | 性能基线 | 记录硬件/DB/配置、吞吐、排队延迟、API/DB成本 | 待实现 |
| [ ] | README 与语义说明 | 安装、使用、事务入队、至少一次、幂等、恢复和限制齐全 | 进行中——**语义部分已齐**（项目定位、执行语义与边界、至少一次与幂等、fencing 保护范围、本地环境、文档索引，2026-10-15 已把自相矛盾的状态段改成分「已可用/尚未实现」）；**缺**安装与使用示例、`go get` 与最小可运行代码、故障恢复演示（这两项依赖示例服务）。 |
| [ ] | STAR 指标回填 | 只写实测数值，并保留命令、环境和原始结果 | 待实现（依赖性能基线） |

## 当前可开工项

§0–§3 全部完成，§4 完成 6/7（租约续期、失租取消 context、**过期租约回收**、fencing 状态写回、**重试退避与永久错误**、**panic 兜底**），只剩「终态清理」。**下一项是 §4「终态清理」**——succeeded/dead/canceled 按保留期小批量删除。回收器落地后 §3 的两个缺口（停机超时、进程崩溃遗留的 `running`）已经闭合，§6「故障注入：claim 后退出」也已可验证。

回收器的现实约束：**回收只在 Worker 进程内跑**，所以"集群里至少有一个 Worker 在运行"是任务终能恢复的前提；Worker 全部下线期间（含用 `ReapInterval` 负值关闭回收器的部署）遗留的 `running` 只剩租约过期、无人接手。这是当前形态的已知边界，写进 README 的语义部分。

已确认的工程约定：module path `github.com/Tidehyc/gorangetask`（gh 账号 `Tidehyc`）、最低 Go 1.24、许可证 MIT、提交以需求为粒度、每次开工前先确认范围。

本地开发库：容器化 MySQL（`gotask-mysql`，`127.0.0.1:3306`，库 `gotask`/`root`/`rootpw`），用 `scripts/dev-mysql.sh up|down|reset|status|shell` 管理。集成测试复用同一个 `gotask` 库（`up` 已建），`store/mysql` 会自动创建派生库 `gotask_store`——见上文「集成测试的库隔离」下的说明，不需要手工建任何库。

阶段完成后更新本表状态，并补上测试命令、结果、commit 或基准文件路径。不要把“代码已写”直接记作“已验收”。
