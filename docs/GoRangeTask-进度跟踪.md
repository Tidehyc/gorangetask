# GoRangeTask（gorangetask）项目进度跟踪

> 用途：对照需求和技术设计追踪项目完成情况；每项完成后补充提交、测试日志、基准结果或文档链接作为证据。
> 当前状态：构建阶段
> 更新日期：2026-10-13
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

测试口径（2026-10-13）：新增根包 **22 个 Worker 单测**（内存假 store，不连库；含「Handler 内栅栏」用例证明并发池确有 N 个同时运行，根包覆盖率 100%）与 `store/mysql` **24 个 Worker 真库集成用例**（到期/队列/limit/顺序/不重复领取、epoch 与 attempt 递增、claim 走 `ix_gotask_claim` 且 `Using index`、三种写回的正常与 fencing 拒绝、延迟与 `last_error` 截断、三个端到端，以及**并发领取**三件套：8 领取者抢 40 条、跨会话 `SKIP LOCKED` 跳过被锁行、4 Worker 每条恰好执行一次）。全量 `go test -race -count=1 ./...`：**239 条用例记录（119 顶层 + 120 子用例）、0 FAIL、0 SKIP**；`store/mysql` 语句覆盖率 93.1%（未覆盖的是驱动层报错分支，如 `BeginTx`/`Exec`/`RowsAffected` 自身失败，无法在真实驱动上构造）。

> 集成测试的库隔离：`go test ./...` 会并发运行不同的包，而集成测试的 setup 是破坏性的（`DROP TABLE` + 重新迁移）。因此 `store/mysql` 使用派生库 `<DSN 库名>_store` 并自动创建，避免与 `migrations/mysql` 的集成测试互相删表而报出与产品无关的假失败（`ErrDirty`、`Error 1050 Table already exists`）。新增集成测试包时应取一个独立后缀。

## 3. Worker 执行闭环

| 状态 | 工作项 | 完成标准 | 证据/备注 |
|---|---|---|---|
| [x] | Handler 注册与 payload 解码 | task type 映射、未知类型处理、schema 错误路径明确 | 2026-10-12 提交 `e2c4147`：`Registry.Resolve` 对未注册类型返回带永久标记的 `ErrNoHandler`（task_type 是入队时定死的值，重试不会让它变得可处理 ⇒ 直接进 dead）；`TypedHandler[T]` 把 `json.Unmarshal` 收进库，解码失败返回带永久标记的 `ErrInvalidPayload`（覆盖"无法解码 payload 不能无限快速重试"），nil 函数构造期 panic；`Permanent`/`IsPermanent` 用私有类型打标记，nil 安全、可重复、`errors.Is`/`As` 仍可穿透。根包覆盖率 100% |
| [x] | 单 Worker claim/execute/ack | handler 在 claim 事务提交后运行；成功写回终态 | 2026-10-13 提交 `4abcf9b`：根包新增执行侧契约 `WorkerStore`（`Claim`/`Succeed`/`Retry`/`MarkDead`，根包只依赖接口不写 SQL）与 `Worker`；`Run` 按 `Config.Concurrency` 起等量 goroutine，各自「领取一批 → 逐条执行 → 写回」，空队列时指数退避至 `MaxPollInterval` 并加抖动、领到任务即重置；Handler 的超时用 `cfg.HandlerTimeout` 作为 ctx deadline。**claim 两段式**（store/mysql）：事务内只 `SELECT id ... FOR UPDATE SKIP LOCKED`（只为停在覆盖索引 `ix_gotask_claim` 上——把 payload 一起取出来覆盖索引就没了）并逐条条件更新为 `running`（置 owner/`lease_until`、`lease_epoch+1`、`attempt+1`），**提交之后**才按主键回读整行，因此 Handler 绝不在事务/行锁内运行。三种写回共用 `(id, status='running', owner, epoch, lease_until 未过期)` 四重 fencing，影响 0 行 → `ErrLeaseLost`；`run_at`/`lease_until` 一律用数据库时钟（`TIMESTAMPADD` + `UTC_TIMESTAMP(6)`）。端到端用例里 Handler 用**另一条连接**读库，断言它看到的已是 `running`——直接证明 claim 事务先提交。失败策略本项**刻意最简**（还有次数立即回 pending、次数耗尽 dead），退避与永久错误分类留给 §4 |
| [x] | 多 Worker 并发领取 | MySQL `SKIP LOCKED` 集成测试证明并发无双重有效租约 | 2026-10-13 提交 `454ccac`：**无需改动生产代码**——claim 从设计上就是并发安全的（`FOR UPDATE SKIP LOCKED` + 单条 `WHERE status='pending'` 条件更新），本项的工作是把该性质钉在真库上。三个用例：① **8 个领取者**在同一 barrier 后抢 40 条任务、各用不同 `Limit`（1..5，顺带覆盖批量领取路径），断言两两不重复且零丢失，并**独立复核库侧** `lease_epoch`/`attempt` 全为 1（epoch 每次领取 +1 ⇒ 1 即「只被领取过一次」，这一层不依赖进程侧记录）；实测 8 个 owner 全部分到任务（2/4/6/8/8/2/4/6），确认确实在并发抢。② 另一会话用普通 `FOR UPDATE` 锁住「本该第一个被领走」的行，Claim 必须**跳过它**拿走其余两条且**不阻塞**（<3s），锁释放后那条仍可领取——直接验证 `SKIP LOCKED` 语义。③ 4 个 Worker 实例（各 2 并发）跑 24 条任务，断言每条 Handler **恰好执行 1 次** |
| [ ] | Worker graceful shutdown | 停止领取、通知运行 handler cancel、在超时内等待退出 | 待实现 |
| [ ] | 持久化逐任务超时 | `Task.Timeout` 落库，领取后能还原该任务的 Handler 超时 | 2026-10-13 决定：**本项先用 Worker 默认 `HandlerTimeout`**。设计缺口——`Task.Timeout` 文档上写作「本次 Handler 超时」，但 DDL 里没有对应列，领取后就丢了，重启/重领无法还原。后续加迁移 `0002` 增一列（如 `timeout_us BIGINT NULL`），入队时持久化、`ClaimedTask` 带回 |

## 4. 租约、恢复与失败处理

| 状态 | 工作项 | 完成标准 | 证据/备注 |
|---|---|---|---|
| [ ] | 租约续期 | DB 时间为准；有效 owner/epoch 可续期；失租 RowsAffected=0 | 待实现 |
| [ ] | lease 丢失取消 context | heartbeat 发现失租后取消 handler context | 待实现 |
| [ ] | 过期租约回收 | 未耗尽次数回 pending；耗尽次数进入 dead | 待实现 |
| [ ] | fencing 状态写回 | 旧 epoch 无法续租、成功确认或失败确认 | 待实现 |
| [ ] | 重试退避与永久错误 | full-jitter 指数退避、上限、永久错误及次数耗尽状态正确 | 待实现 |
| [ ] | panic 处理 | 单任务 panic 转失败；Worker 不因可恢复 panic 崩溃 | 待实现 |
| [ ] | 终态清理 | succeeded/dead/canceled 按保留期小批量清理 | 待实现 |

## 5. 示例、适配器与可观测性

| 状态 | 工作项 | 完成标准 | 证据/备注 |
|---|---|---|---|
| [ ] | 独立示例服务 | 示例业务行与任务行同事务创建；worker 可执行 handler | 待实现 |
| [ ] | 故障演示 | 文档化停止 Worker、等待租约回收、重启后任务完成 | 待实现 |
| [ ] | 官方 GORM adapter | 通过调用方 GORM 事务执行任务 INSERT；核心模块无 GORM 依赖 | 待实现 |
| [ ] | 结构化日志 | 有 task id/type/queue/attempt/epoch/worker，不泄漏 payload | 待实现 |
| [ ] | Metrics 接口 | 任务计数、延迟、处理时间、租约失效等指标，无高基数标签 | 待实现 |

## 6. 验收与简历证据

| 状态 | 工作项 | 完成标准 | 证据/备注 |
|---|---|---|---|
| [ ] | MySQL 集成测试 | 事务、锁、租约和索引语义用真实 MySQL 验证 | 待实现 |
| [ ] | 并发正确性测试 | N 个 worker 并发处理 M 个任务，同租约期每项至多一位有效持有者 | 待实现 |
| [ ] | 故障注入：claim 后退出 | 租约过期后任务能重新执行并最终完成 | 待实现 |
| [ ] | 故障注入：副作用后 ack 前退出 | 允许重复执行，幂等示例最终效果正确 | 待实现 |
| [ ] | 故障注入：旧 worker 复苏 | fencing 拒绝过期 worker 写回 | 待实现 |
| [ ] | Race 检查 | `go test -race` 覆盖 worker 内部并发路径 | 待实现 |
| [ ] | 性能基线 | 记录硬件/DB/配置、吞吐、排队延迟、API/DB成本 | 待实现 |
| [ ] | README 与语义说明 | 安装、使用、事务入队、至少一次、幂等、恢复和限制齐全 | 待实现 |
| [ ] | STAR 指标回填 | 只写实测数值，并保留命令、环境和原始结果 | 待实现 |

## 当前可开工项与待用户提供信息

技术方案已足以开始仓库骨架、migration 和核心 API 设计。已确认：module path `github.com/Tidehyc/gorangetask`（gh 账号 `Tidehyc` 已复核有效）、最低 Go 1.24、许可证 MIT。

本地开发库已就绪：容器化 MySQL 8.0.46（`gotask-mysql`，卷 `gotask-mysql-data`，`127.0.0.1:3306`，库 `gotask`/`root`/`rootpw`），用 `scripts/dev-mysql.sh up|down|reset|status|shell` 管理；集成测试可指向该实例，或后续用 Testcontainers 自建容器（Docker 守护进程已运行，可用）。

下一步：本地 `git init`（main 分支）→ 首个提交为项目骨架（README、LICENSE、go.mod、.gitignore，保留 `docs/`）→ 再 `gh repo create Tidehyc/gorangetask` 建远程并推送。

开始实现后，每次阶段完成时更新本表状态，并补上测试命令、结果、commit 或基准文件路径。不要把“代码已写”直接记作“已验收”。
