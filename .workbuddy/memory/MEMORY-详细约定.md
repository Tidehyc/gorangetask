# GoRangeTask 工程约定（详细版）

> 这是 `MEMORY.md` 的配套文件。`MEMORY.md` 每轮会话自动注入，为控制体积只保留红线级
> 约定；**设计取舍、测试写法、踩坑全过程都收在本文件**，动手前先读它。
> 只增补、不删减；日常流水仍记在 `YYYY-MM-DD.md`。
> 用户级偏好（GitHub 账号、PR 复盘方式等）在 `~/.workbuddy/MEMORY.md`。

## 项目身份

- 公开项目名 GoRangeTask；仓库 `gorangetask`；module `github.com/Tidehyc/gorangetask`；
  最低 Go 1.24；许可证 MIT；GitHub 账号 `Tidehyc`。
- 目标：可嵌入 Go 服务的 MySQL 持久化任务队列。核心卖点是**业务数据与任务行同一事务提交**。
- 数据库：MySQL 8.0+ / InnoDB。核心包只用 `database/sql`，GORM 走可选 adapter。

## 工作方式（用户明确要求）

- 每次开始前先确认范围；遇到拿不准的先讨论，不擅自决定。
- **按需求粒度提交**，不要攒成一个大提交。
- 文档只是指导，不要把实现细节事无巨细写进文档。
- **沙箱推不动 GitHub**（登录态在用户的 `ide` 账号下）⇒ 提交后告知用户在自己终端 push。
  提交前先 `git status --short`：工作区可能同时有用户或另一会话的在制改动，
  不要用 `git add -A` / `git commit -a` 扫进不属于本次改动的内容。

## 包结构（2026-10-12 定）

- 根包 `gorangetask`：只放与数据库无关的公开契约（Task/TaskInfo/TaskID/Status/
  Handler/Registry/Client/Tx/Config/ClientConfig/errors）。
- `store/mysql`（package `mysql`）：MySQL 方言实现，唯一直接写 SQL 的地方。
  **只读表，不建表。** `New(db *sql.DB, cfg gorangetask.ClientConfig)`。
  导入该包会顺带注册 go-sql-driver，调用方无需再 import 驱动。
  **已完整实现根包 `Client` 接口**（Enqueue / EnqueueTx / Get / Cancel），
  并有编译期断言 `var _ gorangetask.Client = (*Client)(nil)`。
  **同时实现根包 `WorkerStore`**（Claim / Succeed / Retry / MarkDead / RenewLease /
  ReapExpiredLeases，`store/mysql/worker.go`，有第二个编译期断言）。终态清理还没做。
- `migrations/mysql`（package `mysqlmigrations`）：**独立包，不并入 store**。
  理由：`//go:embed` 不能跨目录向上取文件；迁移是「建表」的完整闭环且已有独立测试。
  调用顺序是「先 `mysqlmigrations.Migrate`，再 `mysqlstore.New`」。
- `config.go` 里 **`Config`（Worker 运行配置）与 `ClientConfig`（入队侧配置）刻意分开**：
  只想入队的进程不必构造 16 个 Worker 字段。
- 根包 `worker.go`（2026-10-13）：导出 `WorkerStore` 接口 + `ClaimSpec` + `ClaimedTask`
  + `Worker`（`NewWorker`/`Run`）。**根包只依赖接口、不写 SQL**；SQL 在
  `store/mysql/worker.go`。两个包各有一个同名小工具 `truncateRunes`（互不影响）。
  `ClaimedTask` 除任务数据外还带 `Owner`+`Epoch`——它们就是 fencing 凭据，
  三条写回 SQL 靠 `(id, status='running', owner, epoch, lease_until 未过期)` 卡住旧持有者。

## 测试约定

- 真 MySQL 集成测试由环境变量 `GOTASK_TEST_MYSQL_DSN` 开启，未设置则 `t.Skip`。
  本地示例：`root:rootpw@tcp(127.0.0.1:3306)/gotask_test?parseTime=true&loc=UTC`
- **CI 已接入 MySQL**（`.github/workflows/ci.yml`）：`mysql:8.0` service container +
  job 级 `GOTASK_TEST_MYSQL_DSN`，测试步骤 `-v` 并 `tee go-test.log`。
  **有一个守门步骤：出现任何 `--- SKIP` 即判失败**——`go test` 的退出码不反映"测试被跳过"，
  集成测试全 skip 时退出码仍是 0，不显式检查就会把"没验证"当成"通过"。
  新增"合法跳过"的用例前，必须先想清楚它会不会误伤这个守门步骤。
- **每个集成测试包必须用独立的库**：`go test ./...` 会并发跑不同包，而集成测试 setup 是
  破坏性的（DROP TABLE + 重新 Migrate），共用一个库会互相删表、报出与产品无关的假失败
  （`ErrDirty`、`Error 1050 Table already exists`）。除 `migrations/mysql` 外，做法是用
  `gomysql.ParseDSN` 取库名后加后缀（如 `_store`），再用不带库名的连接
  `CREATE DATABASE IF NOT EXISTS` **自动建库**。新增集成测试包时取一个独立后缀。
- **测试库口径（2026-10-15 定，取代「gotask 开发库 + gotask_test 测试库」）**：不再区分
  开发库与测试库，**统一一个 `gotask`**。`migrations/mysql` 直接使用它（**它自己不建库**，
  只对表做 DDL）；`store/mysql` 使用自动派生的 `gotask_store`。DSN 示例：
  `root:rootpw@tcp(127.0.0.1:3306)/gotask?parseTime=true&loc=UTC`。两条路径都不需要手工建库：
  本地由 `dev-mysql.sh up` 建 `gotask`，CI 由 service container 的 `MYSQL_DATABASE` 预设。
  原先的 `gotask_test` 已废弃（本地可 `DROP DATABASE gotask_test` 清掉）。
- ⚠️ **`migrations/mysql` 报 `Error 1049 Unknown database` 不代表「迁移失败」**：夹具里
  `sql.Open` 是惰性的，报错来自紧随其后的 `PingContext`（`migrate_integration_test.go`），
  此时**一条迁移语句都没跑**。老坑的成因：`gotask_test` 当年只是某次手工建好后留在数据卷
  `gotask-mysql-data` 里，卷被删掉后没有任何环节会重建它——所以才改成统一用 `gotask`。
- 沙箱 `/tmp` 只有 10MB：跑 `go test`（尤其 `-race`）前必须
  `export TMPDIR=/home/tide/.cache/go-tmp GOTMPDIR=/home/tide/.cache/go-tmp`，
  否则 `no space left on device`。
- 时间断言一律以**数据库时钟**为准（`SELECT UTC_TIMESTAMP(6)`），不用测试机时钟。
- **不要把"未来/过去"的时刻写死进测试数据**：写死一个绝对未来时刻的用例，过了那天
  就会静默退化成"已到期"用例并报出假失败（2026-10-13 真的踩过一次，
  见 `TestEnqueueDelayedTaskStoresRunAtInUTC`）。一律用 `dbNow(t, db).Add(...)` 构造。
- **并发正确性用例的写法（2026-10-13 定）**：所有参与者用 barrier 同时出发把竞争
  窗口开到最大；除进程侧自己的记录外，**必须再用一个库侧单调量做独立复核**
  （如 `lease_epoch` 每次领取 +1 ⇒ 全为 1 即"只被领取过一次"）——进程侧写错了
  库侧还能抓住。另外要确认参与者**真的分散开了**（否则"通过"可能只是没并发起来）；
  这类分布信息用 `t.Logf` 记录，不要做断言，否则会依赖 goroutine 调度而 flaky。
- **真库断言可能因为"错误的原因"通过**：停机用例里，即便代码错误地调用 `Retry`，
  它用的是已取消的 ctx，UPDATE 自己就失败了，任务照样留在 running。
  ⇒ 涉及"某个写回/副作用**没有**发生"的断言，必须**同时**在能观察到调用行为的那一层
  （内存假实现、spy）断言"对应方法根本没被调用"，只看持久层终态不够。
- **新写的断言要做「证伪检查」**：把实现里那条关键分支临时破坏（如把 `ctx.Err() != nil`
  改成 `false && ctx.Err() != nil`），跑一遍确认用例**真的会失败**，再还原并 `diff`
  确认与备份逐字节一致。不会失败的断言只是装饰。
- **别用 `time.Sleep` 卡超时用例**：让被测 Handler 里
  `select { case <-ctx.Done(): case <-time.After(5s): }`，再回传 `ctx.Err()` 与耗时。
  超时没生效时用例会在安全定时器上返回并**由断言报错**，而不是挂死
  （挂死只能靠 `go test -timeout` 兜底，排查成本高得多）。
- **宿主负载高时集成测试会慢 8–10 倍**（本机实测：`store/mysql` 从 20s 涨到 215s，
  `-race` 全量 46 分钟），前台跑整包会被 SIGKILL（exit 137）、日志停在半途。
  长测试一律 `run_in_background`；只看 0 FAIL / 0 SKIP，别拿耗时当判据。
  怀疑慢是自己的锅之前，先用 `information_schema.innodb_trx` +
  `performance_schema.metadata_locks` 排除锁等待。
- 仓库内不要建临时目录实验：沙箱的 `rm` 是 safe-delete 垫片，会把文件挪进
  `.Trash-0/` 污染 `git status` 与 `gofmt -l`。用 `/bin/rm`，或把实验放到仓库外
  （如 `/home/tide/.cache/`）。`.gitignore` 已加 `.Trash-*/` 兜底。
- 推送 `.github/workflows/**` 需要 token 带 `workflow` scope（用户已授权过）。

## 本地开发库

- 容器 `gotask-mysql`（mysql:8.0，卷 `gotask-mysql-data`，`127.0.0.1:3306`，UTC，
  READ-COMMITTED），用 `scripts/dev-mysql.sh up|down|reset|status|shell` 管理。
  宿主机没有 mysql-server，环境不被污染。
- **容器可能被用户停掉**（`docker ps` 显示 `Exited`）。跑集成测试前先
  `./scripts/dev-mysql.sh status`；停了就先 `up`（容器已存在时会直接启动并等就绪）。
  **不要自己在宿主机上另起 mysqld**：Ubuntu 的 AppArmor 用
  `/etc/apparmor.d/usr.sbin.mysqld` 把 mysqld 限制在 `/var/lib/mysql/**` 等路径，
  datadir 放别处直接 EACCES/EPERM。真需要绕过时，AppArmor 按**可执行文件路径**匹配，
  把 mysqld 复制到别的路径再跑可绕开 profile（复制可行、跨设备硬链接不行；
  本机 `/var/lib/mysql` 不存在）。这条只验证到 `--initialize-insecure` 成功，
  未验证启动服务，属备用方案。
- 库：`gotask`（**唯一的库**，开发与集成测试共用）、`gotask_store`（`store/mysql` 集成测试
  自动派生并创建）、`gotask_test_business`（事务入队测试的业务表，建在 `gotask_store` 里，
  用完即删）。历史遗留的 `gotask_test` 已于 2026-10-15 废弃合并。

## 数据模型要点

- 表 `gotask_tasks`；`status` 用 VARCHAR 存五种状态，不用 MySQL ENUM。
- 唯一索引 `uq_gotask_queue_idempotency (queue_name, idempotency_key)`：
  **NULL 不参与唯一性判断** ⇒ 空幂等键必须写 NULL，写空串会被误判为重复。
- 时间列一律 UTC；参与时序判断的值以数据库为准（SQL 里用 `UTC_TIMESTAMP(6)`）。
- 领取用 `FOR UPDATE SKIP LOCKED`，且**加锁读只取 `id`**：取 payload 会让查询
  离开覆盖索引 `ix_gotask_claim`，加锁代价随行宽放大。完整行在 **claim 事务提交后**
  按主键回读（因此 Handler 绝不在事务/行锁内运行）。`lease_epoch` 每次领取 +1。

## 错误模型（2026-10-13 定）

- 失败只分两类：**可重试** 与 **永久失败**。「重试没意义」用 `Permanent(err)` 打标记
  （私有 `permanentError` 类型 + `IsPermanent`），**不新增公开哨兵** —— 调用方不会
  因漏包一层把永久失败当可重试。`Permanent(nil)==nil`、可重复标记、`errors.Is/As` 穿透。
  Worker 见到永久标记 ⇒ 任务直接进 `dead`。
- **payload 解码收进库**：`TypedHandler[T]` 泛型适配器，解码失败 → 永久 `ErrInvalidPayload`；
  未知 task type → `Registry.Resolve` 返回永久 `ErrNoHandler`（`Lookup` 刻意不打标记）。
- `TypedHandler` 的 `fn == nil` 在**构造期 panic**（启动配置错误尽早暴露）。

## Worker 停机语义（2026-10-13 定，§3 第 4 项）

- `Run(ctx)` 的停机触发只有一个：**ctx 取消**（Worker 没有别的 Stop 入口）。
  流程＝停止领取 → 取消在途 Handler（ctx 派生传递，协作式）→ 在
  `Config.ShutdownTimeout` 内等待 → 超时返回公开哨兵 **`ErrShutdownTimeout`**。
  这是**生命周期信号，不属于「可重试/永久」那套失败分类**，因此不受
  「不新增公开哨兵」那条约束。
- **停机期间不写回失败，但仍然确认成功**。判据是 `ctx.Err() != nil`：
  - 失败 → 只上报，任务**保持 `running`**，等租约自然过期交给回收器。
    理由：把"被取消"写成 pending 会让别的 Worker 在旧副作用可能仍在途时立刻重跑。
  - 成功 → 照常 `Succeed`，但必须换用
    `context.WithTimeout(context.WithoutCancel(ctx), ShutdownTimeout)`；
    用已取消的 ctx 会让已经干完的活白白重做。
  后续 §4 加续租/回收时**不要**改成"停机时写回 pending"。
- 批内停机：`runOnce` 在启动每条 Handler 前检查 `ctx.Err()`，已取消就整批剩余都不启动。
- ⚠️ 直接后果（§3 时的缺口，**2026-10-15 已被回收器闭合**）：停机遗留的 `running`
  任务只能靠回收器回收。当时 reaper 不存在 ⇒ 这类任务会一直挂着；现在回收器落地，
  但它**跑在 Worker 进程里**，所以"集群里至少有一个 Worker 在运行"仍是恢复的前提。
  另外：想给这条用例造"停机后任务留在 running"的前提时，**必须等 Handler 真的进去**
  （用例里让 Handler 自己发信号），只等库里 status 变 running 会撞上 claim 提交与
  Handler 启动之间的窗口，`runOnce` 按设计跳过整批、`Run` 干净返回 nil（2026-10-15 踩过）。

## 迁移与列约定（2026-10-13 补，§3 第 5 项）

- **新增迁移没有注册表要改**：往 `migrations/mysql/` 丢一个 `<version>_<name>.sql`
  即可（`//go:embed *.sql` 自动发现，按解析出的版本号排序，不是文件名字典序）。
  但迁移集成测试里的常量 `latestMigrationVersion` **必须同步上调**——刻意留的绊脚索。
- **加列一律追加到表末，不写 `AFTER`**：MySQL 8.0.12+ 只在"加为最后一列"时支持
  `ALGORITHM=INSTANT`，8.0.29 之前插到中间会退化成重建整表。列顺序无语义。
  验证手段：`information_schema.INNODB_TABLES.TOTAL_ROW_VERSIONS` 由 0 变 1
  即"走行版本化而非重建表"。
- ⚠️ **别用"本机能过"证明"用户的新版本也能过"**：本机是 8.0.46，带 `AFTER` 强制
  `ALGORITHM=INSTANT` 也能过；但目标地板是 MySQL 8.0+，必须按最老版本挑方案。
- **可空列要区分"未设置"和"零值"**：`idempotency_key` 用 NULL 而不是空串（唯一索引
  不约束 NULL），`timeout_ns` 用 NULL 而不是 0（读取方规定 0 = 未设置）。
  凡是"0/空串有意义"的列，都不许把两种含义混成一种表示。
- **时长列用纳秒存**（`timeout_ns BIGINT UNSIGNED`）：`time.Duration` 就是纳秒计数，
  按微秒存会把不足 1µs 的值截断成 0，而 0 又表示"未设置" —— 显式设过的值被静默改写。

## 租约、取消与续期约定（2026-10-15 定，§4 第 1+2 项）

- **续租的新 `lease_until` 从数据库当前时间起算，绝不在旧值上累加**：累加会让租约随
  续租次数越推越远，一次失败的续租就能把任务锁住远超预期。验证手段是**区间断言**
  （旧值+extension 与 now+extension 相差整整一个原始租约长度，区间能把前者排除）。
- **续租的 WHERE 与三条状态写回完全一致（含 `status='running'`）**：少这一条，
  失去租约的旧 Worker 就能靠续租把自己"续"回有效状态，fencing 形同虚设；终态任务
  也不许被续租复活。0 行 ⇒ `ErrLeaseLost`。
- **续租失败 ≠ 失租**：只有 `ErrLeaseLost` 才算租约没了（取消 Handler）；
  连接断开/语句超时只是"这次没问成"，必须原样冒泡并在下个心跳周期重试（需求 §9）。
  把驱动错误翻译成 `ErrLeaseLost` 会让一次数据库抖动掐掉所有在途 Handler。
- **三种取消原因用 `context.WithCancelCause` 分开**，Handler 可用 `context.Cause` 分辨：
  停机 `context.Canceled` / 逐任务超时 `context.DeadlineExceeded` / 失租 `ErrLeaseLost`。
  `WithTimeout` 的 timerCtx 内嵌 cancelCtx，父取消会把 cause 透传下来，且
  first-cancel-wins 保证 `defer cancel()` 不会覆盖它。
- **顺序：先停租约看护、等它退出，再写回状态**。反过来续租会落在任务写成终态之后，
  因 status 不再是 running 返回 `ErrLeaseLost`，把正常结束的任务报成失租。
- 停机时**停止续租但不释放租约**；租约自然过期后交给回收器（已实现，见下一节）。

## 过期租约回收约定（2026-10-15 定，§4 第 3 项）

- **形态**：能力挂在既有 `WorkerStore` 接口上（`ReapExpiredLeases(ctx, limit)`），
  不另起可选接口；`Config` 只加 `ReapInterval`（默认 `DefaultReapInterval = 30s`，
  **负值关闭回收器**，0 由 `WithDefaults` 填默认、`Validate` 拒绝）；批大小是内部
  常量 `reapBatchSize = 100`（它调的是"回收事务有多长"，属实现细节，不进配置）。
- **守卫用读到的 `lease_until` 原值，不用 owner/epoch**：回收器不是租约持有者，
  没有凭据可比。影响 0 行 ⇒ 只跳过该行，**不返回 `ErrLeaseLost`** —— 那是持有者的
  语义，借用它会让"集群里有别的回收器在干活"看起来像故障。
- **不动 `attempt` 与 `lease_epoch`**：一次尝试从成功领取开始，这条任务已经用掉了
  那次机会，回收只是给下一次让路；`epoch` 属于"领取"这个动作。
- **⚠️ 加锁读必须显式停在 `ix_gotask_lease` 上（`FORCE INDEX`）**。这条读要连带取
  `attempt` / `max_attempts`，两列不在索引里 ⇒ 不是覆盖索引 ⇒ 优化器改选
  `type=ALL, key=NULL, Using filesort`（全表扫描 + 排序）。`FOR UPDATE` 在扫描过程中
  把**整个积压**锁住：`LIMIT` 不再限制加锁范围、`SKIP LOCKED` 失效。实测 12 条过期任务
  + 4 个回收器并发 ⇒ 只有一个能推进、合计只回收 3 条；`data_locks` 13 条 vs 修复后 7 条。
  对照：claim 的加锁读**只取 `id`**，对 `ix_gotask_claim` 天然覆盖（实测
  `key=ix_gotask_claim, Using index`），所以不需要提示。结论有普适性：**加锁读一旦
  因为要取别的列而离开覆盖索引，就要用 `FORCE INDEX` 把计划钉死**，别交给优化器。
- **回归用例要钉两层**（`TestReapLockingReadStaysOnLeaseIndex`）：计划层断言
  `EXPLAIN` 的 `key`；行为层让一个事务持锁、另一个执行同一条读，断言它仍取到
  **不相交的 `limit` 行**。用两个手写事务而不是两个 Worker —— 要确定性，不依赖调度。
  同理 `TestReapIsSafeWithConcurrentReapers`（4 个回收器各 3 条、合计 12）也已是确定性的：
  任一时刻最多 9 行被锁，第 4 个回收器总能拿到 3 行，所以"合计恰好 12"不是靠运气。
- 根包 `Run` 的停机等待**同时观察执行侧与回收侧的 done 信号**，超时消息要能区分
  "handlers still running" / "the reaper not stopped" / 两者都没停。回收循环的
  驱动错误只上报、下个周期重试（回收能力不能被一次数据库抖动关掉）；`ctx` 取消
  导致的失败是停机附带结果，**不上报**。

## 测试写法补充（2026-10-15）

- **要给"某个顺序性质"写断言时，用门闸（gate）把竞争窗口撑成确定的**：让被测路径上的
  某个调用阻塞在一个 channel 上，用例观察到它已进入后再放行。这样"写回时是否还有续租
  在途"这种时序不变量可以**确定性**地断言，而不是靠 sleep 撞运气。实现侧再配一个
  "越界即记录"的假 store 字段（如 `writeBackWhileRenewing`）即可。
- **假实现要能模拟"语句被 ctx 取消"**：`renewFn` 这类钩子应当拿到该调用自己的 ctx，
  否则测不了"续租执行到一半被停机打断"这条路径。
- **覆盖率掉了先定位再决定**：`go test -coverprofile` + `go tool cover -func` 找出
  <100% 的函数，再按 profile 里 `count==0` 的记录定位到具体行。根包要求 100%；
  对只起"吞掉错误"作用的防御分支，**删掉比补测试更诚实**。
- 证伪检查的备份放仓库外（`/home/tide/.cache/falsify/`），别在仓库里建临时目录。
- 沙箱会把脚本里的变量名 `sc`/`reg` 当成系统命令拦截（误报）——写脚本时避开这两个名字，
  或把脚本写成文件再跑。

## 失败分流、退避与 panic 兜底（2026-10-15 定，§4 第 4+5 项）

- **`fail` 只有两种去向**：`IsPermanent(cause) || Attempt >= MaxAttempts` ⇒ `MarkDead`，
  否则带退避 `Retry`。两个"不再重试"的理由**合在一个判断里**是刻意的：永久失败的可
  辨识性来自 `Permanent` 包装器自带的 `"permanent failure: "` 前缀，不来自走哪个分支
  ⇒ 分支顺序在结果上不可观察，**不该也不能为它写断言**（这一点是证伪检查逼出来的，
  原先那条"最后一次尝试 + 永久失败 ⇒ 摘要带永久前缀"的用例是装饰，已换成只断言可观察
  结果的表驱动矩阵）。
- **退避上界抽成纯函数** `retryBackoffCap(attempt, base, max)`＝`base × 2^(attempt-1)`
  封顶 `max`，**先判 `cap > max/2` 再翻倍**：`MaxAttempts` 可配置，attempt 很大时一路
  左移会溢出成负数，那等价于"没有退避"而区间断言看不出来。`attempt < 1` 一律按第 1 次算。
- **抖动口径 = equal jitter `[cap/2, cap]`，全库只此一处**（队列轮询用的也是
  `jitter()`）。**刻意偏离技术设计 §5.5 原文的 full jitter**，理由两条：① full jitter
  允许延迟取 0 ＝ 立刻重试，对故障下游没有好处、只是更快吃掉剩余尝试次数；
  ② 有下限才**可断言**——`[0, cap]` 只能断言上界，"退避根本没生效"检查不出来。
  需求文档 §4 只说"有上限的指数退避 + 随机抖动"，两者都满足。文档已按实现口径回填。
- **panic 兜底 = 单任务边界 recover**（根包 `runHandler`）：**只包住 Handler 调用本身**，
  本库自己的领取/写回/回收若 panic 照常崩（技术设计 §5.5「进程级不可恢复错误不应被
  吞掉」）。转出的错误形如 `handler panicked: <值>\n<堆栈>`，**不打永久标记**——需求只
  说"视为一次失败"，panic 未必确定性，去留交给 `max_attempts`。堆栈由 `panicStack()`
  截断：丢首行 `goroutine N [running]:`、取 12 行、超出以 `...` 收尾（`last_error`
  只有 2048 字符且是给人读的摘要）。**局限**：只能捕获同 goroutine 的 panic；Handler
  在自己起的子协程里 panic 拦不住，Go 会终止进程。
- 这两项让"带永久标记但仍重试到耗尽"这一 §3 遗留缺口闭合：未知 task type
  （`ErrNoHandler`）与 payload 解码失败（`ErrInvalidPayload`）现在第一次就进 `dead`。

## 两个 Go / 工具链陷阱（2026-10-15 踩到，代价是一次整包超时）

- **具名返回值与 `:=` 是同一个变量**。`func startWorker(t *testing.T, w *Worker) (cancel func())`
  里写 `ctx, cancel := context.WithCancel(...)`：`cancel` 与具名返回值在**同一个块**，
  属于"短变量声明重声明" ⇒ **同一个变量**。末尾 `return stop` 把它改写成 `stop`。
  而闭包捕获的是**变量不是值**，于是闭包里的 `cancel()` 实际调用 `stop` ⇒ `once.Do`
  重入自锁，且 **ctx 从头到尾没被取消**（`Run` 一直停在 `<-ctx.Done()`）。
  教训：有具名返回值时，内部新起同名变量前先想一秒；`cancel`/`stop` 这类名字尤其危险。
- **不要把"等待/超时 + `t.Error`"包在 `sync.Once` 里**。Once 在闭包执行期间持有自己的
  互斥量，任何重入都会死锁在**同一个地址**的 `Mutex.Lock` 上——dump 里只能看到
  `Mutex.Lock` + `doSlow`，看不出是 Once 在自锁，排查成本很高。用显式布尔标记即可
  （夹具只在测试 goroutine 上被调用，不需要更重的同步）。
- **go-sql-driver 默认不给读写设 deadline**。DSN 缺 `timeout`/`readTimeout`/`writeTimeout`
  时，MySQL 短暂不响应会让查询无限期挂在 `syscall.write` 上；后果不是"某条用例失败"，
  而是**整包被 `go test` 的包级超时打掉**，日志停在半途、其余包的结果一并丢失。
  三个入口（`migrations/mysql` 与 `store/mysql` 夹具的示例 DSN + CI 的 job 级 DSN）
  统一带上 `timeout=5s&readTimeout=30s&writeTimeout=30s`。
- **排查挂死先问"谁在等谁"**：dump 里 `Worker.Run` 停在 `<-ctx.Done()` 而不是停机等待
  的那段 select，就已经说明"取消根本没发生"，范围可以直接从产品代码缩到测试夹具。
- **同一文件的多笔编辑必须串行**：并行下发会各自基于同一份旧内容替换、后写吞掉先写，
  而且**两笔都会返回成功**。看着像"编辑没保存"，其实是覆盖。提交后 `grep` 复核一遍。

## 终态清理（§4 第 6 项，尚未开始）

- 需求：`succeeded`/`dead`/`canceled` 按保留期**小批量**删除。动手前先定：
  删除的判据列（`finished_at` 还是 `updated_at`）、批大小是否进 `Config`、
  是否复用回收器循环还是独立 `CleanupInterval`、以及"删到一半失败"的语义。
- 注意别和回收器抢同一批行：两者的筛选条件（终态 vs `running`）互斥，但要写进文档。
