# GoRangeTask 项目约定（长期）

> 跨会话有效的项目级事实与约定。日常流水记在 `YYYY-MM-DD.md`，这里只留稳定的东西。
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
  **同时实现根包 `WorkerStore`**（Claim / Succeed / Retry / MarkDead，
  `store/mysql/worker.go`，有第二个编译期断言）。租约续期、回收器、清理还没做。
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
  破坏性的（DROP TABLE + 重新 Migrate），共用一个库会互相删表、报出与产品无关的假失败。
  做法：用 `gomysql.ParseDSN` 取库名后加后缀（如 `_store`），再用不带库名的连接
  `CREATE DATABASE IF NOT EXISTS`。`migrations/mysql` 占 `gotask_test`，
  `store/mysql` 占 `gotask_test_store`。
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
- 库：`gotask`（开发）、`gotask_test`（迁移集成测试）、`gotask_test_store`（store 集成测试，
  自动创建）、`demo_business`/`gotask_test_business`（事务入队测试的业务表，用完即删）。

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
- ⚠️ 直接后果：停机遗留的 `running` 任务**只能靠回收器回收**，而 reaper 属 §4、
  目前不存在 ⇒ 这类任务现在会一直挂着。§4 必须补上。

## 测试约定补两条（2026-10-13）

- **真库断言可能因为"错误的原因"通过**：例如停机用例里，即便代码错误地调用
  `Retry`，它用的是已取消的 ctx，UPDATE 自己就失败了，任务照样留在 running。
  ⇒ 涉及"某个写回**没有**发生"的断言，必须**同时**在根包用内存 store 断言
  "对应方法根本没被调用"，光看库侧终态不够。
- **新写的断言要做「证伪检查」**：把实现里那条关键分支临时破坏（如 `&&` 短路成
  `false &&`），跑一遍确认用例**真的会失败**，再还原并 `diff` 确认与备份一致。
  不会失败的断言只是装饰。
