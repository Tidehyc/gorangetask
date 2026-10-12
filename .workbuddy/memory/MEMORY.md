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
- `migrations/mysql`（package `mysqlmigrations`）：**独立包，不并入 store**。
  理由：`//go:embed` 不能跨目录向上取文件；迁移是「建表」的完整闭环且已有独立测试。
  调用顺序是「先 `mysqlmigrations.Migrate`，再 `mysqlstore.New`」。
- `config.go` 里 **`Config`（Worker 运行配置）与 `ClientConfig`（入队侧配置）刻意分开**：
  只想入队的进程不必构造 16 个 Worker 字段。

## 测试约定

- 真 MySQL 集成测试由环境变量 `GOTASK_TEST_MYSQL_DSN` 开启，未设置则 `t.Skip`，
  所以 CI（不设该变量）只跑单测。
  本地示例：`root:rootpw@tcp(127.0.0.1:3306)/gotask_test?parseTime=true&loc=UTC`
- **每个集成测试包必须用独立的库**：`go test ./...` 会并发跑不同包，而集成测试 setup 是
  破坏性的（DROP TABLE + 重新 Migrate），共用一个库会互相删表、报出与产品无关的假失败。
  做法：用 `gomysql.ParseDSN` 取库名后加后缀（如 `_store`），再用不带库名的连接
  `CREATE DATABASE IF NOT EXISTS`。`migrations/mysql` 占 `gotask_test`，
  `store/mysql` 占 `gotask_test_store`。
- 沙箱 `/tmp` 只有 10MB：跑 `go test`（尤其 `-race`）前必须
  `export TMPDIR=/home/tide/.cache/go-tmp GOTMPDIR=/home/tide/.cache/go-tmp`，
  否则 `no space left on device`。
- 时间断言一律以**数据库时钟**为准（`SELECT UTC_TIMESTAMP(6)`），不用测试机时钟。
- 仓库内不要建临时目录实验：沙箱的 `rm` 是 safe-delete 垫片，会把文件挪进
  `.Trash-0/` 污染 `git status` 与 `gofmt -l`。用 `/bin/rm`，或把实验放到仓库外
  （如 `/home/tide/.cache/`）。`.gitignore` 已加 `.Trash-*/` 兜底。

## 本地开发库

- 容器 `gotask-mysql`（mysql:8.0，卷 `gotask-mysql-data`，`127.0.0.1:3306`，UTC，
  READ-COMMITTED），用 `scripts/dev-mysql.sh up|down|reset|status|shell` 管理。
  宿主机没有 mysql-server，环境不被污染。
- 库：`gotask`（开发）、`gotask_test`（迁移集成测试）、`gotask_test_store`（store 集成测试，
  自动创建）、`demo_business`/`gotask_test_business`（事务入队测试的业务表，用完即删）。

## 数据模型要点

- 表 `gotask_tasks`；`status` 用 VARCHAR 存五种状态，不用 MySQL ENUM。
- 唯一索引 `uq_gotask_queue_idempotency (queue_name, idempotency_key)`：
  **NULL 不参与唯一性判断** ⇒ 空幂等键必须写 NULL，写空串会被误判为重复。
- 时间列一律 UTC；参与时序判断的值以数据库为准（SQL 里用 `UTC_TIMESTAMP(6)`）。
- 领取用 `FOR UPDATE SKIP LOCKED`（尚未实现）。
