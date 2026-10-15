# GoRangeTask 项目约定（长期·精简版）

> 本文件每轮自动注入，故只留红线。**详细版（设计取舍、测试写法、踩坑全过程）在
> `.workbuddy/memory/MEMORY-详细约定.md`，动手前先读它**；日常流水见同目录 `YYYY-MM-DD.md`。
> 用户级偏好（GitHub 账号、PR 复盘方式）在 `~/.workbuddy/MEMORY.md`。

## 身份与协作方式

- GoRangeTask：可嵌入 Go 服务的 MySQL 持久化任务队列；卖点＝**业务数据与任务行同一事务提交**。
  module `github.com/Tidehyc/gorangetask`，Go 1.24+，MIT，MySQL 8.0+/InnoDB，核心只用 `database/sql`。
- 开工前先确认范围，拿不准先讨论，不擅自决定；**按需求粒度提交**，不攒大提交。
- 沙箱推不动 GitHub（登录态在用户账号下）⇒ 提交后请用户自己 push。提交前先
  `git status --short`；**不要** `git add -A`（工作区可能有别人或另一会话的在制改动）。
- 文档只是指导，别把实现细节事无巨细写进文档。

## 包结构

- 根包 `gorangetask`：只放与数据库无关的公开契约（Client/Worker/WorkerStore/Config/errors），**不写 SQL**。
- `store/mysql`：唯一写 SQL 处，**只读表不建表**；已实现 `Client` 与 `WorkerStore` + 编译期断言。
- `migrations/mysql`：独立包（`//go:embed` 不能跨目录）。调用顺序＝先 `Migrate` 再 `mysqlstore.New`。
- `Config`（Worker 侧）与 `ClientConfig`（入队侧）刻意分开。

## 本地库与测试环境

- 容器 `gotask-mysql`（mysql:8.0，卷 `gotask-mysql-data`，UTC，READ-COMMITTED），用
  `scripts/dev-mysql.sh up|down|reset|status|shell` 管理。跑测试前先 `status`，`Exited` 就先 `up`。
  **不要自己在宿主机起 mysqld**（AppArmor 限制 datadir）。
- 集成测试由 `GOTASK_TEST_MYSQL_DSN` 开启，未设则 `t.Skip`。**DSN 统一指向 `gotask`**（唯一
  的库，开发与集成测试共用，`up` 已自动建）；`migrations/mysql` 直接用该库、`store/mysql` 用
  自动派生的 `gotask_store`。历史遗留的 `gotask_test` 已废弃。
- **每个集成测试包占独立库**：`go test ./...` 并发跑包，而 setup 是破坏性的（DROP + 重新 Migrate）。
  约定＝`ParseDSN` 取库名加后缀（如 `_store`），**由测试自己 `CREATE DATABASE IF NOT EXISTS`**。
- ⚠️ `migrations/mysql` 报 `Error 1049 Unknown database` **不是迁移失败**：`sql.Open` 是惰性的，
  报错来自随后的 `PingContext`，此时一条迁移语句都没跑。
- `/tmp` 只有 10MB：跑 `go test`（尤其 `-race`）前
  `export TMPDIR=/home/tide/.cache/go-tmp GOTMPDIR=/home/tide/.cache/go-tmp`。
- CI 已接 MySQL service；**守门步骤：出现任何 `--- SKIP` 即判失败**（`go test` 退出码不反映 skip）。
  新增「合法跳过」用例前，先想清楚会不会误伤它。
- 宿主负载高时集成测试慢 8–10 倍、前台跑会被 SIGKILL ⇒ 长测试一律后台跑；
  判据是 **0 FAIL / 0 SKIP**，不是耗时。

## 最容易踩的红线

- 时间断言一律以**数据库时钟**为准（`SELECT UTC_TIMESTAMP(6)`）；**绝不把「未来/过去」时刻
  写死进测试数据**——过了那天会静默退化成另一类用例并报假失败。
- 断言某事**没有**发生，必须同时在能观察调用行为的层（fake/spy）断言「该方法没被调用」，
  只看持久层终态不够——终态可能因为「错误的原因」而正确。
- 新断言必须做**证伪检查**：临时破坏实现 ⇒ 确认用例真的失败 ⇒ 还原并 `diff` 逐字节一致。
- 别用 `time.Sleep` 卡超时用例（用 `select` + `ctx.Done()`）；要断言顺序性质就用**门闸**
  把竞争窗口撑成确定的。
- 并发用例除进程侧记录外，还要用一个**库侧单调量**独立复核（如 `lease_epoch` 全为 1）。
- 仓库内不建临时目录（沙箱 `rm` 是 safe-delete 垫片，会污染 git status）；用 `/bin/rm`，
  或把实验放到 `/home/tide/.cache/`。
- 沙箱会把脚本里的变量名 `sc`/`reg` 当成系统命令拦截（误报），写脚本时避开。
