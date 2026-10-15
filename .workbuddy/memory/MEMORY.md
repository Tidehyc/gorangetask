# GoRangeTask 项目约定（长期·精简版）

> 每轮自动注入，只留红线。**详细版（设计取舍、测试写法、踩坑全过程）在
> `.workbuddy/memory/MEMORY-详细约定.md`，动手前先读它**；流水见 `YYYY-MM-DD.md`。
> 用户级偏好见 `~/.workbuddy/MEMORY.md`。

## 身份与协作

- GoRangeTask：可嵌入 Go 服务的 MySQL 持久化任务队列；卖点＝**业务数据与任务行同一事务提交**。
  module `github.com/Tidehyc/gorangetask`，Go 1.24+，MIT，MySQL 8.0+/InnoDB，核心只用 `database/sql`。
- 开工前先确认范围，拿不准先讨论；**按需求粒度提交**，不攒大提交。
- 沙箱推不动 GitHub ⇒ 提交后请用户自己 push。提交前先 `git status --short`；**不要**
  `git add -A`（工作区可能有别人或另一会话的在制改动）。文档只作指导，别写太细。

## 包结构

- 根包 `gorangetask`：只放与数据库无关的公开契约（Client/Worker/WorkerStore/Config/errors），**不写 SQL**。
- `store/mysql`：唯一写 SQL 处，**只读表不建表**；已实现 `Client` 与 `WorkerStore` + 编译期断言。
- `migrations/mysql`：独立包（`//go:embed` 不能跨目录）。顺序＝先 `Migrate` 再 `mysqlstore.New`。
- `Config`（Worker 侧）与 `ClientConfig`（入队侧）刻意分开。

## 本地库与测试

- 容器 `gotask-mysql`（mysql:8.0，UTC），用 `scripts/dev-mysql.sh up|down|reset|status`。
  跑测试前先 `status`，`Exited` 就先 `up`；**别在宿主机起 mysqld**（AppArmor 限 datadir）。
- 集成测试由 `GOTASK_TEST_MYSQL_DSN` 开启，未设则 `t.Skip`。**统一一个 `gotask`**：
  `root:rootpw@tcp(127.0.0.1:3306)/gotask?parseTime=true&loc=UTC&timeout=5s&readTimeout=30s&writeTimeout=30s`；
  `store/mysql` 自动派生 `gotask_store`；**每个集成测试包占独立库**（并发跑包 + 破坏性 Migrate）；
  旧 `gotask_test` 已废弃。⚠️ 驱动**默认不给读写设 deadline** —— 缺了超时，MySQL 一停顿就
  无限期挂住，整包被 `go test` 包级超时打掉（只剩 dump，其余结果全丢）。
  ⚠️ `migrations/mysql` 报 `Error 1049` **不是迁移失败**（挂在 `PingContext`）。
- `/tmp` 只有 10MB ⇒ 跑 `go test`（尤其 `-race`）前
  `export TMPDIR=/home/tide/.cache/go-tmp GOTMPDIR=/home/tide/.cache/go-tmp`。
- CI 已接 MySQL service；**守门步骤：出现任何 `--- SKIP` 即判失败**（退出码不反映 skip）。
- 负载高时集成测试慢 8–10 倍、前台会被 SIGKILL ⇒ 长测试后台跑；判据 **0 FAIL / 0 SKIP**。

## 最容易踩的红线

- 时间断言以**数据库时钟**为准（`SELECT UTC_TIMESTAMP(6)`）；**绝不把「未来/过去」时刻写死
  进测试数据**——过了那天会静默退化成另一类用例并报假失败。
- 断言某事**没有**发生，必须同时在能观察调用行为的层（fake/spy）断言「该方法没被调用」；
  只看持久层终态不够——终态可能因为「错误的原因」而正确。
- 新断言必须做**证伪检查**：临时破坏实现 ⇒ 确认用例真的失败 ⇒ 还原并 `diff` 逐字节一致。
- 别用 `time.Sleep` 卡超时用例（用 `select`+`ctx.Done()`）；断言顺序性质就用**门闸**。
- 并发用例除进程侧记录外，还要用一个**库侧单调量**独立复核（如 `lease_epoch` 全为 1）。
- **加锁读必须停在预期二级索引上**，否则 `LIMIT` 不再限制加锁范围、`SKIP LOCKED` 失效。
  只取索引内列（覆盖索引，如 claim 只取 `id`）；必须多取就显式 `FORCE INDEX`（回收器即此类）。
  用 `EXPLAIN` 的 `key` ＋一次并发加锁读钉成回归用例——实测优化器会改选全表扫描 + filesort。
- 仓库内不建临时目录（沙箱 `rm` 是 safe-delete 垫片，污染 git status）。⚠️ `rm`/`unlink`
  （**含 `/bin/rm`**）可能 EBUSY 失败，删文件用 Python `os.remove` 或放到 `/home/tide/.cache/`。
- 沙箱会把脚本里的变量名 `sc`/`reg` 当系统命令拦截（误报），避开。
- **具名返回值陷阱**：`func f() (cancel func())` 里写 `ctx, cancel := ...`，与具名返回值
  是**同一个变量**（同块重声明），末尾 `return stop` 会改写它；而闭包捕获的是**变量**
  不是值 ⇒ 闭包里的 `cancel()` 实际调用的是 `stop`。有具名返回值时，内部新增同名变量
  前先想一秒。
- 测试夹具的「停机等待超时」**不要包在 `sync.Once` 里**：Once 持锁执行闭包，任何重入
  直接自锁，dump 里只能看到同地址的 `Mutex.Lock`、看不出是 Once。用显式布尔标记。
- **同一文件的多笔编辑必须串行**（一个消息里只放一笔）：并行下发会各自基于同一份旧内容
  替换、后写吞掉先写，而且**两笔都会报成功**。
