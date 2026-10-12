# GoRangeTask

GoRangeTask 是一个可嵌入 Go 服务的 **MySQL 事务型持久化任务队列**。

它把数据库中的任务表本身当作持久化投递通道：调用方可以在**自己的业务事务**中写入业务数据与任务记录，事务提交后由 Worker 从表中领取任务并直接执行本进程注册的 Go Handler。**不依赖 RabbitMQ** 等外部 Broker。

> 状态：**设计阶段，尚未实现代码**。仓库当前只包含需求、技术设计与进度文档。下面描述的是目标行为与验收计划，不是已实现结果。

## 解决什么问题

业务数据变更往往需要触发延迟处理、文件/媒体加工、索引构建或补偿工作。若先在数据库写入业务数据、再独立向队列投递任务，两步之间可能因进程退出或网络故障而不一致。

GoRangeTask 要保证两件事：

1. **原子创建**：业务事务提交时任务一定存在；事务回滚时任务一定不存在。
2. **可靠执行**：多 Worker 并发领取；Worker 崩溃后可恢复；失败按策略重试；租约失效的旧 Worker 不能覆盖新状态。

执行语义是**至少一次**：Handler 可能重复运行，业务副作用需要幂等。GoRangeTask 只保证任务记录最终被持久化并至少一次交给已注册的 Handler 尝试执行，**不保证外部副作用恰好一次**。

## 与 Outbox + RabbitMQ 的边界

GoRangeTask 处理的是**本服务要执行的 Job**（生产者与 Handler 同服务、共用数据库），而不是要发布给多个订阅者的**领域事件**。RabbitMQ 仍适用于跨服务/跨语言分发、广播、Broker 缓冲与独立订阅等场景；GoRangeTask 不做 RabbitMQ 的转发器，也不替代它。详见 [docs/GoRangeTask-STAR.md](docs/GoRangeTask-STAR.md)。

## 目标能力（MVP）

- 事务内入队：`EnqueueTx` 把任务写入调用方正在使用的事务，库不接管事务生命周期。
- 立即执行与延迟执行。
- 多 Worker 基于 MySQL InnoDB 行锁 + `SKIP LOCKED` 并发领取。
- 数据库时间基准的租约、续租与过期回收。
- 递增 `lease_epoch`（fencing token）拒绝旧执行者写回。
- 失败重试（full-jitter 指数退避）、永久失败、死信。
- 可选幂等键、任务状态查询、取消未领取任务。
- 优雅停机、结构化日志与基础指标。

## 技术基线

| 项 | 选定值 |
|---|---|
| 语言 | Go（最低 **1.24**） |
| 数据库 | MySQL **8.0+** / InnoDB（MVP 仅验证 MySQL） |
| SQL 层 | 核心包基于 `database/sql`，**不依赖 GORM**；另提供可选官方 GORM adapter 子包 |
| 许可 | MIT |
| 模块路径 | `github.com/Tidehyc/gorangetask` |

## 本地开发环境

仓库自带 `scripts/dev-mysql.sh`，用容器化 MySQL 8.0 做本地开发与集成测试，不污染宿主机：

```bash
./scripts/dev-mysql.sh up      # 启动（首次会按固定参数创建容器）
./scripts/dev-mysql.sh status  # 查看状态
./scripts/dev-mysql.sh shell   # 进入 mysql 客户端
./scripts/dev-mysql.sh down    # 停止（保留数据卷）
./scripts/dev-mysql.sh reset   # 删除容器与数据卷，彻底重来
```

默认连接信息：`127.0.0.1:3306`，库 `gotask`，用户 `root`，密码 `rootpw`，UTC 时区，`READ-COMMITTED` 隔离级别。

## 文档

- [需求文档](docs/GoRangeTask-需求文档.md)：目标、非目标、P0/P1 功能需求、状态机、数据存储、API 与配置草案、可观测性与验收标准。
- [技术设计](docs/GoRangeTask-技术设计.md)：数据模型 DDL、领取/续租/确认/回收/清理 SQL、Go API 草案、包结构与验证计划。
- [迁移机制](docs/GoRangeTask-迁移机制.md)：数据库迁移的原理、MySQL DDL 不可回滚与脏状态、advisory lock、多语句处理及实测验证记录。
- [进度跟踪](docs/GoRangeTask-进度跟踪.md)：对照需求的完成状态与证据。
- [STAR 说明](docs/GoRangeTask-STAR.md)：项目背景、与 Outbox/RabbitMQ 的边界、简历证据框架。

## 许可

[MIT](LICENSE)
