# GoRangeTask（gorangetask）项目进度跟踪

> 用途：对照需求和技术设计追踪项目完成情况；每项完成后补充提交、测试日志、基准结果或文档链接作为证据。
> 当前状态：设计阶段；尚未开始实现。文档已定稿为开工基线，本地尚未 `git init`。
> 更新日期：2026-10-11
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
| [ ] | 创建仓库并初始化 Go module | 本地 `git init`、README、LICENSE、go.mod、.gitignore 可运行 | 待执行（下一步） |
| [ ] | 创建远程仓库并首次推送 | GitHub 上 `Tidehyc/gorangetask` 存在且 main 已推送 | 待本地初始化后再建远程 |

## 1. 项目骨架与开发约定

| 状态 | 工作项 | 完成标准 | 证据/备注 |
|---|---|---|---|
| [ ] | 初始化模块与包结构 | core、MySQL store、migration、example 结构建立 | 待实现 |
| [ ] | 定义公开错误与 API 类型 | Task、TaskID、TaskInfo、Handler、Client、配置、错误语义有文档 | 待实现 |
| [x] | 建立本地 MySQL 开发环境 | 一条命令启动 MySQL 8，migration 可应用/回滚 | 2026-10-11 容器化 MySQL 8.0.46（`gotask-mysql`）已跑通；`scripts/dev-mysql.sh up` 启停；宿主机未装 mysql-server，环境不被污染 |
| [ ] | 建立 CI 基线 | format、vet、unit test、MySQL integration test 可在 CI 执行 | 待实现 |

## 2. 持久化与事务入队

| 状态 | 工作项 | 完成标准 | 证据/备注 |
|---|---|---|---|
| [ ] | 编写版本化 migration | 任务表、索引和版本管理完成；重复运行行为明确 | 待实现 |
| [ ] | 实现普通 `Enqueue` | 立即/延迟任务能持久化，参数校验和 payload 上限生效 | 待实现 |
| [ ] | 实现 `EnqueueTx` | 调用方事务提交任务可见、回滚无任务；库不接管事务生命周期 | 待实现 |
| [ ] | 实现幂等键 | 并发同 key 仅一条任务创建，重复返回 `ErrDuplicateTask` | 待实现 |
| [ ] | 实现状态读取和 pending 取消 | 查询结果稳定；只允许取消 pending | 待实现 |

## 3. Worker 执行闭环

| 状态 | 工作项 | 完成标准 | 证据/备注 |
|---|---|---|---|
| [ ] | Handler 注册与 payload 解码 | task type 映射、未知类型处理、schema 错误路径明确 | 待实现 |
| [ ] | 单 Worker claim/execute/ack | handler 在 claim 事务提交后运行；成功写回终态 | 待实现 |
| [ ] | 多 Worker 并发领取 | MySQL `SKIP LOCKED` 集成测试证明并发无双重有效租约 | 待实现 |
| [ ] | Worker graceful shutdown | 停止领取、通知运行 handler cancel、在超时内等待退出 | 待实现 |

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
