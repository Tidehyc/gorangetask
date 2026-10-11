// Package gorangetask 提供一个可嵌入 Go 服务的 MySQL 事务型持久化任务队列。
//
// 调用方可以在自己的业务事务中写入业务数据与任务记录，事务提交后由 Worker
// 从数据库任务表领取任务，并直接执行本进程已注册的 Go Handler。任务表本身
// 即为持久化投递通道，不依赖 RabbitMQ 等外部 Broker。
//
// 执行语义为「至少一次」：Handler 可能被重复调用，业务副作用必须幂等。
// 本包只保证任务记录被持久化并至少一次交给 Handler 尝试执行，不保证外部
// 副作用恰好发生一次。
package gorangetask
