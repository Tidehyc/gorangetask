-- 0001: 创建 GoRangeTask 任务表。
--
-- 对应技术设计 §3.1 的 DDL。相对草案的唯一改动：显式声明字符集与排序规则，
-- 不依赖服务器默认值，保证不同环境建出的表结构一致。
--
-- 索引说明：
--   uq_gotask_queue_idempotency  幂等键唯一约束（幂等键为 NULL 时不受约束）
--   ix_gotask_claim              领取查询：queue_name + status + run_at 排序
--   ix_gotask_lease              租约回收：status + lease_until
--   ix_gotask_cleanup            终态清理：status + finished_at

CREATE TABLE gotask_tasks (
    id                BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    queue_name        VARCHAR(64)     NOT NULL,
    task_type         VARCHAR(128)    NOT NULL,
    payload           JSON            NOT NULL,
    status            VARCHAR(16)     NOT NULL,
    run_at            DATETIME(6)     NOT NULL,
    attempt           INT UNSIGNED    NOT NULL DEFAULT 0,
    max_attempts      INT UNSIGNED    NOT NULL DEFAULT 10,
    lease_owner       VARCHAR(128)    NULL,
    lease_until       DATETIME(6)     NULL,
    lease_epoch       BIGINT UNSIGNED NOT NULL DEFAULT 0,
    idempotency_key   VARCHAR(191)    NULL,
    last_error        VARCHAR(2048)   NULL,
    created_at        DATETIME(6)     NOT NULL,
    updated_at        DATETIME(6)     NOT NULL,
    finished_at       DATETIME(6)     NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uq_gotask_queue_idempotency (queue_name, idempotency_key),
    KEY ix_gotask_claim   (queue_name, status, run_at, id),
    KEY ix_gotask_lease   (status, lease_until, id),
    KEY ix_gotask_cleanup (status, finished_at, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
