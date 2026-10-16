-- 示例业务表：订单。
--
-- 这张表属于**示例**，不属于 GoRangeTask 库：库只管理自己的任务表
-- （gotask_tasks，由 migrations/mysql 创建），业务表由使用方自己负责建。
-- 示例刻意把它做得足够窄，好让读者一眼看清"业务行 + 任务行同事务"这件事。
--
-- 整个文件只有一条语句，因此示例直接把它整段交给驱动执行，不需要自己写
-- 分号切分（库的 migrations/mysql 里有完整实现，供多语句的 DDL 文件使用）。
CREATE TABLE IF NOT EXISTS example_orders (
    id           BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    customer     VARCHAR(128)    NOT NULL,
    amount_cents BIGINT UNSIGNED NOT NULL,
    -- pending   订单已写入、导出任务已投递
    -- exported  导出任务已把订单推进到终态
    status       VARCHAR(16)     NOT NULL,
    exported_at  DATETIME(6)     NULL,
    created_at   DATETIME(6)     NOT NULL,
    PRIMARY KEY (id)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci
