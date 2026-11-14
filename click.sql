CREATE TABLE `click` (
    `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
    `surl` VARCHAR(11) NOT NULL COMMENT '短链标识',
    `ts` BIGINT NOT NULL COMMENT '点击时间，Unix 毫秒时间戳',
    `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '入库时间',
    `event_id` CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT '点击事件唯一标识',
    PRIMARY KEY (`id`),
    UNIQUE KEY `idx_unique_event` (`event_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='短链点击记录';
