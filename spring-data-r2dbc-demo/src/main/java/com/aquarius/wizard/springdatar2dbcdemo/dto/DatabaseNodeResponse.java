package com.aquarius.wizard.springdatar2dbcdemo.dto;

/**
 * 单个数据库节点的角色快照。
 */
public record DatabaseNodeResponse(
        String role,
        String host,
        Integer port,
        String databaseName,
        boolean inRecovery,
        String transactionReadOnly
) {
}
