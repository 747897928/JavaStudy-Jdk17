package com.aquarius.wizard.springdatar2dbcdemo.dto;

public record DatabaseNodeResponse(
        String role,
        String host,
        Integer port,
        String databaseName,
        boolean inRecovery,
        String transactionReadOnly
) {
}
