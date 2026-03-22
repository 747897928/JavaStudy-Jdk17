package com.aquarius.wizard.springdatar2dbcdemo.dto;

import java.time.Instant;

/**
 * 统一异常响应。
 */
public record ErrorResponse(
        Instant timestamp,
        int status,
        String error,
        String message,
        String path
) {
}
