package com.aquarius.wizard.springdatar2dbcdemo.dto;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * 订单接口响应。
 */
public record OrderResponse(
        String orderNo,
        String customerName,
        String customerTier,
        String skuCode,
        int quantity,
        BigDecimal unitPrice,
        BigDecimal shippingFee,
        BigDecimal orderAmount,
        String riskLevel,
        String status,
        String remark,
        Instant createdAt,
        Instant updatedAt
) {
}
