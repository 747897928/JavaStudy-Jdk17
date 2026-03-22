package com.aquarius.wizard.springdatar2dbcdemo.dto;

import java.math.BigDecimal;

/**
 * 下游风控/运费服务返回值。
 */
public record PartnerRiskDecision(
        String riskLevel,
        BigDecimal shippingFee,
        boolean reviewRequired
) {
}
