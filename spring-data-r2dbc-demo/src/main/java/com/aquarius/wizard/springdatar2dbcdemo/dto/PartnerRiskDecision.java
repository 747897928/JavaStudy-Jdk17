package com.aquarius.wizard.springdatar2dbcdemo.dto;

import java.math.BigDecimal;

public record PartnerRiskDecision(
        String riskLevel,
        BigDecimal shippingFee,
        boolean reviewRequired
) {
}
