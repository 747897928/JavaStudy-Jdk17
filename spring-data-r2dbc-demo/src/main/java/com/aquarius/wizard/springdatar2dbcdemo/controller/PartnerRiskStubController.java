package com.aquarius.wizard.springdatar2dbcdemo.controller;

import com.aquarius.wizard.springdatar2dbcdemo.dto.PartnerRiskDecision;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.util.Locale;

/**
 * 本地模拟下游服务。
 * <p>
 * 真实项目里这里通常是外部服务，这里为了让 Demo 自包含，直接在同一个应用里做了一个 stub。
 */
@RestController
@RequestMapping("/partner-api")
public class PartnerRiskStubController {

    private static final String RISK_HIGH = "HIGH";
    private static final String RISK_MEDIUM = "MEDIUM";
    private static final String RISK_LOW = "LOW";
    private static final String TIER_PLATINUM = "PLATINUM";
    private static final String TIER_GOLD = "GOLD";

    /**
     * 根据客户等级和订单金额，返回一个简化版风控结果。
     */
    @GetMapping("/risk-assessments")
    public Mono<PartnerRiskDecision> assess(
            @RequestParam String customerTier,
            @RequestParam BigDecimal orderAmount
    ) {
        return Mono.delay(Duration.ofMillis(120))
                .map(ignore -> {
                    String normalizedTier = customerTier.trim().toUpperCase(Locale.ROOT);
                    BigDecimal shippingFee = calculateShippingFee(normalizedTier, orderAmount);
                    String riskLevel = determineRiskLevel(orderAmount);
                    boolean reviewRequired = RISK_HIGH.equals(riskLevel);
                    return new PartnerRiskDecision(riskLevel, shippingFee, reviewRequired);
                });
    }

    /**
     * 计算运费。
     * <p>
     * 这里只是一个可读性优先的演示规则，不是业务最佳实践。
     */
    private BigDecimal calculateShippingFee(String customerTier, BigDecimal orderAmount) {
        BigDecimal baseFee = orderAmount.compareTo(BigDecimal.valueOf(500)) >= 0
                ? BigDecimal.valueOf(12)
                : BigDecimal.valueOf(24);

        if (TIER_PLATINUM.equals(customerTier)) {
            return BigDecimal.ZERO.setScale(2, RoundingMode.HALF_UP);
        }
        if (TIER_GOLD.equals(customerTier)) {
            return baseFee.multiply(BigDecimal.valueOf(0.5)).setScale(2, RoundingMode.HALF_UP);
        }
        return baseFee.setScale(2, RoundingMode.HALF_UP);
    }

    /**
     * 计算风控等级。
     */
    private String determineRiskLevel(BigDecimal orderAmount) {
        if (orderAmount.compareTo(BigDecimal.valueOf(1000)) >= 0) {
            return RISK_HIGH;
        }
        if (orderAmount.compareTo(BigDecimal.valueOf(300)) >= 0) {
            return RISK_MEDIUM;
        }
        return RISK_LOW;
    }
}
