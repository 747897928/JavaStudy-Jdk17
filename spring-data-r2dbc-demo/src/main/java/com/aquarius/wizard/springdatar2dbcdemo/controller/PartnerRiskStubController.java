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

@RestController
@RequestMapping("/partner-api")
public class PartnerRiskStubController {

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
                    boolean reviewRequired = "HIGH".equals(riskLevel);
                    return new PartnerRiskDecision(riskLevel, shippingFee, reviewRequired);
                });
    }

    private BigDecimal calculateShippingFee(String customerTier, BigDecimal orderAmount) {
        BigDecimal baseFee = orderAmount.compareTo(BigDecimal.valueOf(500)) >= 0
                ? BigDecimal.valueOf(12)
                : BigDecimal.valueOf(24);

        if ("PLATINUM".equals(customerTier)) {
            return BigDecimal.ZERO.setScale(2, RoundingMode.HALF_UP);
        }
        if ("GOLD".equals(customerTier)) {
            return baseFee.multiply(BigDecimal.valueOf(0.5)).setScale(2, RoundingMode.HALF_UP);
        }
        return baseFee.setScale(2, RoundingMode.HALF_UP);
    }

    private String determineRiskLevel(BigDecimal orderAmount) {
        if (orderAmount.compareTo(BigDecimal.valueOf(1000)) >= 0) {
            return "HIGH";
        }
        if (orderAmount.compareTo(BigDecimal.valueOf(300)) >= 0) {
            return "MEDIUM";
        }
        return "LOW";
    }
}
