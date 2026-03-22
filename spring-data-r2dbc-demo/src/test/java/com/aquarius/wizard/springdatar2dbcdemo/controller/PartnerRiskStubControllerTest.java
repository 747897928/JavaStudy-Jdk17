package com.aquarius.wizard.springdatar2dbcdemo.controller;

import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

class PartnerRiskStubControllerTest {

    private final PartnerRiskStubController controller = new PartnerRiskStubController();

    @Test
    void shouldReturnFreeShippingForPlatinumAndHighRiskForLargeOrders() {
        StepVerifier.create(controller.assess("platinum", BigDecimal.valueOf(1200)))
                .assertNext(decision -> {
                    assertThat(decision.riskLevel()).isEqualTo("HIGH");
                    assertThat(decision.reviewRequired()).isTrue();
                    assertThat(decision.shippingFee()).isEqualByComparingTo("0.00");
                })
                .verifyComplete();
    }
}
