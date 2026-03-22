package com.aquarius.wizard.springdatar2dbcdemo.service;

import com.aquarius.wizard.springdatar2dbcdemo.client.PartnerRiskClient;
import com.aquarius.wizard.springdatar2dbcdemo.dto.CreateOrderRequest;
import com.aquarius.wizard.springdatar2dbcdemo.dto.OrderResponse;
import com.aquarius.wizard.springdatar2dbcdemo.dto.PartnerRiskDecision;
import com.aquarius.wizard.springdatar2dbcdemo.entity.PurchaseOrderEntity;
import com.aquarius.wizard.springdatar2dbcdemo.repository.PurchaseOrderWriteRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OrderCommandServiceTest {

    private PurchaseOrderWriteRepository writeRepository;
    private PartnerRiskClient partnerRiskClient;
    private TransactionalOperator transactionalOperator;
    private OrderCommandService orderCommandService;

    @BeforeEach
    void setUp() {
        writeRepository = mock(PurchaseOrderWriteRepository.class);
        partnerRiskClient = mock(PartnerRiskClient.class);
        transactionalOperator = mock(TransactionalOperator.class);

        when(transactionalOperator.transactional(org.mockito.ArgumentMatchers.<Mono<OrderResponse>>any()))
                .thenAnswer(invocation -> invocation.getArgument(0));

        orderCommandService = new OrderCommandService(
                writeRepository,
                partnerRiskClient,
                new OrderResponseMapper(),
                transactionalOperator
        );
    }

    @Test
    void shouldCreateReactiveOrderWithoutBlockingAdapters() {
        CreateOrderRequest request = new CreateOrderRequest(
                "Cathy",
                "gold",
                "pg-ha-case",
                3,
                BigDecimal.valueOf(199),
                "learning r2dbc"
        );

        when(partnerRiskClient.evaluate(anyString(), any(BigDecimal.class)))
                .thenReturn(Mono.just(new PartnerRiskDecision("MEDIUM", BigDecimal.valueOf(12), false)));

        when(writeRepository.save(any(PurchaseOrderEntity.class)))
                .thenAnswer(invocation -> {
                    PurchaseOrderEntity entity = invocation.getArgument(0);
                    entity.setId(1L);
                    return Mono.just(entity);
                });

        StepVerifier.create(orderCommandService.createOrder(request))
                .assertNext(response -> {
                    assertThat(response.orderNo()).startsWith("PO-");
                    assertThat(response.customerTier()).isEqualTo("GOLD");
                    assertThat(response.status()).isEqualTo("CREATED");
                    assertThat(response.shippingFee()).isEqualByComparingTo("12.00");
                    assertThat(response.orderAmount()).isEqualByComparingTo("609.00");
                })
                .verifyComplete();

        ArgumentCaptor<PurchaseOrderEntity> captor = ArgumentCaptor.forClass(PurchaseOrderEntity.class);
        verify(writeRepository).save(captor.capture());
        assertThat(captor.getValue().getSkuCode()).isEqualTo("PG-HA-CASE");
        assertThat(captor.getValue().getRiskLevel()).isEqualTo("MEDIUM");
    }
}
