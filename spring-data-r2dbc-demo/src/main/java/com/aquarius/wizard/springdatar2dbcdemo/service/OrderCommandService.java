package com.aquarius.wizard.springdatar2dbcdemo.service;

import com.aquarius.wizard.springdatar2dbcdemo.client.PartnerRiskClient;
import com.aquarius.wizard.springdatar2dbcdemo.dto.CreateOrderRequest;
import com.aquarius.wizard.springdatar2dbcdemo.dto.OrderResponse;
import com.aquarius.wizard.springdatar2dbcdemo.dto.PartnerRiskDecision;
import com.aquarius.wizard.springdatar2dbcdemo.entity.OrderStatus;
import com.aquarius.wizard.springdatar2dbcdemo.entity.PurchaseOrderEntity;
import com.aquarius.wizard.springdatar2dbcdemo.repository.PurchaseOrderWriteRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.UUID;

@Service
public class OrderCommandService {

    private static final DateTimeFormatter ORDER_NO_TIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyyMMddHHmmss").withZone(ZoneOffset.UTC);

    private final PurchaseOrderWriteRepository writeRepository;
    private final PartnerRiskClient partnerRiskClient;
    private final OrderResponseMapper responseMapper;
    private final TransactionalOperator writerTransactionalOperator;

    public OrderCommandService(
            PurchaseOrderWriteRepository writeRepository,
            PartnerRiskClient partnerRiskClient,
            OrderResponseMapper responseMapper,
            TransactionalOperator writerTransactionalOperator
    ) {
        this.writeRepository = writeRepository;
        this.partnerRiskClient = partnerRiskClient;
        this.responseMapper = responseMapper;
        this.writerTransactionalOperator = writerTransactionalOperator;
    }

    public Mono<OrderResponse> createOrder(CreateOrderRequest request) {
        return Mono.defer(() -> {
                    BigDecimal normalizedUnitPrice = request.unitPrice().setScale(2, RoundingMode.HALF_UP);
                    BigDecimal orderAmount = normalizedUnitPrice
                            .multiply(BigDecimal.valueOf(request.quantity()))
                            .setScale(2, RoundingMode.HALF_UP);

                    return partnerRiskClient.evaluate(request.customerTier(), orderAmount)
                            .map(decision -> buildEntity(request, normalizedUnitPrice, decision));
                })
                .flatMap(writeRepository::save)
                .map(responseMapper::toResponse)
                .as(writerTransactionalOperator::transactional);
    }

    private PurchaseOrderEntity buildEntity(
            CreateOrderRequest request,
            BigDecimal unitPrice,
            PartnerRiskDecision decision
    ) {
        Instant now = Instant.now();
        PurchaseOrderEntity entity = new PurchaseOrderEntity();
        entity.setOrderNo(generateOrderNo());
        entity.setCustomerName(request.customerName().trim());
        entity.setCustomerTier(request.customerTier().trim().toUpperCase(Locale.ROOT));
        entity.setSkuCode(request.skuCode().trim().toUpperCase(Locale.ROOT));
        entity.setQuantity(request.quantity());
        entity.setUnitPrice(unitPrice);
        entity.setShippingFee(decision.shippingFee().setScale(2, RoundingMode.HALF_UP));
        entity.setRiskLevel(decision.riskLevel());
        entity.setStatus(decision.reviewRequired() ? OrderStatus.PENDING_REVIEW.name() : OrderStatus.CREATED.name());
        entity.setRemark(request.remark());
        entity.setCreatedAt(now);
        entity.setUpdatedAt(now);
        return entity;
    }

    private String generateOrderNo() {
        String timestamp = ORDER_NO_TIME_FORMATTER.format(Instant.now());
        String randomPart = UUID.randomUUID().toString().substring(0, 6).toUpperCase(Locale.ROOT);
        return "PO-" + timestamp + "-" + randomPart;
    }
}
