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

/**
 * 订单写服务。
 * <p>
 * 这条链路演示的是“完整响应式写流程”：
 * <p>
 * 1. Controller 收到请求后不阻塞线程
 * 2. 先用 {@link PartnerRiskClient} 发起非阻塞下游 HTTP 调用
 * 3. 下游返回后组装订单实体
 * 4. 再通过 R2DBC 写入 PostgreSQL
 */
@Service
public class OrderCommandService {

    private static final String ORDER_NO_PREFIX = "PO-";
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

    /**
     * 创建订单。
     * <p>
     * 注意这里没有任何 {@code block()}，整个流程都是 Mono 链式拼接。
     */
    public Mono<OrderResponse> createOrder(CreateOrderRequest request) {
        return Mono.defer(() -> {
                    BigDecimal normalizedUnitPrice = request.unitPrice().setScale(2, RoundingMode.HALF_UP);
                    BigDecimal orderAmount = normalizedUnitPrice
                            .multiply(BigDecimal.valueOf(request.quantity()))
                            .setScale(2, RoundingMode.HALF_UP);

                    // 先调用下游风控/运费服务，再把结果拼回订单实体。
                    return partnerRiskClient.evaluate(request.customerTier(), orderAmount)
                            .map(decision -> buildEntity(request, normalizedUnitPrice, decision));
                })
                .flatMap(writeRepository::save)
                .map(responseMapper::toResponse)
                .as(writerTransactionalOperator::transactional);
    }

    /**
     * 把请求对象和下游决策结果合并成待落库实体。
     */
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

    /**
     * 生成订单号。
     * <p>
     * 这里选择“UTC 时间 + 随机串”，方便演示，不追求分布式全局单调有序。
     */
    private String generateOrderNo() {
        String timestamp = ORDER_NO_TIME_FORMATTER.format(Instant.now());
        String randomPart = UUID.randomUUID().toString().substring(0, 6).toUpperCase(Locale.ROOT);
        return ORDER_NO_PREFIX + timestamp + "-" + randomPart;
    }
}
