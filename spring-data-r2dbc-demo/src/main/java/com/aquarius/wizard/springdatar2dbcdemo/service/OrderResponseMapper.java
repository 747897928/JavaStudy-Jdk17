package com.aquarius.wizard.springdatar2dbcdemo.service;

import com.aquarius.wizard.springdatar2dbcdemo.dto.OrderResponse;
import com.aquarius.wizard.springdatar2dbcdemo.entity.PurchaseOrderEntity;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * 实体到接口响应的转换器。
 */
@Component
public class OrderResponseMapper {

    /**
     * 组装接口返回对象，并顺手计算订单总金额。
     */
    public OrderResponse toResponse(PurchaseOrderEntity entity) {
        BigDecimal orderAmount = entity.getUnitPrice()
                .multiply(BigDecimal.valueOf(entity.getQuantity()))
                .add(entity.getShippingFee())
                .setScale(2, RoundingMode.HALF_UP);

        return new OrderResponse(
                entity.getOrderNo(),
                entity.getCustomerName(),
                entity.getCustomerTier(),
                entity.getSkuCode(),
                entity.getQuantity(),
                entity.getUnitPrice(),
                entity.getShippingFee(),
                orderAmount,
                entity.getRiskLevel(),
                entity.getStatus(),
                entity.getRemark(),
                entity.getCreatedAt(),
                entity.getUpdatedAt()
        );
    }
}
