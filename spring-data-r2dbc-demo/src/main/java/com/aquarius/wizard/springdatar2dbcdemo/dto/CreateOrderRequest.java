package com.aquarius.wizard.springdatar2dbcdemo.dto;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.math.BigDecimal;

/**
 * 创建订单请求。
 */
public record CreateOrderRequest(
        @NotBlank(message = "customerName must not be blank")
        String customerName,
        @NotBlank(message = "customerTier must not be blank")
        String customerTier,
        @NotBlank(message = "skuCode must not be blank")
        String skuCode,
        @Min(value = 1, message = "quantity must be greater than 0")
        int quantity,
        @NotNull(message = "unitPrice must not be null")
        @DecimalMin(value = "0.01", message = "unitPrice must be greater than 0")
        BigDecimal unitPrice,
        String remark
) {
}
