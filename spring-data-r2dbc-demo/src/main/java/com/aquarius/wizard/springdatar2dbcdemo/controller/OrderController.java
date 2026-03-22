package com.aquarius.wizard.springdatar2dbcdemo.controller;

import com.aquarius.wizard.springdatar2dbcdemo.dto.CreateOrderRequest;
import com.aquarius.wizard.springdatar2dbcdemo.dto.OrderResponse;
import com.aquarius.wizard.springdatar2dbcdemo.service.OrderCommandService;
import com.aquarius.wizard.springdatar2dbcdemo.service.OrderQueryService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import org.springframework.http.HttpStatus;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * 订单接口。
 * <p>
 * - 创建接口走写链路
 * - 查询接口走读链路
 */
@Validated
@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private final OrderCommandService orderCommandService;
    private final OrderQueryService orderQueryService;

    public OrderController(OrderCommandService orderCommandService, OrderQueryService orderQueryService) {
        this.orderCommandService = orderCommandService;
        this.orderQueryService = orderQueryService;
    }

    /**
     * 创建订单，返回写入结果。
     */
    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public Mono<OrderResponse> create(@Valid @RequestBody CreateOrderRequest request) {
        return orderCommandService.createOrder(request);
    }

    /**
     * 列出订单，默认最多返回 10 条。
     */
    @GetMapping
    public Flux<OrderResponse> list(
            @RequestParam(defaultValue = "10")
            @Min(value = 1, message = "limit must be at least 1")
            @Max(value = 100, message = "limit must be at most 100")
            int limit
    ) {
        return orderQueryService.listOrders(limit);
    }

    /**
     * 按订单号查询明细。
     */
    @GetMapping("/{orderNo}")
    public Mono<OrderResponse> detail(@PathVariable String orderNo) {
        return orderQueryService.getOrder(orderNo);
    }
}
