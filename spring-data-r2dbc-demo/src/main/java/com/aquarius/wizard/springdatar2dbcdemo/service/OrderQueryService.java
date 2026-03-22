package com.aquarius.wizard.springdatar2dbcdemo.service;

import com.aquarius.wizard.springdatar2dbcdemo.dto.OrderResponse;
import com.aquarius.wizard.springdatar2dbcdemo.entity.PurchaseOrderEntity;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.domain.Sort;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.data.relational.core.query.Criteria;
import org.springframework.data.relational.core.query.Query;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * 订单读服务。
 * <p>
 * 这个类刻意绑定 reader 侧 {@link R2dbcEntityTemplate}，
 * 目的是把“读流量优先走从库”这件事在代码层写清楚。
 */
@Service
public class OrderQueryService {

    private final R2dbcEntityTemplate readerEntityTemplate;
    private final OrderResponseMapper responseMapper;

    public OrderQueryService(
            @Qualifier("readerEntityTemplate") R2dbcEntityTemplate readerEntityTemplate,
            OrderResponseMapper responseMapper
    ) {
        this.readerEntityTemplate = readerEntityTemplate;
        this.responseMapper = responseMapper;
    }

    /**
     * 从读库按创建时间倒序列出订单。
     */
    public Flux<OrderResponse> listOrders(int limit) {
        Query query = Query.empty()
                .sort(Sort.by(Sort.Order.desc("createdAt")))
                .limit(limit);

        return readerEntityTemplate.select(query, PurchaseOrderEntity.class)
                .map(responseMapper::toResponse);
    }

    /**
     * 从读库查询单个订单。
     * <p>
     * 如果主从复制存在延迟，刚写完立刻来查，这里理论上可能读不到。
     * 这正是读写分离场景下需要权衡的一致性问题。
     */
    public Mono<OrderResponse> getOrder(String orderNo) {
        Query query = Query.query(Criteria.where("orderNo").is(orderNo));
        return readerEntityTemplate.selectOne(query, PurchaseOrderEntity.class)
                .switchIfEmpty(Mono.error(new ResponseStatusException(
                        HttpStatus.NOT_FOUND,
                        "Order not found: " + orderNo
                )))
                .map(responseMapper::toResponse);
    }
}
