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

    public Flux<OrderResponse> listOrders(int limit) {
        Query query = Query.empty()
                .sort(Sort.by(Sort.Order.desc("createdAt")))
                .limit(limit);

        return readerEntityTemplate.select(query, PurchaseOrderEntity.class)
                .map(responseMapper::toResponse);
    }

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
