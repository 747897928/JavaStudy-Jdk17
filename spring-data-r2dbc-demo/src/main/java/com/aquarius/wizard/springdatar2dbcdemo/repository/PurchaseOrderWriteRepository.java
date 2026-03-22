package com.aquarius.wizard.springdatar2dbcdemo.repository;

import com.aquarius.wizard.springdatar2dbcdemo.entity.PurchaseOrderEntity;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Mono;

public interface PurchaseOrderWriteRepository extends ReactiveCrudRepository<PurchaseOrderEntity, Long> {

    Mono<PurchaseOrderEntity> findByOrderNo(String orderNo);
}
