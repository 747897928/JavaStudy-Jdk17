package com.aquarius.wizard.springdatar2dbcdemo.repository;

import com.aquarius.wizard.springdatar2dbcdemo.entity.PurchaseOrderEntity;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;

/**
 * 写库 Repository。
 * <p>
 * 这个 Repository 被绑定在 writer 侧 {@code R2dbcEntityTemplate} 上，
 * 用于演示“写请求固定走主库”。
 */
public interface PurchaseOrderWriteRepository extends ReactiveCrudRepository<PurchaseOrderEntity, Long> {
}
