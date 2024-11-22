package com.aquarius.wizard.utils.config;

import com.aquarius.wizard.utils.entity.JwtUserInfo;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * @author zhaoyijie
 * @since 2024/11/22 22:20
 */
@EnableCaching
@Configuration
public class CacheConfiguration {

    private final long defaultExpirationInNanos = TimeUnit.HOURS.toNanos(2);

    @Bean(name = "jwtUserInfoCacheManager")
    public CacheManager jwtUserInfoCacheManager() {
        CaffeineCacheManager cacheManager = new CaffeineCacheManager("jwtUserInfos");
        cacheManager.setCaffeine(caffeineJwtUserInfosBuilder());
        return cacheManager;
    }

    @Bean(name = "otherCacheManager")
    public CacheManager otherCacheManager() {
        CaffeineCacheManager cacheManager = new CaffeineCacheManager("otherCacheName");
        cacheManager.setCaffeine(Caffeine.newBuilder().expireAfterWrite(10, TimeUnit.SECONDS));
        return cacheManager;
    }

    private Caffeine<Object, Object> caffeineJwtUserInfosBuilder() {
        return Caffeine.newBuilder()
                .expireAfter(new Expiry<Object, Object>() {
                    @Override
                    public long expireAfterCreate(Object key, Object value, long currentTime) {
                        if (!(value instanceof JwtUserInfo jwtUserInfo)) {
                            return defaultExpirationInNanos;
                        }
                        Date expiry = jwtUserInfo.getExpiry();
                        if (expiry == null) {
                            return defaultExpirationInNanos;
                        }
                        long expiryTimeMillis = expiry.getTime();
                        long currentTimeMillis = System.currentTimeMillis();
                        long durationNanos = TimeUnit.MILLISECONDS.toNanos(expiryTimeMillis - currentTimeMillis);
                        return Math.max(durationNanos, defaultExpirationInNanos);
                    }

                    @Override
                    public long expireAfterUpdate(Object key, Object value, long currentTime, long currentDuration) {
                        return expireAfterCreate(key, value, currentTime);
                    }

                    @Override
                    public long expireAfterRead(Object key, Object value, long currentTime, long currentDuration) {
                        return currentDuration; // 不在读取时改变过期时间
                    }
                });
    }
}
