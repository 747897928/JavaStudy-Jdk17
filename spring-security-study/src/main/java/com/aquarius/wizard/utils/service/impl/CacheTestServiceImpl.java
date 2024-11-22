package com.aquarius.wizard.utils.service.impl;

import com.aquarius.wizard.utils.service.CacheTestService;
import org.springframework.cache.annotation.CacheConfig;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

/**
 * @author zhaoyijie
 * @since 2024/11/22 23:27
 */
@CacheConfig(cacheManager = "otherCacheManager")
@Service
public class CacheTestServiceImpl implements CacheTestService {

    @Cacheable(value = "otherCacheName", key = "#amToken")
    @Override
    public String test1(String amToken) {
        return "test1" + amToken;
    }
}
