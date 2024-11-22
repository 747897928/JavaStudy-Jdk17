package com.aquarius.wizard.utils.controller;

import com.aquarius.wizard.utils.service.CacheTestService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author zhaoyijie
 * @since 2024/11/22 23:22
 */
@Slf4j
@RestController
@RequestMapping(value = "/cache")
public class CacheTestController {

    @Autowired
    private CacheTestService cacheTestService;

    @GetMapping(value = "/test1")
    public String test1(@RequestParam(value = "key") String key) {
        return cacheTestService.test1(key);
    }
}
