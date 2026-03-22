package com.aquarius.wizard.springdatar2dbcdemo.controller;

import com.aquarius.wizard.springdatar2dbcdemo.dto.TopologySummaryResponse;
import com.aquarius.wizard.springdatar2dbcdemo.service.DatabaseTopologyService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

/**
 * 拓扑探针接口。
 * <p>
 * 学习主从切换时，优先看这个接口返回，再去看日志。
 */
@RestController
@RequestMapping("/api/topology")
public class DatabaseTopologyController {

    private final DatabaseTopologyService databaseTopologyService;

    public DatabaseTopologyController(DatabaseTopologyService databaseTopologyService) {
        this.databaseTopologyService = databaseTopologyService;
    }

    /**
     * 返回当前 writer / reader 实际连接到的数据库节点信息。
     */
    @GetMapping
    public Mono<TopologySummaryResponse> inspect() {
        return databaseTopologyService.inspect();
    }
}
