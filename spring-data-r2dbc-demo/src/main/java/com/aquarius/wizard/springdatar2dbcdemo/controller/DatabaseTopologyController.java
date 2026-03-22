package com.aquarius.wizard.springdatar2dbcdemo.controller;

import com.aquarius.wizard.springdatar2dbcdemo.dto.TopologySummaryResponse;
import com.aquarius.wizard.springdatar2dbcdemo.service.DatabaseTopologyService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/topology")
public class DatabaseTopologyController {

    private final DatabaseTopologyService databaseTopologyService;

    public DatabaseTopologyController(DatabaseTopologyService databaseTopologyService) {
        this.databaseTopologyService = databaseTopologyService;
    }

    @GetMapping
    public Mono<TopologySummaryResponse> inspect() {
        return databaseTopologyService.inspect();
    }
}
