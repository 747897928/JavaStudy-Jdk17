package com.aquarius.wizard.study.sparklauncher.controller;

import com.aquarius.wizard.study.sparklauncher.model.request.LaunchSparkJobRequest;
import com.aquarius.wizard.study.sparklauncher.model.response.LaunchSparkJobResponse;
import com.aquarius.wizard.study.sparklauncher.model.response.SparkJobStatusResponse;
import com.aquarius.wizard.study.sparklauncher.service.SparkLauncherSubmissionService;
import com.aquarius.wizard.study.sparklauncher.service.YarnApplicationStatusService;
import jakarta.validation.Valid;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

/**
 * 这里暴露的是最小可用的 Spark 提交网关接口。
 * 提交接口只负责把任务送到 SparkLauncher，并尽快返回 submissionId / appId；
 * 状态接口则根据 submissionId 回查网关本地记录，再决定是否去 Yarn 查询。
 */
@RestController
@RequestMapping("/api/spark/jobs")
public class SparkJobController {

    private final SparkLauncherSubmissionService submissionService;
    private final YarnApplicationStatusService statusService;

    public SparkJobController(
            SparkLauncherSubmissionService submissionService,
            YarnApplicationStatusService statusService
    ) {
        this.submissionService = submissionService;
        this.statusService = statusService;
    }

    @PostMapping
    public Mono<LaunchSparkJobResponse> submit(@Valid @RequestBody LaunchSparkJobRequest request) {
        return submissionService.submit(request);
    }

    @GetMapping("/{submissionId}/status")
    public Mono<SparkJobStatusResponse> status(@PathVariable String submissionId) {
        return statusService.queryBySubmissionId(submissionId);
    }
}
