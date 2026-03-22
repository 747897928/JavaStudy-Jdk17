package com.aquarius.wizard.study.sparklauncher;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/spark/jobs")
public class SparkJobController {

    private final SparkLauncherSubmissionService submissionService;
    private final YarnApplicationStatusService statusService;
    private final SubmissionRecordRepository submissionRecordRepository;

    public SparkJobController(
            SparkLauncherSubmissionService submissionService,
            YarnApplicationStatusService statusService,
            SubmissionRecordRepository submissionRecordRepository
    ) {
        this.submissionService = submissionService;
        this.statusService = statusService;
        this.submissionRecordRepository = submissionRecordRepository;
    }

    @PostMapping
    public Mono<LaunchSparkJobResponse> submit(@RequestBody LaunchSparkJobRequest request) {
        return submissionService.submit(request);
    }

    @GetMapping("/{submissionId}/status")
    public Mono<YarnApplicationStatusService.ApplicationStatusView> status(@PathVariable String submissionId) {
        return Mono.fromCallable(() -> submissionRecordRepository.findRequiredBySubmissionId(submissionId))
                .flatMap(record -> statusService.queryByAppId(record.applicationId()));
    }
}
