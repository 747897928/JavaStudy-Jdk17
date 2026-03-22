package com.aquarius.wizard.study.sparklauncher;

import jakarta.validation.Valid;
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
    public Mono<LaunchSparkJobResponse> submit(@Valid @RequestBody LaunchSparkJobRequest request) {
        return submissionService.submit(request);
    }

    @GetMapping("/{submissionId}/status")
    public Mono<SparkJobStatusResponse> status(@PathVariable String submissionId) {
        return Mono.fromCallable(() -> submissionRecordRepository.findRequiredBySubmissionId(submissionId))
                .flatMap(record -> {
                    if (record.applicationId() == null || record.applicationId().isBlank()) {
                        return Mono.just(new SparkJobStatusResponse(
                                record.submissionId(),
                                null,
                                record.launcherState(),
                                "APP_ID_PENDING",
                                null,
                                null
                        ));
                    }
                    return statusService.queryByAppId(record.applicationId())
                            .map(status -> new SparkJobStatusResponse(
                                    record.submissionId(),
                                    record.applicationId(),
                                    record.launcherState(),
                                    status.yarnState(),
                                    status.finalStatus(),
                                    status.trackingUrl()
                            ));
                });
    }
}
