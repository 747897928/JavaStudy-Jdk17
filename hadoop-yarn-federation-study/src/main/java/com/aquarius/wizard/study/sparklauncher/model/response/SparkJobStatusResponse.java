package com.aquarius.wizard.study.sparklauncher.model.response;

import java.time.Instant;

public record SparkJobStatusResponse(
        String submissionId,
        String jobName,
        String applicationId,
        String queue,
        String launcherState,
        String yarnState,
        String finalStatus,
        String trackingUrl,
        String routerTrackingUrl,
        String lastError,
        Instant createdAt,
        Instant updatedAt
) {
}
