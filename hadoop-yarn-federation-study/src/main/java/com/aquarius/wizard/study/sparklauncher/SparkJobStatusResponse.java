package com.aquarius.wizard.study.sparklauncher;

public record SparkJobStatusResponse(
        String submissionId,
        String applicationId,
        String launcherState,
        String yarnState,
        String finalStatus,
        String trackingUrl
) {
}
