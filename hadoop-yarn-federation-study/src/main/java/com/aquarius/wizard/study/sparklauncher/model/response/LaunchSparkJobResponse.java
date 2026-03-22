package com.aquarius.wizard.study.sparklauncher.model.response;

public record LaunchSparkJobResponse(
        String submissionId,
        String applicationId,
        String launcherState,
        String statusQueryUri,
        String jobName,
        String queue
) {
}
