package com.aquarius.wizard.study.sparklauncher;

public record LaunchSparkJobResponse(
        String submissionId,
        String applicationId,
        String launcherState,
        String statusQueryUri
) {
}
