package com.aquarius.wizard.study.sparklauncher;

public record SubmissionRecord(
        String submissionId,
        String jobName,
        String applicationId,
        String launcherState
) {
}
