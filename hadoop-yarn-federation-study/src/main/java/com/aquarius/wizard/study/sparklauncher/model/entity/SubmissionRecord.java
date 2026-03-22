package com.aquarius.wizard.study.sparklauncher.model.entity;

import java.time.Instant;

/**
 * 提交记录实体。
 * 这里保存网关视角下的一次 Spark 提交及其后续状态演进。
 */
public record SubmissionRecord(
        String submissionId,
        String jobName,
        String queue,
        String applicationId,
        String launcherState,
        String yarnState,
        String finalStatus,
        String trackingUrl,
        String lastError,
        Instant createdAt,
        Instant updatedAt
) {
    public SubmissionRecord withApplicationId(String newApplicationId) {
        return new SubmissionRecord(
                submissionId,
                jobName,
                queue,
                newApplicationId,
                launcherState,
                yarnState,
                finalStatus,
                trackingUrl,
                lastError,
                createdAt,
                Instant.now()
        );
    }

    public SubmissionRecord withLauncherState(String newLauncherState) {
        return new SubmissionRecord(
                submissionId,
                jobName,
                queue,
                applicationId,
                newLauncherState,
                yarnState,
                finalStatus,
                trackingUrl,
                lastError,
                createdAt,
                Instant.now()
        );
    }

    public SubmissionRecord withYarnStatus(String newYarnState, String newFinalStatus, String newTrackingUrl) {
        return new SubmissionRecord(
                submissionId,
                jobName,
                queue,
                applicationId,
                launcherState,
                newYarnState,
                newFinalStatus,
                newTrackingUrl,
                lastError,
                createdAt,
                Instant.now()
        );
    }

    public SubmissionRecord withLastError(String newLastError) {
        return new SubmissionRecord(
                submissionId,
                jobName,
                queue,
                applicationId,
                launcherState,
                yarnState,
                finalStatus,
                trackingUrl,
                newLastError,
                createdAt,
                Instant.now()
        );
    }
}
