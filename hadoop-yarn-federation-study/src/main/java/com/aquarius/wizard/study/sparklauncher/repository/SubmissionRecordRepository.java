package com.aquarius.wizard.study.sparklauncher.repository;

import com.aquarius.wizard.study.sparklauncher.model.entity.SubmissionRecord;

public interface SubmissionRecordRepository {

    void saveSubmitted(String submissionId, String jobName, String queue);

    void updateApplicationId(String submissionId, String applicationId);

    void updateLauncherState(String submissionId, String launcherState);

    void updateYarnStatus(String submissionId, String yarnState, String finalStatus, String trackingUrl);

    void updateLastError(String submissionId, String lastError);

    SubmissionRecord findRequiredBySubmissionId(String submissionId);
}
