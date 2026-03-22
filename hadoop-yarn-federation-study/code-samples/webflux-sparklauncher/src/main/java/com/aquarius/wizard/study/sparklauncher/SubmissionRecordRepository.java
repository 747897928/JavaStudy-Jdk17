package com.aquarius.wizard.study.sparklauncher;

public interface SubmissionRecordRepository {

    void saveSubmitted(String submissionId, String jobName);

    void updateApplicationId(String submissionId, String applicationId);

    void updateLauncherState(String submissionId, String launcherState);

    SubmissionRecord findRequiredBySubmissionId(String submissionId);
}
