package com.aquarius.wizard.study.sparklauncher;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class InMemorySubmissionRecordRepository implements SubmissionRecordRepository {

    private final Map<String, SubmissionRecord> store = new ConcurrentHashMap<>();

    @Override
    public void saveSubmitted(String submissionId, String jobName) {
        store.put(submissionId, new SubmissionRecord(submissionId, jobName, null, "SUBMITTED"));
    }

    @Override
    public void updateApplicationId(String submissionId, String applicationId) {
        store.computeIfPresent(submissionId, (id, record) ->
                new SubmissionRecord(record.submissionId(), record.jobName(), applicationId, record.launcherState()));
    }

    @Override
    public void updateLauncherState(String submissionId, String launcherState) {
        store.computeIfPresent(submissionId, (id, record) ->
                new SubmissionRecord(record.submissionId(), record.jobName(), record.applicationId(), launcherState));
    }

    @Override
    public SubmissionRecord findRequiredBySubmissionId(String submissionId) {
        SubmissionRecord record = store.get(submissionId);
        if (record == null) {
            throw new IllegalArgumentException("Submission not found: " + submissionId);
        }
        return record;
    }
}
