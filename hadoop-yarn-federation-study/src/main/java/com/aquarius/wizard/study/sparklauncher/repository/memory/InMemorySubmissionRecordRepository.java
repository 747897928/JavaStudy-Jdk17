package com.aquarius.wizard.study.sparklauncher.repository.memory;

import com.aquarius.wizard.study.sparklauncher.model.entity.SubmissionRecord;
import com.aquarius.wizard.study.sparklauncher.repository.SubmissionRecordRepository;
import com.aquarius.wizard.study.sparklauncher.support.SubmissionNotFoundException;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class InMemorySubmissionRecordRepository implements SubmissionRecordRepository {

    // 当前先用内存仓储做演示，后续接数据库时替换这一层即可。
    private final Map<String, SubmissionRecord> store = new ConcurrentHashMap<>();

    @Override
    public void saveSubmitted(String submissionId, String jobName, String queue) {
        Instant now = Instant.now();
        store.put(submissionId, new SubmissionRecord(
                submissionId,
                jobName,
                queue,
                null,
                "SUBMITTED",
                "APP_ID_PENDING",
                null,
                null,
                null,
                now,
                now
        ));
    }

    @Override
    public void updateApplicationId(String submissionId, String applicationId) {
        store.computeIfPresent(submissionId, (id, record) -> record.withApplicationId(applicationId));
    }

    @Override
    public void updateLauncherState(String submissionId, String launcherState) {
        store.computeIfPresent(submissionId, (id, record) -> record.withLauncherState(launcherState));
    }

    @Override
    public void updateYarnStatus(String submissionId, String yarnState, String finalStatus, String trackingUrl) {
        store.computeIfPresent(submissionId, (id, record) -> record.withYarnStatus(yarnState, finalStatus, trackingUrl));
    }

    @Override
    public void updateLastError(String submissionId, String lastError) {
        store.computeIfPresent(submissionId, (id, record) -> record.withLastError(lastError));
    }

    @Override
    public SubmissionRecord findRequiredBySubmissionId(String submissionId) {
        SubmissionRecord record = store.get(submissionId);
        if (record == null) {
            throw new SubmissionNotFoundException(submissionId);
        }
        return record;
    }
}
