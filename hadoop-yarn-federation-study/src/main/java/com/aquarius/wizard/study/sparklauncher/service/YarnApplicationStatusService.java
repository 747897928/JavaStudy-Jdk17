package com.aquarius.wizard.study.sparklauncher.service;

import com.aquarius.wizard.study.sparklauncher.config.SparkLauncherProperties;
import com.aquarius.wizard.study.sparklauncher.model.entity.SubmissionRecord;
import com.aquarius.wizard.study.sparklauncher.model.response.SparkJobStatusResponse;
import com.aquarius.wizard.study.sparklauncher.repository.SubmissionRecordRepository;
import com.aquarius.wizard.study.sparklauncher.support.YarnClientPool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class YarnApplicationStatusService {

    private final SubmissionRecordRepository submissionRecordRepository;
    private final YarnClientPool yarnClientPool;
    private final Configuration hadoopConfiguration;
    private final SparkLauncherProperties properties;

    public YarnApplicationStatusService(
            SubmissionRecordRepository submissionRecordRepository,
            YarnClientPool yarnClientPool,
            Configuration hadoopConfiguration,
            SparkLauncherProperties properties
    ) {
        this.submissionRecordRepository = submissionRecordRepository;
        this.yarnClientPool = yarnClientPool;
        this.hadoopConfiguration = hadoopConfiguration;
        this.properties = properties;
    }

    public Mono<SparkJobStatusResponse> queryBySubmissionId(String submissionId) {
        return Mono.fromCallable(() -> queryBlocking(submissionId))
                .subscribeOn(Schedulers.boundedElastic());
    }

    /**
     * 中文说明：
     * 状态查询优先根据 submissionId 找到网关自己的提交记录。
     * 如果 appId 还没拿到，就直接返回网关视角的状态；拿到 appId 后再去 Yarn 查询。
     */
    private SparkJobStatusResponse queryBlocking(String submissionId) throws Exception {
        SubmissionRecord record = submissionRecordRepository.findRequiredBySubmissionId(submissionId);
        if (record.applicationId() == null || record.applicationId().isBlank()) {
            return toResponse(record);
        }

        ApplicationReport report = yarnClientPool.getApplicationReport(
                record.applicationId(),
                properties.getStatusClientBorrowTimeout().toMillis()
        );
        submissionRecordRepository.updateYarnStatus(
                submissionId,
                report.getYarnApplicationState().name(),
                report.getFinalApplicationStatus().name(),
                report.getTrackingUrl()
        );
        return toResponse(submissionRecordRepository.findRequiredBySubmissionId(submissionId));
    }

    private SparkJobStatusResponse toResponse(SubmissionRecord record) {
        return new SparkJobStatusResponse(
                record.submissionId(),
                record.jobName(),
                record.applicationId(),
                record.queue(),
                record.launcherState(),
                record.yarnState(),
                record.finalStatus(),
                record.trackingUrl(),
                buildRouterTrackingUrl(record.applicationId()),
                record.lastError(),
                record.createdAt(),
                record.updatedAt()
        );
    }

    private String buildRouterTrackingUrl(String applicationId) {
        if (applicationId == null || applicationId.isBlank()) {
            return null;
        }
        String configuredBaseUrl = properties.getRouterBaseUrl();
        if (configuredBaseUrl != null && !configuredBaseUrl.isBlank()) {
            return configuredBaseUrl + "/cluster/app/" + applicationId;
        }

        // 中文说明：没有显式配置 Router 地址时，再退化为从 hadoop 配置里推断 webapp 地址。
        boolean httpsOnly = "HTTPS_ONLY".equalsIgnoreCase(hadoopConfiguration.get("yarn.http.policy", "HTTP_ONLY"));
        String webappAddress = httpsOnly
                ? firstNonBlank(
                hadoopConfiguration.get("yarn.router.webapp.https.address"),
                hadoopConfiguration.get("yarn.resourcemanager.webapp.https.address")
        )
                : firstNonBlank(
                hadoopConfiguration.get("yarn.router.webapp.address"),
                hadoopConfiguration.get("yarn.resourcemanager.webapp.address")
        );
        if (webappAddress == null) {
            return null;
        }
        String scheme = httpsOnly ? "https://" : "http://";
        return scheme + webappAddress + "/cluster/app/" + applicationId;
    }

    private static String firstNonBlank(String first, String second) {
        if (first != null && !first.isBlank()) {
            return first;
        }
        if (second != null && !second.isBlank()) {
            return second;
        }
        return null;
    }
}
