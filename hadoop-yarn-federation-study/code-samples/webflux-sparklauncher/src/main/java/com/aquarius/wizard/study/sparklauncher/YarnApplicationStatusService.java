package com.aquarius.wizard.study.sparklauncher;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.client.api.YarnClient;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class YarnApplicationStatusService {

    private final Configuration configuration;

    public YarnApplicationStatusService(Configuration configuration) {
        this.configuration = configuration;
    }

    public Mono<ApplicationStatusView> queryByAppId(String appId) {
        return Mono.fromCallable(() -> queryBlocking(appId))
                .subscribeOn(Schedulers.boundedElastic());
    }

    private ApplicationStatusView queryBlocking(String appId) throws Exception {
        try (YarnClient client = YarnClient.createYarnClient()) {
            client.init(configuration);
            client.start();

            ApplicationReport report = client.getApplicationReport(ApplicationId.fromString(appId));
            return new ApplicationStatusView(
                    appId,
                    report.getYarnApplicationState().name(),
                    report.getFinalApplicationStatus().name(),
                    report.getTrackingUrl()
            );
        }
    }

    public record ApplicationStatusView(
            String applicationId,
            String yarnState,
            String finalStatus,
            String trackingUrl
    ) {
    }
}
