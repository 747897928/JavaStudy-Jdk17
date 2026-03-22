package com.aquarius.wizard.study.sparklauncher.service;

import com.aquarius.wizard.study.sparklauncher.config.SparkLauncherProperties;
import com.aquarius.wizard.study.sparklauncher.model.request.LaunchSparkJobRequest;
import com.aquarius.wizard.study.sparklauncher.model.response.LaunchSparkJobResponse;
import com.aquarius.wizard.study.sparklauncher.repository.SubmissionRecordRepository;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

public class SparkLauncherSubmissionService {

    private final SparkLauncherProperties properties;
    private final SubmissionRecordRepository submissionRecordRepository;

    public SparkLauncherSubmissionService(
            SparkLauncherProperties properties,
            SubmissionRecordRepository submissionRecordRepository
    ) {
        this.properties = properties;
        this.submissionRecordRepository = submissionRecordRepository;
    }

    public Mono<LaunchSparkJobResponse> submit(LaunchSparkJobRequest request) {
        return Mono.fromCallable(() -> submitBlocking(request))
                .subscribeOn(Schedulers.boundedElastic());
    }

    private LaunchSparkJobResponse submitBlocking(LaunchSparkJobRequest request) throws Exception {
        String submissionId = "sub-" + UUID.randomUUID();
        String queue = defaultIfBlank(request.queue(), properties.getDefaultQueue());
        AtomicReference<String> appIdRef = new AtomicReference<>();
        AtomicReference<SparkAppHandle.State> stateRef = new AtomicReference<>(SparkAppHandle.State.UNKNOWN);

        submissionRecordRepository.saveSubmitted(submissionId, request.jobName(), queue);
        submissionRecordRepository.updateLauncherState(submissionId, "LAUNCHING");

        try {
            SparkLauncher launcher = buildLauncher(request, submissionId, queue);
            SparkAppHandle handle = launcher.startApplication(new SubmissionListener(submissionId, appIdRef, stateRef));

            waitForApplicationId(handle, appIdRef);

            String launcherState = stateRef.get().name();
            submissionRecordRepository.updateLauncherState(submissionId, launcherState);
            return new LaunchSparkJobResponse(
                    submissionId,
                    appIdRef.get(),
                    launcherState,
                    "/api/spark/jobs/" + submissionId + "/status",
                    request.jobName(),
                    queue
            );
        } catch (Exception exception) {
            submissionRecordRepository.updateLauncherState(submissionId, "FAILED_TO_LAUNCH");
            submissionRecordRepository.updateLastError(submissionId, exception.getMessage());
            throw exception;
        }
    }

    private SparkLauncher buildLauncher(LaunchSparkJobRequest request, String submissionId, String queue) {
        Map<String, String> launcherEnv = new HashMap<>();
        launcherEnv.put("HADOOP_CONF_DIR", properties.getHadoopConfDir().toString());
        launcherEnv.put("YARN_CONF_DIR", properties.getHadoopConfDir().toString());

        SparkLauncher launcher = new SparkLauncher(launcherEnv)
                .setSparkHome(properties.getSparkHome().toString())
                .setMaster("yarn")
                .setDeployMode(properties.getDeployMode())
                .setAppName(request.jobName())
                .setMainClass(request.mainClass())
                .setAppResource(request.appResource())
                .setConf("spark.kerberos.principal", properties.getPrincipal())
                .setConf("spark.kerberos.keytab", properties.getKeytab().toString())
                .setConf("spark.yarn.principal", properties.getPrincipal())
                .setConf("spark.yarn.keytab", properties.getKeytab().toString())
                .setConf("spark.yarn.queue", queue)
                .setConf("spark.driver.memory", defaultIfBlank(request.driverMemory(), properties.getDefaultDriverMemory()))
                .setConf("spark.executor.memory", defaultIfBlank(request.executorMemory(), properties.getDefaultExecutorMemory()))
                .setConf("spark.executor.instances", String.valueOf(defaultIfNull(request.executorInstances(), properties.getDefaultExecutorInstances())))
                .setConf("spark.hadoop.cloneConf", "true")
                .setConf("spark.yarn.submit.waitAppCompletion", "false")
                .setConf("spark.yarn.maxAppAttempts", "1")
                .setConf("spark.yarn.tags", buildSparkTags(submissionId, request.businessKey()))
                .setConf(SparkLauncher.DRIVER_EXTRA_JAVA_OPTIONS, "-Djava.security.krb5.conf=/etc/krb5.conf");

        List<String> appArgs = request.appArgs();
        if (appArgs != null && !appArgs.isEmpty()) {
            launcher.addAppArgs(appArgs.toArray(String[]::new));
        }
        if (request.proxyUser() != null && !request.proxyUser().isBlank()) {
            launcher.addSparkArg("--proxy-user", request.proxyUser());
        }

        launcher.directory(properties.getSparkHome().toFile());
        launcher.setVerbose(true);
        launcher.setConf("spark.launcher.childProcLoggerName", "org.apache.spark.launcher.app." + request.jobName());
        return launcher;
    }

    private void waitForApplicationId(SparkAppHandle handle, AtomicReference<String> appIdRef) throws InterruptedException {
        long deadline = System.nanoTime() + properties.getAppIdWaitTimeout().toNanos();
        while (System.nanoTime() < deadline) {
            if (handle.getAppId() != null) {
                appIdRef.compareAndSet(null, handle.getAppId());
                break;
            }
            Thread.sleep(200);
        }
    }

    private static String buildSparkTags(String submissionId, String businessKey) {
        if (businessKey == null || businessKey.isBlank()) {
            return "submissionId=" + submissionId;
        }
        return "submissionId=" + submissionId + ",businessKey=" + businessKey;
    }

    private static String defaultIfBlank(String value, String fallback) {
        return value == null || value.isBlank() ? fallback : value;
    }

    private static Integer defaultIfNull(Integer value, Integer fallback) {
        return value == null ? fallback : value;
    }

    private final class SubmissionListener implements SparkAppHandle.Listener {

        private final String submissionId;
        private final AtomicReference<String> appIdRef;
        private final AtomicReference<SparkAppHandle.State> stateRef;

        private SubmissionListener(
                String submissionId,
                AtomicReference<String> appIdRef,
                AtomicReference<SparkAppHandle.State> stateRef
        ) {
            this.submissionId = submissionId;
            this.appIdRef = appIdRef;
            this.stateRef = stateRef;
        }

        @Override
        public void stateChanged(SparkAppHandle handle) {
            stateRef.set(handle.getState());
            submissionRecordRepository.updateLauncherState(submissionId, handle.getState().name());
            syncAppId(handle);
        }

        @Override
        public void infoChanged(SparkAppHandle handle) {
            syncAppId(handle);
        }

        private void syncAppId(SparkAppHandle handle) {
            if (handle.getAppId() != null) {
                appIdRef.compareAndSet(null, handle.getAppId());
                submissionRecordRepository.updateApplicationId(submissionId, handle.getAppId());
            }
        }
    }
}
