package com.aquarius.wizard.study.sparklauncher;

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

    private final SparkGatewayProperties properties;
    private final SubmissionRecordRepository submissionRecordRepository;

    public SparkLauncherSubmissionService(
            SparkGatewayProperties properties,
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
        AtomicReference<String> appIdRef = new AtomicReference<>();
        AtomicReference<SparkAppHandle.State> stateRef = new AtomicReference<>(SparkAppHandle.State.UNKNOWN);

        submissionRecordRepository.saveSubmitted(submissionId, request.jobName());

        Map<String, String> launcherEnv = new HashMap<>();
        launcherEnv.put("HADOOP_CONF_DIR", properties.getHadoopConfDir().toString());
        launcherEnv.put("YARN_CONF_DIR", properties.getHadoopConfDir().toString());

        SparkLauncher launcher = new SparkLauncher(launcherEnv)
                .setSparkHome(properties.getSparkHome().toString())
                .setMaster("yarn")
                .setDeployMode("cluster")
                .setAppName(request.jobName())
                .setMainClass(request.mainClass())
                .setAppResource(request.appResource())
                .setConf("spark.kerberos.principal", properties.getPrincipal())
                .setConf("spark.kerberos.keytab", properties.getKeytab().toString())
                .setConf("spark.yarn.principal", properties.getPrincipal())
                .setConf("spark.yarn.keytab", properties.getKeytab().toString())
                .setConf("spark.yarn.queue", defaultIfBlank(request.queue(), "root.batch"))
                .setConf("spark.driver.memory", defaultIfBlank(request.driverMemory(), "2g"))
                .setConf("spark.executor.memory", defaultIfBlank(request.executorMemory(), "4g"))
                .setConf("spark.executor.instances", String.valueOf(request.executorInstances()))
                .setConf(SparkLauncher.DRIVER_EXTRA_JAVA_OPTIONS, "-Djava.security.krb5.conf=/etc/krb5.conf");

        launcher.setConf("spark.hadoop.cloneConf", "true");
        launcher.setConf("spark.yarn.submit.waitAppCompletion", "false");

        List<String> appArgs = request.appArgs();
        if (appArgs != null) {
            launcher.addAppArgs(appArgs.toArray(String[]::new));
        }

        launcher.directory(properties.getSparkHome().toFile());
        launcher.setVerbose(true);
        launcher.setConf("spark.launcher.childProcLoggerName", "org.apache.spark.launcher.app." + request.jobName());

        try {
            SparkAppHandle handle = launcher.startApplication(new SparkAppHandle.Listener() {
                @Override
                public void stateChanged(SparkAppHandle handle) {
                    stateRef.set(handle.getState());
                    submissionRecordRepository.updateLauncherState(submissionId, handle.getState().name());
                    if (handle.getAppId() != null) {
                        appIdRef.compareAndSet(null, handle.getAppId());
                        submissionRecordRepository.updateApplicationId(submissionId, handle.getAppId());
                    }
                }

                @Override
                public void infoChanged(SparkAppHandle handle) {
                    if (handle.getAppId() != null) {
                        appIdRef.compareAndSet(null, handle.getAppId());
                        submissionRecordRepository.updateApplicationId(submissionId, handle.getAppId());
                    }
                }
            });

            long deadline = System.nanoTime() + properties.getAppIdWaitTimeout().toNanos();
            while (System.nanoTime() < deadline) {
                if (handle.getAppId() != null) {
                    appIdRef.compareAndSet(null, handle.getAppId());
                    break;
                }
                Thread.sleep(200);
            }

            String appId = appIdRef.get();
            String launcherState = stateRef.get().name();
            submissionRecordRepository.updateLauncherState(submissionId, launcherState);
            return new LaunchSparkJobResponse(
                    submissionId,
                    appId,
                    launcherState,
                    "/api/spark/jobs/" + submissionId + "/status"
            );
        } catch (Exception e) {
            submissionRecordRepository.updateLauncherState(submissionId, "FAILED_TO_LAUNCH");
            throw e;
        }
    }

    private static String defaultIfBlank(String value, String fallback) {
        return value == null || value.isBlank() ? fallback : value;
    }
}
