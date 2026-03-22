package com.aquarius.wizard.study.sparklauncher;

import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.HashMap;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

public class SparkLauncherSubmissionService {

    private final Path sparkHome;
    private final Path hadoopConfDir;
    private final String principal;
    private final Path keytab;
    private final SubmissionRecordRepository submissionRecordRepository;

    public SparkLauncherSubmissionService(
            Path sparkHome,
            Path hadoopConfDir,
            String principal,
            Path keytab,
            SubmissionRecordRepository submissionRecordRepository
    ) {
        this.sparkHome = sparkHome;
        this.hadoopConfDir = hadoopConfDir;
        this.principal = principal;
        this.keytab = keytab;
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
        launcherEnv.put("HADOOP_CONF_DIR", hadoopConfDir.toString());
        launcherEnv.put("YARN_CONF_DIR", hadoopConfDir.toString());

        SparkLauncher launcher = new SparkLauncher(launcherEnv)
                .setSparkHome(sparkHome.toString())
                .setMaster("yarn")
                .setDeployMode("cluster")
                .setAppName(request.jobName())
                .setMainClass(request.mainClass())
                .setAppResource(request.appResource())
                .setConf("spark.yarn.principal", principal)
                .setConf("spark.yarn.keytab", keytab.toString())
                .setConf("spark.yarn.queue", defaultIfBlank(request.queue(), "root.batch"))
                .setConf("spark.driver.memory", defaultIfBlank(request.driverMemory(), "2g"))
                .setConf("spark.executor.memory", defaultIfBlank(request.executorMemory(), "4g"))
                .setConf("spark.executor.instances", String.valueOf(request.executorInstances() == null ? 2 : request.executorInstances()))
                .setConf(SparkLauncher.DRIVER_EXTRA_JAVA_OPTIONS, "-Djava.security.krb5.conf=/etc/krb5.conf");

        launcher.setConf("spark.hadoop.cloneConf", "true");
        launcher.setConf("spark.yarn.submit.waitAppCompletion", "false");

        List<String> appArgs = request.appArgs();
        if (appArgs != null) {
            launcher.addAppArgs(appArgs.toArray(String[]::new));
        }

        launcher.directory(sparkHome.toFile());
        launcher.setVerbose(true);
        launcher.setConf("spark.launcher.childProcLoggerName", "org.apache.spark.launcher.app." + request.jobName());

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

        long deadline = System.nanoTime() + Duration.ofSeconds(8).toNanos();
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
    }

    private static String defaultIfBlank(String value, String fallback) {
        return value == null || value.isBlank() ? fallback : value;
    }
}
