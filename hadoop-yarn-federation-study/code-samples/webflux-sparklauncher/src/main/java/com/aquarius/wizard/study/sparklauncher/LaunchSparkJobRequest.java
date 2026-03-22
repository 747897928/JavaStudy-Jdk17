package com.aquarius.wizard.study.sparklauncher;

import java.util.List;

public record LaunchSparkJobRequest(
        String jobName,
        String mainClass,
        String appResource,
        List<String> appArgs,
        String queue,
        String driverMemory,
        String executorMemory,
        Integer executorInstances
) {
}
