package com.aquarius.wizard.study.sparklauncher;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.util.List;

public record LaunchSparkJobRequest(
        @NotBlank String jobName,
        @NotBlank String mainClass,
        @NotBlank String appResource,
        List<String> appArgs,
        String queue,
        String driverMemory,
        String executorMemory,
        @NotNull Integer executorInstances
) {
}
