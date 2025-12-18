package com.aquarius.wizard.webfluxparquetexportdemo.config;

import lombok.Getter;
import lombok.Setter;

import java.time.Duration;

/**
 * Thread-pool settings for the bounded export executor.
 * <p>
 * This is a top-level class (instead of a nested one) to keep {@code ExportProperties} readable and avoid
 * name confusion with JDK concurrency types.
 */
@Getter
@Setter
public class ExportExecutorProperties {

    /**
     * Fixed pool size for blocking export IO.
     */
    private int poolSize = Math.min(8, Math.max(2, Runtime.getRuntime().availableProcessors()));

    /**
     * Bounded queue capacity: avoid infinite task accumulation.
     */
    private int queueCapacity = 64;

    /**
     * Idle thread keep-alive time.
     */
    private Duration keepAlive = Duration.ofSeconds(60);
}

