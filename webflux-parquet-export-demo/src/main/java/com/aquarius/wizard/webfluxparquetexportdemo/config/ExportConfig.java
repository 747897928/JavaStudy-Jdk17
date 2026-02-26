package com.aquarius.wizard.webfluxparquetexportdemo.config;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Configuration
@EnableConfigurationProperties(ExportProperties.class)
public class ExportConfig {

    /**
     * Run all blocking export work (ParquetReader/Writer, ZipOutputStream, etc.) on a dedicated pool.
     * <p>
     * The queue is bounded to avoid unbounded memory growth when too many export requests arrive.
     */
    @Bean(destroyMethod = "shutdown")
    public ExecutorService exportExecutor(ExportProperties props, MeterRegistry meterRegistry) {
        ExportExecutorProperties cfg = props.getExecutorProperties();
        int poolSize = Math.max(1, cfg.getPoolSize());
        int queueCapacity = Math.max(1, cfg.getQueueCapacity());

        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                poolSize,
                poolSize,
                Math.max(0L, cfg.getKeepAlive().toMillis()),
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(queueCapacity),
                new ThreadFactory() {
                    private final ThreadFactory delegate = Executors.defaultThreadFactory();

                    @Override
                    public Thread newThread(Runnable r) {
                        Thread t = delegate.newThread(r);
                        t.setName("export-io-" + t.getId());
                        t.setDaemon(true);
                        return t;
                    }
                },
                // Never run blocking export work on the caller thread (might be Netty event-loop).
                // Reject instead and let the request fail fast (caller can retry later).
                new ThreadPoolExecutor.AbortPolicy()
        );
        registerExportExecutorGauges(executor, meterRegistry);
        return executor;
    }

    /**
     * Shared Reactor {@link Scheduler} backed by the same bounded export executor.
     * <p>
     * We reuse a single Scheduler instance to avoid creating a new wrapper for every request.
     */
    @Bean(destroyMethod = "dispose")
    public Scheduler exportScheduler(ExecutorService exportExecutor) {
        return Schedulers.fromExecutorService(exportExecutor);
    }

    private void registerExportExecutorGauges(ThreadPoolExecutor executor, MeterRegistry meterRegistry) {
        Gauge.builder("demo.export.executor.active", executor, ThreadPoolExecutor::getActiveCount)
                .description("Active threads in the bounded export executor")
                .register(meterRegistry);
        Gauge.builder("demo.export.executor.pool.size", executor, ThreadPoolExecutor::getPoolSize)
                .description("Current pool size of the export executor")
                .register(meterRegistry);
        Gauge.builder("demo.export.executor.queue.size", executor.getQueue(), queue -> (double) queue.size())
                .description("Queue size of the export executor")
                .register(meterRegistry);
        Gauge.builder("demo.export.executor.queue.remaining", executor.getQueue(), queue -> (double) queue.remainingCapacity())
                .description("Remaining queue capacity of the export executor")
                .register(meterRegistry);
    }
}
