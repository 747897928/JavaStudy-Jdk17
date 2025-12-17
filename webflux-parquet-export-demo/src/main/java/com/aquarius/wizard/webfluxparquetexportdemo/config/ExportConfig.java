package com.aquarius.wizard.webfluxparquetexportdemo.config;

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

@Configuration
@EnableConfigurationProperties(ExportProperties.class)
public class ExportConfig {

    /**
     * Run all blocking export work (ParquetReader/Writer, ZipOutputStream, etc.) on a dedicated pool.
     * <p>
     * The queue is bounded to avoid unbounded memory growth when too many export requests arrive.
     */
    @Bean(destroyMethod = "shutdown")
    public ExecutorService exportExecutor(ExportProperties props) {
        ExportProperties.ExecutorProperties cfg = props.getExecutor();
        int poolSize = Math.max(1, cfg.getPoolSize());
        int queueCapacity = Math.max(1, cfg.getQueueCapacity());

        return new ThreadPoolExecutor(
                poolSize,
                poolSize,
                cfg.getKeepAlive(),
                cfg.getKeepAliveUnit(),
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
}
