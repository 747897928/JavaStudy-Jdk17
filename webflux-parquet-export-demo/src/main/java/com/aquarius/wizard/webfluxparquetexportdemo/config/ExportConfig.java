package com.aquarius.wizard.webfluxparquetexportdemo.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.*;

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
        ExportProperties.Executor cfg = props.getExecutor();
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
                new ThreadPoolExecutor.CallerRunsPolicy()
        );
    }
}

