package com.aquarius.wizard.webfluxparquetexportdemo.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.concurrent.TimeUnit;
import java.util.zip.Deflater;

@ConfigurationProperties(prefix = "demo.export")
public class ExportProperties {

    /**
     * WebFlux response body chunk size (DataBuffer size), in bytes.
     */
    private int chunkSize = 64 * 1024;

    /**
     * BufferedOutputStream buffer size used before ZipOutputStream, in bytes.
     */
    private int outputBufferSize = 64 * 1024;

    /**
     * Safety cap: export at most N rows.
     */
    private long maxRows = 5_000_000L;

    /**
     * Flush every N rows (avoid flush per row).
     */
    private long flushEveryRows = 10_000L;

    /**
     * Zip deflater level. Common choices:
     * - 1 (BEST_SPEED)
     * - 6 (DEFAULT_COMPRESSION)
     * - 9 (BEST_COMPRESSION)
     */
    private int zipLevel = Deflater.BEST_SPEED;

    private final Executor executor = new Executor();

    public int getChunkSize() {
        return chunkSize;
    }

    public void setChunkSize(int chunkSize) {
        this.chunkSize = chunkSize;
    }

    public int getOutputBufferSize() {
        return outputBufferSize;
    }

    public void setOutputBufferSize(int outputBufferSize) {
        this.outputBufferSize = outputBufferSize;
    }

    public long getMaxRows() {
        return maxRows;
    }

    public void setMaxRows(long maxRows) {
        this.maxRows = maxRows;
    }

    public long getFlushEveryRows() {
        return flushEveryRows;
    }

    public void setFlushEveryRows(long flushEveryRows) {
        this.flushEveryRows = flushEveryRows;
    }

    public int getZipLevel() {
        return zipLevel;
    }

    public void setZipLevel(int zipLevel) {
        this.zipLevel = zipLevel;
    }

    public Executor getExecutor() {
        return executor;
    }

    public static class Executor {
        /**
         * Fixed pool size for blocking export IO.
         */
        private int poolSize = Math.min(8, Math.max(2, Runtime.getRuntime().availableProcessors()));

        /**
         * Bounded queue capacity: avoid infinite task accumulation.
         */
        private int queueCapacity = 64;

        private long keepAlive = 60L;

        private TimeUnit keepAliveUnit = TimeUnit.SECONDS;

        public int getPoolSize() {
            return poolSize;
        }

        public void setPoolSize(int poolSize) {
            this.poolSize = poolSize;
        }

        public int getQueueCapacity() {
            return queueCapacity;
        }

        public void setQueueCapacity(int queueCapacity) {
            this.queueCapacity = queueCapacity;
        }

        public long getKeepAlive() {
            return keepAlive;
        }

        public void setKeepAlive(long keepAlive) {
            this.keepAlive = keepAlive;
        }

        public TimeUnit getKeepAliveUnit() {
            return keepAliveUnit;
        }

        public void setKeepAliveUnit(TimeUnit keepAliveUnit) {
            this.keepAliveUnit = keepAliveUnit;
        }
    }
}

