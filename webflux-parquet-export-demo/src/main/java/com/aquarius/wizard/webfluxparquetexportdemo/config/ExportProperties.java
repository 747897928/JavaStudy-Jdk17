package com.aquarius.wizard.webfluxparquetexportdemo.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.concurrent.TimeUnit;
import java.util.zip.Deflater;

@ConfigurationProperties(prefix = "demo.export")
public class ExportProperties {

    /**
     * WebFlux response body chunk size (DataBuffer size), in bytes.
     * <p>
     * Beginner note: Spring groups bytes written to the response OutputStream into DataBuffer chunks of this size.
     * Larger chunks usually mean better throughput; smaller chunks can reduce first-byte latency.
     */
    private int chunkSize = 64 * 1024;

    /**
     * BufferedOutputStream buffer size used before ZipOutputStream, in bytes.
     * <p>
     * Beginner note: CSV writing produces many small writes (commas/newlines/quotes). Buffering reduces overhead.
     */
    private int outputBufferSize = 64 * 1024;

    /**
     * Safety cap: export at most N rows.
     * <p>
     * Beginner note: Parquet -> CSV expansion can be huge, so an optional safety limit can protect servers.
     */
    private long maxRows = 5_000_000L;

    /**
     * Flush CSV header immediately (reduce first-byte latency for small outputs).
     * <p>
     * This does not change correctness; it only helps the client see the download start sooner.
     */
    private boolean csvFlushHeader = true;

    /**
     * Flush CSV output when bytes since last flush reaches this threshold.
     * <p>
     * Set to 0 to disable periodic flushing (still flushes at the end).
     */
    private long csvFlushEveryBytes = 1L * 1024 * 1024; // 1MB

    /**
     * Flush ZIP entry header immediately (optional; may reduce compression efficiency slightly).
     * <p>
     * For large ZIP outputs, frequent flushing can hurt compression/CPU. Default is disabled.
     */
    private boolean zipFlushHeader = false;

    /**
     * Flush ZIP output when bytes since last flush reaches this threshold.
     * <p>
     * Recommended to keep this much larger than CSV (or 0 to disable) to avoid hurting deflate efficiency.
     */
    private long zipFlushEveryBytes = 0L;

    /**
     * Zip deflater level. Common choices:
     * - 0 (NO_COMPRESSION)
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

    public boolean isCsvFlushHeader() {
        return csvFlushHeader;
    }

    public void setCsvFlushHeader(boolean csvFlushHeader) {
        this.csvFlushHeader = csvFlushHeader;
    }

    public long getCsvFlushEveryBytes() {
        return csvFlushEveryBytes;
    }

    public void setCsvFlushEveryBytes(long csvFlushEveryBytes) {
        this.csvFlushEveryBytes = csvFlushEveryBytes;
    }

    public boolean isZipFlushHeader() {
        return zipFlushHeader;
    }

    public void setZipFlushHeader(boolean zipFlushHeader) {
        this.zipFlushHeader = zipFlushHeader;
    }

    public long getZipFlushEveryBytes() {
        return zipFlushEveryBytes;
    }

    public void setZipFlushEveryBytes(long zipFlushEveryBytes) {
        this.zipFlushEveryBytes = zipFlushEveryBytes;
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
