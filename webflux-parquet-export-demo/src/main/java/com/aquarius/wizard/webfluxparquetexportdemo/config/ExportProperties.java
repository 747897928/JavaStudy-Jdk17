package com.aquarius.wizard.webfluxparquetexportdemo.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;

import java.util.zip.Deflater;

@ConfigurationProperties(prefix = "demo.export")
@Getter
@Setter
public class ExportProperties {

    /**
     * WebFlux response body chunk size (DataBuffer size), in bytes.
     * <p>
     * Note: Spring groups bytes written to the response OutputStream into DataBuffer chunks of this size.
     * Larger chunks usually mean better throughput; smaller chunks can reduce first-byte latency.
     */
    private int chunkSize = 64 * 1024;

    /**
     * BufferedOutputStream buffer size used before ZipOutputStream, in bytes.
     * <p>
     * Note: CSV writing produces many small writes (commas/newlines/quotes). Buffering reduces overhead.
     */
    private int outputBufferSize = 64 * 1024;

    /**
     * Safety limit: reject exporting CSV/ZIP if the Parquet file has more than N rows.
     * <p>
     * Why this is a "reject" (fail-fast) limit instead of "truncate":
     * <ul>
     *   <li>Truncating would produce an incomplete CSV/ZIP download, which is usually worse than a clear error.</li>
     *   <li>Fail-fast happens before streaming starts, so clients get a proper HTTP error response.</li>
     * </ul>
     * Set to {@code 0} to disable the check (unlimited).
     */
    private long maxAllowedRows = 0L;

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

    @Getter
    @NestedConfigurationProperty
    private final ExportExecutorProperties executorProperties = new ExportExecutorProperties();
}
