package com.aquarius.wizard.webfluxparquetexportdemo.service;

import com.aquarius.wizard.webfluxparquetexportdemo.io.CountingOutputStream;
import com.aquarius.wizard.webfluxparquetexportdemo.config.ExportProperties;
import com.aquarius.wizard.webfluxparquetexportdemo.model.FileFormat;
import com.aquarius.wizard.webfluxparquetexportdemo.parquet.ParquetToCsvCellValueConverter;
import com.aquarius.wizard.webfluxparquetexportdemo.util.CsvUtil;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.util.StreamUtils;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.SignalType;
import reactor.netty.channel.AbortedException;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.channels.ClosedChannelException;
import java.util.Locale;
import java.util.Map;
import java.util.EnumMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.zip.Deflater;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * Core: Parquet -> CSV/ZIP streaming export.
 * <p>
 * Key ideas:
 * <ul>
 *   <li><b>Do not build a "row object stream"</b> like {@code Flux<Map<...>>}.
 *       If the client is slow, those per-row objects can accumulate and cause OOM.</li>
 *   <li><b>Read one row, write one row</b>: {@link ParquetReader} reads one {@link Group} at a time, we immediately
 *       convert it to CSV bytes and write to an {@link OutputStream}.</li>
 *   <li><b>Bridge blocking IO to WebFlux</b> with {@link DataBufferUtils#outputStreamPublisher}:
 *       we write to an {@link OutputStream} on a dedicated thread pool and Spring converts written bytes to
 *       {@link DataBuffer}s for the HTTP response.</li>
 *   <li><b>Backpressure</b>: when the client is slow, HTTP writes become slow; that slowness is naturally propagated
 *       back to our loop (we stop producing data too fast), so memory stays bounded.</li>
 * </ul>
 */
@Service
public class ParquetExportService {

    private static final Logger log = LoggerFactory.getLogger(ParquetExportService.class);

    /**
     * CSV encoding used by this demo.
     * <p>
     * We use UTF-8 for interoperability.
     * For {@code BINARY} columns we output Base64 text (see {@code ParquetToCsvCellValueConverter}) so the CSV stays valid UTF-8.
     */
    public static final Charset CSV_CHARSET = StandardCharsets.UTF_8;

    private final ExportProperties props;
    private final ExecutorService exportExecutor;
    private final MeterRegistry meterRegistry;
    private final Map<FileFormat, Counter> bytesCounters = new EnumMap<>(FileFormat.class);
    private final Map<FileFormat, Timer> durationTimers = new EnumMap<>(FileFormat.class);
    private final Map<FileFormat, Counter> rejectedCounters = new EnumMap<>(FileFormat.class);

    public ParquetExportService(ExportProperties props, ExecutorService exportExecutor, MeterRegistry meterRegistry) {
        this.props = props;
        this.exportExecutor = exportExecutor;
        this.meterRegistry = meterRegistry;
        initializeMetrics();
    }

    private void initializeMetrics() {
        for (FileFormat format : FileFormat.values()) {
            String formatTag = format.name().toLowerCase(Locale.ROOT);
            bytesCounters.put(format, Counter.builder("demo.export.bytes")
                    .tag("operation", "export")
                    .tag("format", formatTag)
                    .register(meterRegistry));
            durationTimers.put(format, Timer.builder("demo.export.duration")
                    .tag("operation", "export")
                    .tag("format", formatTag)
                    .register(meterRegistry));
            rejectedCounters.put(format, Counter.builder("demo.export.rejections")
                    .tag("operation", "export")
                    .tag("format", formatTag)
                    .register(meterRegistry));
        }
    }

    /**
     * Best-effort guard: fail fast before starting a streaming export if the bounded executor is overloaded.
     * <p>
     * This avoids the old {@code CallerRunsPolicy} pitfall (running blocking work on Netty event-loop when saturated),
     * and makes overload behavior explicit (503).
     */
    public void assertExportCapacityOrThrow() {
        if (!(exportExecutor instanceof ThreadPoolExecutor tpe)) {
            return;
        }
        if (tpe.getQueue().remainingCapacity() > 0) {
            return;
        }
        if (tpe.getActiveCount() < tpe.getMaximumPoolSize()) {
            return;
        }
        throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE,
                "Export system is busy (bounded executor queue is full)");
    }

    /**
     * Export as PARQUET/CSV/ZIP(CSV) using streaming.
     */
    public Publisher<DataBuffer> streamExport(java.nio.file.Path parquetFile, FileFormat format, DataBufferFactory bufferFactory) {
        return streamExport(parquetFile, format, bufferFactory, "data");
    }

    /**
     * Stream-export with a preferred base name.
     * <p>
     * For ZIP, this controls the CSV entry name inside the ZIP: {@code <baseName>.csv}.
     */
    public Publisher<DataBuffer> streamExport(java.nio.file.Path parquetFile,
                                              FileFormat format,
                                              DataBufferFactory bufferFactory,
                                              String baseName) {
        String safeBaseName = (baseName == null || baseName.isBlank()) ? "data" : baseName;
        String zipEntryName = sanitizeZipEntryName(safeBaseName + ".csv");

        return createStreamingExportPublisher(parquetFile, format, bufferFactory, zipEntryName);
    }

    private Publisher<DataBuffer> createStreamingExportPublisher(java.nio.file.Path parquetFile,
                                                                 FileFormat format,
                                                                 DataBufferFactory bufferFactory,
                                                                 String zipEntryName) {
        return switch (format) {
            case PARQUET -> streamParquetWithMetrics(parquetFile, bufferFactory);
            // CSV/ZIP are generated on-the-fly without creating a full file in memory.
            case CSV -> Flux
                    .from(DataBufferUtils.outputStreamPublisher(
                            os -> writeCsvTo(os, parquetFile),
                            bufferFactory,
                            exportExecutor,
                            props.getChunkSize()
                    ))
                    .onErrorMap(RejectedExecutionException.class, ex -> exportRejected(FileFormat.CSV, ex));
            case ZIP -> Flux
                .from(DataBufferUtils.outputStreamPublisher(
                        os -> writeZipCsvTo(os, parquetFile, zipEntryName),
                        bufferFactory,
                        exportExecutor,
                        props.getChunkSize()
                ))
                .onErrorMap(RejectedExecutionException.class, ex -> exportRejected(FileFormat.ZIP, ex));
        };
    }

    private RuntimeException exportRejected(FileFormat format, RejectedExecutionException ex) {
        Counter counter = rejectedCounters.get(format);
        if (counter != null) {
            counter.increment();
        }
        return new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE,
                "Export system is busy (executor rejected the task)", ex);
    }

    private void writeZipCsvTo(OutputStream rawOut, java.nio.file.Path parquetFile, String zipEntryName) {
        Timer.Sample sample = Timer.start(meterRegistry);
        CountingOutputStream countingZip = null;
        try {
            // Important: the OutputStream provided by outputStreamPublisher is owned by Spring.
            // We must NOT close it ourselves (otherwise the HTTP response breaks).
            OutputStream nonClosing = StreamUtils.nonClosing(rawOut);

            // Add a small buffer to reduce the number of system calls / tiny writes.
            BufferedOutputStream bos = new BufferedOutputStream(nonClosing, props.getOutputBufferSize());

            // Charset here is for ZIP metadata (entry names, flags). It does NOT affect CSV content bytes we write.
            try (ZipOutputStream zos = new ZipOutputStream(bos, StandardCharsets.UTF_8)) {
                int level = props.getZipLevel();
                if (level < Deflater.NO_COMPRESSION || level > Deflater.BEST_COMPRESSION) {
                    level = Deflater.BEST_SPEED;
                }
                zos.setLevel(level);

                zos.putNextEntry(new ZipEntry(zipEntryName));

                // We write CSV bytes directly into the ZIP entry stream.
                // The CSV byte encoding is controlled by CSV_CHARSET when we convert String -> bytes.
                countingZip = new CountingOutputStream(zos);
                writeCsvTo(countingZip, parquetFile, props.isZipFlushHeader(), props.getZipFlushEveryBytes(), false);
                zos.closeEntry();

                // finish() writes the ZIP central directory (required for a valid ZIP file).
                zos.finish();
                zos.flush();
            }
        } catch (IOException e) {
            if (isClientAbort(e)) {
                return;
            }
            throw new UncheckedIOException(e);
        } catch (RuntimeException e) {
            if (isClientAbort(e)) {
                return;
            }
            throw e;
        } finally {
            if (countingZip != null) {
                bytesCounters.get(FileFormat.ZIP).increment(countingZip.getCount());
            }
            sample.stop(durationTimers.get(FileFormat.ZIP));
        }
    }

    private static String sanitizeZipEntryName(String name) {
        if (name == null || name.isBlank()) {
            return "data.csv";
        }
        // Avoid path traversal/dir creation in zip entry names.
        String sanitized = name.replace('\\', '_').replace('/', '_');
        // Avoid weird edge cases like "." or ".." as entry names.
        if (sanitized.equals(".") || sanitized.equals("..")) {
            return "data.csv";
        }
        return sanitized;
    }

    private void writeCsvTo(OutputStream out, java.nio.file.Path parquetFile) {
        writeCsvTo(out, parquetFile, props.isCsvFlushHeader(), props.getCsvFlushEveryBytes(), true);
    }

    private void writeCsvTo(OutputStream rawOut,
                            java.nio.file.Path parquetFile,
                            boolean flushHeader,
                            long flushEveryBytes,
                            boolean wrapPlainCsvBuffer) {
        long flushThresholdBytes = Math.max(0, flushEveryBytes);
        long startedAtNanos = System.nanoTime();
        Timer.Sample sample = Timer.start(meterRegistry);
        CountingOutputStream countingOut = null;

        try {
            Configuration conf = new Configuration();
            Path parquetPath = new Path(parquetFile.toUri());
            MessageType schema = readSchema(conf, parquetPath);

            countingOut = prepareCsvOutputStream(rawOut, wrapPlainCsvBuffer);
            long lastFlushedAt = writeHeaderAndMaybeFlush(countingOut, schema, flushHeader);

            streamCsvRows(conf, parquetPath, schema, countingOut, flushThresholdBytes, lastFlushedAt);

            countingOut.flush();

            if (wrapPlainCsvBuffer) {
                long elapsedMs = (System.nanoTime() - startedAtNanos) / 1_000_000L;
                log.debug("CSV export completed: parquetFile={}, csvBytes={}, elapsedMs={}",
                        parquetFile.getFileName(), countingOut.getCount(), elapsedMs);
            }
        } catch (IOException e) {
            if (isClientAbort(e)) {
                if (wrapPlainCsvBuffer && countingOut != null) {
                    long elapsedMs = (System.nanoTime() - startedAtNanos) / 1_000_000L;
                    log.debug("CSV export aborted by client: parquetFile={}, csvBytesSoFar={}, elapsedMs={}",
                            parquetFile.getFileName(), countingOut.getCount(), elapsedMs);
                }
                return;
            }
            throw new UncheckedIOException(e);
        } catch (RuntimeException e) {
            if (isClientAbort(e)) {
                if (wrapPlainCsvBuffer && countingOut != null) {
                    long elapsedMs = (System.nanoTime() - startedAtNanos) / 1_000_000L;
                    log.debug("CSV export aborted by client: parquetFile={}, csvBytesSoFar={}, elapsedMs={}",
                            parquetFile.getFileName(), countingOut.getCount(), elapsedMs);
                }
                return;
            }
            throw e;
        } finally {
            if (countingOut != null) {
                bytesCounters.get(FileFormat.CSV).increment(countingOut.getCount());
            }
            sample.stop(durationTimers.get(FileFormat.CSV));
        }
    }

    private CountingOutputStream prepareCsvOutputStream(OutputStream rawOut, boolean wrapPlainCsvBuffer) {
        OutputStream out = rawOut;
        if (wrapPlainCsvBuffer) {
            out = new BufferedOutputStream(out, props.getOutputBufferSize());
        }
        return (out instanceof CountingOutputStream c) ? c : new CountingOutputStream(out);
    }

    private long writeHeaderAndMaybeFlush(CountingOutputStream out, MessageType schema, boolean flushHeader) throws IOException {
        CsvUtil.writeHeader(out, schema, CSV_CHARSET);
        if (flushHeader) {
            out.flush();
        }
        return out.getCount();
    }

    private void streamCsvRows(Configuration conf,
                               Path parquetPath,
                               MessageType schema,
                               CountingOutputStream out,
                               long flushThresholdBytes,
                               long lastFlushedAt) throws IOException {
        GroupReadSupport readSupport = new GroupReadSupport();
        try (ParquetReader<Group> reader = ParquetReader.builder(readSupport, parquetPath).withConf(conf).build()) {
            Group rowGroup;
            while ((rowGroup = reader.read()) != null) {
                CsvUtil.writeRow(out, rowGroup, schema, CSV_CHARSET, ParquetToCsvCellValueConverter::toCsvCellValue);
                if (flushThresholdBytes > 0 && (out.getCount() - lastFlushedAt) >= flushThresholdBytes) {
                    out.flush();
                    lastFlushedAt = out.getCount();
                }
            }
        }
    }

    private MessageType readSchema(Configuration conf, Path hPath) throws IOException {
        try (ParquetFileReader pfr = ParquetFileReader.open(HadoopInputFile.fromPath(hPath, conf))) {
            return pfr.getFileMetaData().getSchema();
        }
    }

    private Publisher<DataBuffer> streamParquetWithMetrics(java.nio.file.Path parquetFile, DataBufferFactory bufferFactory) {
        Timer.Sample sample = Timer.start(meterRegistry);
        java.util.concurrent.atomic.AtomicLong totalBytes = new java.util.concurrent.atomic.AtomicLong();

        return DataBufferUtils.read(parquetFile, bufferFactory, props.getChunkSize())
                .doOnNext(buf -> totalBytes.addAndGet(buf.readableByteCount()))
                .doFinally(signalType -> {
                    if (signalType != SignalType.CANCEL) {
                        bytesCounters.get(FileFormat.PARQUET).increment(totalBytes.get());
                    }
                    sample.stop(durationTimers.get(FileFormat.PARQUET));
                });
    }

    private boolean isClientAbort(Throwable error) {
        for (Throwable throwable = error; throwable != null; throwable = throwable.getCause()) {
            if (isClientAbortThrowable(throwable)) {
                return true;
            }
        }
        return false;
    }

    private boolean isClientAbortThrowable(Throwable throwable) {
        if (throwable instanceof AbortedException) {
            return true;
        }
        if (AbortedException.isConnectionReset(throwable)) {
            return true;
        }
        if (throwable instanceof ClosedChannelException) {
            return true;
        }
        if (throwable instanceof IOException ioException) {
            String msg = (ioException.getMessage() == null) ? "" : ioException.getMessage().toLowerCase(Locale.ROOT);
            return msg.contains("broken pipe")
                    || msg.contains("connection reset")
                    || msg.contains("reset by peer")
                    || msg.contains("forcibly closed")
                    || msg.contains("clientabort")
                    || msg.contains("abort");
        }
        return false;
    }
}
