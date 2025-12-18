package com.aquarius.wizard.webfluxparquetexportdemo.service;

import com.aquarius.wizard.webfluxparquetexportdemo.io.CountingOutputStream;
import com.aquarius.wizard.webfluxparquetexportdemo.io.NonClosingOutputStream;
import com.aquarius.wizard.webfluxparquetexportdemo.config.ExportProperties;
import com.aquarius.wizard.webfluxparquetexportdemo.model.FileFormat;
import com.aquarius.wizard.webfluxparquetexportdemo.parquet.ParquetToCsvCellValueConverter;
import com.aquarius.wizard.webfluxparquetexportdemo.util.CsvUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.reactivestreams.Publisher;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
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

    /**
     * CSV encoding used by this demo.
     * <p>
     * We use UTF-8 for interoperability.
     * For {@code BINARY} columns we output Base64 text (see {@code ParquetToCsvCellValueConverter}) so the CSV stays valid UTF-8.
     */
    public static final Charset CSV_CHARSET = StandardCharsets.UTF_8;

    private final ExportProperties props;
    private final ExecutorService exportExecutor;

    public ParquetExportService(ExportProperties props, ExecutorService exportExecutor) {
        this.props = props;
        this.exportExecutor = exportExecutor;
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
            case PARQUET -> DataBufferUtils.read(parquetFile, bufferFactory, props.getChunkSize());
            // CSV/ZIP are generated on-the-fly without creating a full file in memory.
            case CSV -> reactor.core.publisher.Flux
                    .from(DataBufferUtils.outputStreamPublisher(
                            os -> writeCsvTo(os, parquetFile),
                            bufferFactory,
                            exportExecutor,
                            props.getChunkSize()
                    ))
                    .onErrorMap(RejectedExecutionException.class, this::exportRejected);
            case ZIP -> reactor.core.publisher.Flux
                .from(DataBufferUtils.outputStreamPublisher(
                        os -> writeZipCsvTo(os, parquetFile, zipEntryName),
                        bufferFactory,
                        exportExecutor,
                        props.getChunkSize()
                ))
                .onErrorMap(RejectedExecutionException.class, this::exportRejected);
        };
    }

    private RuntimeException exportRejected(RejectedExecutionException ex) {
        return new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE,
                "Export system is busy (executor rejected the task)", ex);
    }

    private void writeZipCsvTo(OutputStream rawOut, java.nio.file.Path parquetFile, String zipEntryName) {
        try {
            // Important: the OutputStream provided by outputStreamPublisher is owned by Spring.
            // We must NOT close it ourselves (otherwise the HTTP response breaks).
            OutputStream nonClosing = new NonClosingOutputStream(rawOut);

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
                CountingOutputStream countingZip = new CountingOutputStream(zos);
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
        long maxRows = props.getMaxRows() <= 0 ? Long.MAX_VALUE : props.getMaxRows();

        try {
            Configuration conf = new Configuration();
            Path parquetPath = new Path(parquetFile.toUri());
            MessageType schema = readSchema(conf, parquetPath);

            CountingOutputStream countingOut = prepareCsvOutputStream(rawOut, wrapPlainCsvBuffer);
            long lastFlushedAt = writeHeaderAndMaybeFlush(countingOut, schema, flushHeader);

            streamCsvRows(conf, parquetPath, schema, countingOut, maxRows, flushThresholdBytes, lastFlushedAt);

            countingOut.flush();
        } catch (IOException e) {
            if (isClientAbort(e)) {
                return;
            }
            throw new UncheckedIOException(e);
        }
    }

    private CountingOutputStream prepareCsvOutputStream(OutputStream rawOut, boolean wrapPlainCsvBuffer) {
        OutputStream out = rawOut;
        if (wrapPlainCsvBuffer) {
            out = new BufferedOutputStream(new NonClosingOutputStream(out), props.getOutputBufferSize());
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
                               long maxRows,
                               long flushThresholdBytes,
                               long lastFlushedAt) throws IOException {
        GroupReadSupport readSupport = new GroupReadSupport();
        try (ParquetReader<Group> reader = ParquetReader.builder(readSupport, parquetPath).withConf(conf).build()) {
            long rowIndex = 0L;
            Group rowGroup;
            while ((rowGroup = reader.read()) != null) {
                rowIndex++;
                if (rowIndex > maxRows) {
                    break;
                }

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

    private boolean isClientAbort(IOException e) {
        String msg = (e.getMessage() == null) ? "" : e.getMessage().toLowerCase();
        return msg.contains("broken pipe")
                || msg.contains("connection reset")
                || msg.contains("forcibly closed")
                || msg.contains("abort");
    }
}
