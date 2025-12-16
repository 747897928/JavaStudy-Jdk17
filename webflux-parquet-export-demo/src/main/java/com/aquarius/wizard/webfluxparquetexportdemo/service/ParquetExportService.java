package com.aquarius.wizard.webfluxparquetexportdemo.service;

import com.aquarius.wizard.webfluxparquetexportdemo.config.ExportProperties;
import com.aquarius.wizard.webfluxparquetexportdemo.model.FileFormat;
import com.aquarius.wizard.webfluxparquetexportdemo.util.CountingOutputStream;
import com.aquarius.wizard.webfluxparquetexportdemo.util.CsvUtil;
import com.aquarius.wizard.webfluxparquetexportdemo.util.Int96Util;
import com.aquarius.wizard.webfluxparquetexportdemo.util.NonClosingOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.reactivestreams.Publisher;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.Deflater;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * Core: Parquet -> CSV/ZIP streaming export.
 * <p>
 * Key ideas (beginner-friendly):
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
     * For BINARY: we output raw bytes to the CSV stream and ask the client to interpret CSV as ISO-8859-1 so that
     * bytes(0..255) map 1:1 to characters.
     */
    public static final Charset CSV_CHARSET = Charset.forName("ISO-8859-1");

    private static final String DEMO_SCHEMA = """
            message demo {
              optional int32   i32;
              optional int64   i64;
              optional int96   t96;
              optional float   f32;
              optional boolean b;
              optional double  d64;
              optional binary  bin;
            }
            """;

    private final ExportProperties props;
    private final ExecutorService exportExecutor;
    private final AtomicReference<java.nio.file.Path> demoParquetPath = new AtomicReference<>();

    public ParquetExportService(ExportProperties props, ExecutorService exportExecutor) {
        this.props = props;
        this.exportExecutor = exportExecutor;
    }

    public long size(java.nio.file.Path p) {
        try {
            return Files.size(p);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public java.nio.file.Path getDemoParquetOrThrow() {
        java.nio.file.Path p = demoParquetPath.get();
        if (p == null || !Files.exists(p)) {
            throw new IllegalStateException("Demo parquet not found. Call POST /demo/generate first.");
        }
        return p;
    }

    /**
     * Generate a Parquet file at {@code ./data/demo.parquet} (relative to user.dir).
     */
    public Mono<java.nio.file.Path> generateDemoParquet(long rows) {
        return Mono.fromCallable(() -> generateDemoParquetBlocking(rows))
                .subscribeOn(Schedulers.fromExecutor(exportExecutor));
    }

    private java.nio.file.Path generateDemoParquetBlocking(long rows) throws IOException {
        java.nio.file.Path dir = Paths.get(System.getProperty("user.dir"), "data");
        Files.createDirectories(dir);

        java.nio.file.Path out = dir.resolve("demo.parquet");

        MessageType schema = MessageTypeParser.parseMessageType(DEMO_SCHEMA);
        Configuration conf = new Configuration();
        GroupWriteSupport.setSchema(schema, conf);

        SimpleGroupFactory factory = new SimpleGroupFactory(schema);
        Random random = new Random(1234567L);

        try (ParquetWriter<Group> writer =
                     new ParquetWriter<>(
                             new Path(out.toUri()),
                             new GroupWriteSupport(),
                             CompressionCodecName.SNAPPY,
                             ParquetWriter.DEFAULT_BLOCK_SIZE,
                             ParquetWriter.DEFAULT_PAGE_SIZE,
                             ParquetWriter.DEFAULT_PAGE_SIZE,
                             ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED,
                             ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED,
                             org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_1_0,
                             conf
                     )) {

            for (long i = 0; i < rows; i++) {
                Group g = factory.newGroup();

                // Make some columns null randomly (optional fields), to test: null -> empty
                if (random.nextInt(10) != 0) g.add("i32", random.nextInt());
                if (random.nextInt(10) != 0) g.add("i64", random.nextLong());
                if (random.nextInt(10) != 0) {
                    Instant now = Instant.ofEpochMilli(System.currentTimeMillis() + random.nextInt(1_000_000));
                    g.add("t96", Binary.fromConstantByteArray(Int96Util.instantToInt96(now)));
                }
                if (random.nextInt(10) != 0) g.add("f32", random.nextFloat());
                if (random.nextInt(10) != 0) g.add("b", random.nextBoolean());
                if (random.nextInt(10) != 0) g.add("d64", random.nextDouble());

                if (random.nextInt(10) != 0) {
                    // ASCII bytes are easier to eyeball in CSV, but export logic supports arbitrary bytes.
                    byte[] bytes = new byte[16];
                    for (int k = 0; k < bytes.length; k++) {
                        int ch = 33 + random.nextInt(94); // '!'..'~'
                        bytes[k] = (byte) ch;
                    }
                    g.add("bin", Binary.fromConstantByteArray(bytes));
                }

                writer.write(g);
            }
        }

        demoParquetPath.set(out);
        return out;
    }

    /**
     * Export as PARQUET/CSV/ZIP(CSV) using streaming.
     */
    public Publisher<DataBuffer> export(java.nio.file.Path parquetFile, FileFormat format, DataBufferFactory bufferFactory) {
        return switch (format) {
            case PARQUET -> DataBufferUtils.read(parquetFile, bufferFactory, props.getChunkSize());
            // CSV/ZIP are generated on-the-fly without creating a full file in memory.
            case CSV -> DataBufferUtils.outputStreamPublisher(
                    os -> writeCsvTo(os, parquetFile),
                    bufferFactory,
                    exportExecutor,
                    props.getChunkSize()
            );
            case ZIP -> DataBufferUtils.outputStreamPublisher(
                    os -> writeZipCsvTo(os, parquetFile),
                    bufferFactory,
                    exportExecutor,
                    props.getChunkSize()
            );
        };
    }

    private void writeZipCsvTo(OutputStream rawOut, java.nio.file.Path parquetFile) {
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

                zos.putNextEntry(new ZipEntry("data.csv"));

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

    private void writeCsvTo(OutputStream out, java.nio.file.Path parquetFile) {
        writeCsvTo(out, parquetFile, props.isCsvFlushHeader(), props.getCsvFlushEveryBytes(), true);
    }

    private void writeCsvTo(OutputStream rawOut,
                            java.nio.file.Path parquetFile,
                            boolean flushHeader,
                            long flushEveryBytes,
                            boolean wrapPlainCsvBuffer) {
        Configuration conf = new Configuration();
        Path hPath = new Path(parquetFile.toUri());

        long flushThresholdBytes = Math.max(0, flushEveryBytes);
        long maxRows = props.getMaxRows() <= 0 ? Long.MAX_VALUE : props.getMaxRows();

        try {
            MessageType schema = readSchema(conf, hPath);

            OutputStream out = rawOut;
            if (wrapPlainCsvBuffer) {
                // Plain CSV writing can be very "chatty" (commas, quotes, newlines).
                // BufferedOutputStream reduces the number of underlying write() calls.
                out = new BufferedOutputStream(new NonClosingOutputStream(out), props.getOutputBufferSize());
            }
            CountingOutputStream countingOut = (out instanceof CountingOutputStream c) ? c : new CountingOutputStream(out);

            CsvUtil.writeHeader(countingOut, schema, CSV_CHARSET);
            if (flushHeader) {
                // Flush once after header so small outputs start downloading immediately.
                countingOut.flush();
            }
            long lastFlushedAt = countingOut.getCount();

            GroupReadSupport readSupport = new GroupReadSupport();
            try (ParquetReader<Group> reader = ParquetReader.builder(readSupport, hPath).withConf(conf).build()) {
                long row = 0L;
                Group g;
                while ((g = reader.read()) != null) {
                    row++;
                    if (row > maxRows) {
                        break;
                    }

                    try {
                        CsvUtil.writeRow(countingOut, g, schema, CSV_CHARSET, this::cellValueToCell);
                        if (flushThresholdBytes > 0 && (countingOut.getCount() - lastFlushedAt) >= flushThresholdBytes) {
                            // Flush is for "latency/progress feel", not for memory safety.
                            // Memory safety mainly comes from streaming + backpressure + bounded buffers.
                            countingOut.flush();
                            lastFlushedAt = countingOut.getCount();
                        }
                    } catch (IOException e) {
                        if (isClientAbort(e)) {
                            return;
                        }
                        throw e;
                    }
                }
            }

            // Final flush to push out remaining buffered bytes.
            countingOut.flush();
        } catch (IOException e) {
            if (isClientAbort(e)) {
                return;
            }
            throw new UncheckedIOException(e);
        }
    }

    private MessageType readSchema(Configuration conf, Path hPath) throws IOException {
        try (ParquetFileReader pfr = ParquetFileReader.open(HadoopInputFile.fromPath(hPath, conf))) {
            return pfr.getFileMetaData().getSchema();
        }
    }

    /**
     * Convert a parquet cell into an output cell:
     * - null -> empty
     * - primitive -> string/bytes
     * - BINARY/FIXED_LEN_BYTE_ARRAY -> raw bytes
     */
    private CsvUtil.Cell cellValueToCell(Group g, int fieldIndex, Type fieldType) {
        if (g.getFieldRepetitionCount(fieldIndex) == 0) {
            return CsvUtil.Cell.empty();
        }

        if (!fieldType.isPrimitive()) {
            return CsvUtil.Cell.ofString(g.getGroup(fieldIndex, 0).toString());
        }

        PrimitiveType pt = fieldType.asPrimitiveType();
        PrimitiveTypeName ptn = pt.getPrimitiveTypeName();

        return switch (ptn) {
            case INT32 -> CsvUtil.Cell.ofString(Integer.toString(g.getInteger(fieldIndex, 0)));
            case INT64 -> CsvUtil.Cell.ofString(Long.toString(g.getLong(fieldIndex, 0)));
            case FLOAT -> CsvUtil.Cell.ofString(Float.toString(g.getFloat(fieldIndex, 0)));
            case DOUBLE -> CsvUtil.Cell.ofString(Double.toString(g.getDouble(fieldIndex, 0)));
            case BOOLEAN -> CsvUtil.Cell.ofString(Boolean.toString(g.getBoolean(fieldIndex, 0)));

            case INT96 -> {
                Binary b = g.getInt96(fieldIndex, 0);
                Instant instant = Int96Util.int96ToInstant(b.getBytes());
                yield CsvUtil.Cell.ofString(instant.toString());
            }

            case BINARY, FIXED_LEN_BYTE_ARRAY -> {
                Binary b = g.getBinary(fieldIndex, 0);
                yield CsvUtil.Cell.ofBytes(b.getBytes());
            }

            default -> CsvUtil.Cell.ofString(g.getValueToString(fieldIndex, 0));
        };
    }

    private boolean isClientAbort(IOException e) {
        String msg = (e.getMessage() == null) ? "" : e.getMessage().toLowerCase();
        return msg.contains("broken pipe")
                || msg.contains("connection reset")
                || msg.contains("forcibly closed")
                || msg.contains("abort");
    }
}
