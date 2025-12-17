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
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.StandardCopyOption;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
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
     * For BINARY: we output raw bytes to the CSV stream and ask the client to interpret CSV as ISO-8859-1 so that
     * bytes(0..255) map 1:1 to characters.
     */
    public static final Charset CSV_CHARSET = StandardCharsets.ISO_8859_1;

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
    private final Scheduler exportScheduler;
    private final AtomicReference<java.nio.file.Path> demoParquetPath = new AtomicReference<>();

    public ParquetExportService(ExportProperties props, ExecutorService exportExecutor, Scheduler exportScheduler) {
        this.props = props;
        this.exportExecutor = exportExecutor;
        this.exportScheduler = exportScheduler;
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

    public record TemporaryParquet(java.nio.file.Path localParquetPath, String baseName) {
    }

    /**
     * Simulate: user provides a Parquet download URL (s3a:// or gs://), we obtain an InputStream,
     * then write that InputStream to a local temp parquet file for {@link ParquetReader} to read.
     * <p>
     * In this demo project we don't actually call S3/GCS. Instead, we generate a parquet file with random size and name,
     * open it as an InputStream, and copy it to another "local" temp parquet file (to exercise the same code path).
     */
    public Mono<TemporaryParquet> prepareTemporaryParquet(String sourceUrl, Long rows) {
        return runOnExportScheduler(() -> prepareTemporaryParquetBlocking(sourceUrl, rows));
    }

    public Mono<Void> deleteTemporaryParquet(java.nio.file.Path parquetPath) {
        return Mono.fromRunnable(() -> deleteTemporaryParquetBlocking(parquetPath))
                // Cleanup should not depend on executor availability; if we can't schedule, just run inline.
                .subscribeOn(exportScheduler)
                .onErrorResume(RejectedExecutionException.class, ex -> {
                    deleteTemporaryParquetBlocking(parquetPath);
                    return Mono.empty();
                })
                .then();
    }

    private void deleteTemporaryParquetBlocking(java.nio.file.Path parquetPath) {
        if (parquetPath == null) {
            return;
        }
        try {
            Files.deleteIfExists(parquetPath);
        } catch (IOException e) {
            // Best effort cleanup. The temp file may already be deleted or locked by OS/AV.
            log.debug("Failed to delete temporary parquet: {}", parquetPath, e);
        }
    }

    /**
     * Generate a Parquet file at {@code ./data/demo.parquet} (relative to user.dir).
     */
    public Mono<java.nio.file.Path> generateDemoParquet(long rows) {
        return runOnExportScheduler(() -> generateDemoParquetBlocking(rows));
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

    private java.nio.file.Path generateDemoParquetBlocking(long rows) throws IOException {
        java.nio.file.Path dir = Paths.get(System.getProperty("user.dir"), "data");
        Files.createDirectories(dir);

        java.nio.file.Path out = dir.resolve("demo.parquet");
        generateParquetFile(out, rows, new Random(1234567L));
        demoParquetPath.set(out);
        return out;
    }

    private TemporaryParquet prepareTemporaryParquetBlocking(String sourceUrl, Long rows) throws IOException {
        java.nio.file.Path tmpDir = Paths.get(System.getProperty("user.dir"), "data", "tmp");
        Files.createDirectories(tmpDir);

        ThreadLocalRandom random = ThreadLocalRandom.current();
        String sourceBaseName = sanitizeBaseNameFromUrl(sourceUrl);
        String randomSuffix = randomSafeName(8);
        String baseName = sourceBaseName.isEmpty() ? randomSafeName(12) : sourceBaseName + "_" + randomSuffix;

        long rowsToGenerate = (rows != null && rows > 0) ? rows : pickRandomRows();

        // "Remote" parquet file (simulated).
        java.nio.file.Path remoteParquet = tmpDir.resolve(baseName + "_remote.parquet");
        // "Local" parquet file (what our export pipeline will read).
        java.nio.file.Path localParquet = tmpDir.resolve(baseName + ".parquet");

        // 1) Simulate remote object: generate parquet to remoteParquet.
        generateParquetFile(remoteParquet, rowsToGenerate, new Random(random.nextLong()));

        // 2) Simulate: download stream -> local file.
        try (InputStream inputStream = Files.newInputStream(remoteParquet)) {
            Files.copy(inputStream, localParquet, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            Files.deleteIfExists(localParquet);
            throw e;
        } finally {
            Files.deleteIfExists(remoteParquet);
        }

        return new TemporaryParquet(localParquet, baseName);
    }

    private void generateParquetFile(java.nio.file.Path out, long rows, Random randomGenerator) throws IOException {

        MessageType schema = MessageTypeParser.parseMessageType(DEMO_SCHEMA);
        Configuration conf = new Configuration();
        GroupWriteSupport.setSchema(schema, conf);

        SimpleGroupFactory factory = new SimpleGroupFactory(schema);

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

            for (long rowIndex = 0; rowIndex < rows; rowIndex++) {
                Group rowGroup = factory.newGroup();

                // Make some columns null randomly (optional fields), to test: null -> empty
                if (randomGenerator.nextInt(10) != 0) rowGroup.add("i32", randomGenerator.nextInt());
                if (randomGenerator.nextInt(10) != 0) rowGroup.add("i64", randomGenerator.nextLong());
                if (randomGenerator.nextInt(10) != 0) {
                    Instant now = Instant.ofEpochMilli(System.currentTimeMillis() + randomGenerator.nextInt(1_000_000));
                    rowGroup.add("t96", Binary.fromConstantByteArray(Int96Util.instantToInt96(now)));
                }
                if (randomGenerator.nextInt(10) != 0) rowGroup.add("f32", randomGenerator.nextFloat());
                if (randomGenerator.nextInt(10) != 0) rowGroup.add("b", randomGenerator.nextBoolean());
                if (randomGenerator.nextInt(10) != 0) rowGroup.add("d64", randomGenerator.nextDouble());

                if (randomGenerator.nextInt(10) != 0) {
                    // ASCII bytes are easier to eyeball in CSV, but export logic supports arbitrary bytes.
                    byte[] randomAsciiBytes = new byte[16];
                    for (int byteIndex = 0; byteIndex < randomAsciiBytes.length; byteIndex++) {
                        int ch = 33 + randomGenerator.nextInt(94); // '!'..'~'
                        randomAsciiBytes[byteIndex] = (byte) ch;
                    }
                    rowGroup.add("bin", Binary.fromConstantByteArray(randomAsciiBytes));
                }

                writer.write(rowGroup);
            }
        }
    }

    private long pickRandomRows() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        // Weighted distribution to cover small/medium/large files in a dev-friendly way.
        int bucket = random.nextInt(100);
        if (bucket < 40) {
            return random.nextLong(200, 10_000);           // tiny/small
        }
        if (bucket < 85) {
            return random.nextLong(50_000, 400_000);       // medium
        }
        return random.nextLong(700_000, 2_000_000);        // large
    }

    private static String sanitizeBaseNameFromUrl(String sourceUrl) {
        if (sourceUrl == null || sourceUrl.isBlank()) {
            return "";
        }
        String trimmed = sourceUrl.trim();
        int slash = Math.max(trimmed.lastIndexOf('/'), trimmed.lastIndexOf(':'));
        String lastPart = (slash >= 0) ? trimmed.substring(slash + 1) : trimmed;
        if (lastPart.endsWith(".parquet")) {
            lastPart = lastPart.substring(0, lastPart.length() - ".parquet".length());
        }
        // Keep only [A-Za-z0-9_], replace others with underscore, and collapse multiple underscores.
        StringBuilder sb = new StringBuilder(lastPart.length());
        char prev = 0;
        for (int i = 0; i < lastPart.length(); i++) {
            char c = lastPart.charAt(i);
            char normalized = isAllowedNameChar(c) ? c : '_';
            if (normalized == '_' && prev == '_') {
                continue;
            }
            sb.append(normalized);
            prev = normalized;
        }
        String sanitized = sb.toString();
        // Trim underscores.
        int start = 0;
        int end = sanitized.length();
        while (start < end && sanitized.charAt(start) == '_') start++;
        while (end > start && sanitized.charAt(end - 1) == '_') end--;
        return sanitized.substring(start, end);
    }

    private String randomSafeName(int length) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        final char[] alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_".toCharArray();
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append(alphabet[random.nextInt(alphabet.length)]);
        }
        return sb.toString();
    }

    private static boolean isAllowedNameChar(char c) {
        return (c >= 'a' && c <= 'z')
                || (c >= 'A' && c <= 'Z')
                || (c >= '0' && c <= '9')
                || c == '_';
    }

    /**
     * Export as PARQUET/CSV/ZIP(CSV) using streaming.
     */
    public Publisher<DataBuffer> export(java.nio.file.Path parquetFile, FileFormat format, DataBufferFactory bufferFactory) {
        return export(parquetFile, format, bufferFactory, "data");
    }

    /**
     * Export with a preferred base name.
     * <p>
     * For ZIP, this controls the CSV entry name inside the ZIP: {@code <baseName>.csv}.
     */
    public Publisher<DataBuffer> export(java.nio.file.Path parquetFile,
                                        FileFormat format,
                                        DataBufferFactory bufferFactory,
                                        String baseName) {
        String safeBaseName = (baseName == null || baseName.isBlank()) ? "data" : baseName;
        String zipEntryName = sanitizeZipEntryName(safeBaseName + ".csv");

        return exportFlux(parquetFile, format, bufferFactory, zipEntryName);
    }

    private Publisher<DataBuffer> exportFlux(java.nio.file.Path parquetFile,
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

    private <T> Mono<T> runOnExportScheduler(Callable<T> task) {
        return Mono.fromCallable(task)
                .subscribeOn(exportScheduler)
                .onErrorMap(RejectedExecutionException.class, this::exportRejected);
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
        Configuration conf = new Configuration();
        Path parquetPath = new Path(parquetFile.toUri());

        long flushThresholdBytes = Math.max(0, flushEveryBytes);
        long maxRows = props.getMaxRows() <= 0 ? Long.MAX_VALUE : props.getMaxRows();

        try {
            MessageType schema = readSchema(conf, parquetPath);

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
            try (ParquetReader<Group> reader = ParquetReader.builder(readSupport, parquetPath).withConf(conf).build()) {
                long row = 0L;
                Group rowGroup;
                while ((rowGroup = reader.read()) != null) {
                    row++;
                    if (row > maxRows) {
                        break;
                    }

                    try {
                        CsvUtil.writeRow(countingOut, rowGroup, schema, CSV_CHARSET, this::getCsvCellValue);
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
     * Converts one Parquet cell to something that can be written to CSV.
     * <p>
     * Returns:
     * <ul>
     *   <li>{@code null} - empty cell</li>
     *   <li>{@code String/Number/...} - will be written as text</li>
     *   <li>{@code byte[]} - will be written as raw bytes (still CSV-escaped)</li>
     * </ul>
     * <p>
     * Note: Parquet has "physical types" (INT32/INT64/BINARY...) and optional "logical types"
     * (DATE/TIME/TIMESTAMP/DECIMAL...). The same physical INT32 can mean "int" or "date", depending on the
     * logical type annotation.
     */
    private Object getCsvCellValue(Group rowGroup, Type fieldType, int fieldIndex) {
        if (rowGroup.getFieldRepetitionCount(fieldIndex) == 0) {
            return null;
        }

        if (!fieldType.isPrimitive()) {
            return rowGroup.getGroup(fieldIndex, 0).toString();
        }

        PrimitiveType primitiveType = fieldType.asPrimitiveType();
        PrimitiveTypeName physicalType = primitiveType.getPrimitiveTypeName();
        LogicalTypeAnnotation logicalType = primitiveType.getLogicalTypeAnnotation();

        return switch (physicalType) {
            case INT32 -> formatInt32(rowGroup.getInteger(fieldIndex, 0), logicalType);
            case INT64 -> formatInt64(rowGroup.getLong(fieldIndex, 0), logicalType);
            case FLOAT -> Float.toString(rowGroup.getFloat(fieldIndex, 0));
            case DOUBLE -> Double.toString(rowGroup.getDouble(fieldIndex, 0));
            case BOOLEAN -> Boolean.toString(rowGroup.getBoolean(fieldIndex, 0));

            case INT96 -> {
                // INT96 is historically used for timestamps (e.g. older Hive/Impala writers).
                // It is not a standard logical type, but converting to Instant is the most common expectation.
                Binary int96Binary = rowGroup.getInt96(fieldIndex, 0);
                Instant instant = Int96Util.int96ToInstant(int96Binary.getBytes());
                yield instant.toString();
            }

            case BINARY, FIXED_LEN_BYTE_ARRAY -> {
                Binary binaryValue = rowGroup.getBinary(fieldIndex, 0);

                // If annotated as DECIMAL, decode to a human-readable decimal string.
                if (logicalType instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimal) {
                    BigInteger unscaled = new BigInteger(binaryValue.getBytes());
                    yield new BigDecimal(unscaled, decimal.getScale()).toPlainString();
                }

                // If annotated as STRING-like, decode UTF-8 for readability.
                // We intentionally do NOT call String.intern(): for large exports it can cause memory pressure.
                if (logicalType instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation
                        || logicalType instanceof LogicalTypeAnnotation.EnumLogicalTypeAnnotation
                        || logicalType instanceof LogicalTypeAnnotation.JsonLogicalTypeAnnotation) {
                    yield binaryValue.toStringUsingUTF8();
                }

                // Otherwise keep raw bytes (works for arbitrary binary).
                yield binaryValue.getBytes();
            }

            default -> rowGroup.getValueToString(fieldIndex, 0);
        };
    }

    private Object formatInt32(int value, LogicalTypeAnnotation logicalType) {
        if (logicalType instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation) {
            return LocalDate.ofEpochDay(value).toString();
        }
        if (logicalType instanceof LogicalTypeAnnotation.TimeLogicalTypeAnnotation time) {
            long nanos = switch (time.getUnit()) {
                case MILLIS -> (long) value * 1_000_000L;
                case MICROS -> (long) value * 1_000L;
                case NANOS -> (long) value;
            };
            return LocalTime.ofNanoOfDay(nanos).toString();
        }
        if (logicalType instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimal) {
            return BigDecimal.valueOf(value, decimal.getScale()).toPlainString();
        }
        return Integer.toString(value);
    }

    private Object formatInt64(long value, LogicalTypeAnnotation logicalType) {
        if (logicalType instanceof LogicalTypeAnnotation.TimeLogicalTypeAnnotation time) {
            long nanos = switch (time.getUnit()) {
                case MILLIS -> value * 1_000_000L;
                case MICROS -> value * 1_000L;
                case NANOS -> value;
            };
            return LocalTime.ofNanoOfDay(nanos).toString();
        }
        if (logicalType instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation timestamp) {
            Instant instant = switch (timestamp.getUnit()) {
                case MILLIS -> Instant.ofEpochMilli(value);
                case MICROS -> Instant.ofEpochSecond(
                        Math.floorDiv(value, 1_000_000L),
                        Math.floorMod(value, 1_000_000L) * 1_000L
                );
                case NANOS -> Instant.ofEpochSecond(
                        Math.floorDiv(value, 1_000_000_000L),
                        Math.floorMod(value, 1_000_000_000L)
                );
            };
            return instant.toString();
        }
        if (logicalType instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimal) {
            return BigDecimal.valueOf(value, decimal.getScale()).toPlainString();
        }
        return Long.toString(value);
    }

    private boolean isClientAbort(IOException e) {
        String msg = (e.getMessage() == null) ? "" : e.getMessage().toLowerCase();
        return msg.contains("broken pipe")
                || msg.contains("connection reset")
                || msg.contains("forcibly closed")
                || msg.contains("abort");
    }
}
