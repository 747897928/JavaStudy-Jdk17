package com.aquarius.wizard.webfluxparquetexportdemo.service;

import com.aquarius.wizard.webfluxparquetexportdemo.demo.DemoParquetGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Stage a remote Parquet object (S3/GCS) to a local temp file and return it as a {@link File}.
 * <p>
 * Why this exists:
 * <ul>
 *   <li>Parquet readers typically need a {@code Path/HadoopInputFile}, so we must have a local file.</li>
 *   <li>We want a clean API to integrate elsewhere: input is a URL like {@code s3a://.../abc.parquet}, output is
 *       a local {@link File} with the same file name ({@code abc.parquet}).</li>
 *   <li>Cleanup is best-effort and happens on complete/error/cancel of the HTTP response.</li>
 * </ul>
 */
@Service
public class ParquetStagingService {

    private static final Logger log = LoggerFactory.getLogger(ParquetStagingService.class);

    private final Scheduler exportScheduler;
    private final DemoParquetGenerator demoParquetGenerator;

    public ParquetStagingService(Scheduler exportScheduler) {
        this.exportScheduler = exportScheduler;
        this.demoParquetGenerator = new DemoParquetGenerator();
    }

    /**
     * Stage a remote parquet to local temp storage and return the local file.
     * <p>
     * In production you would:
     * <ul>
     *   <li>open an InputStream from S3/GCS by {@code sourceUrl}</li>
     *   <li>stream it to {@code localParquet}</li>
     * </ul>
     * In this demo we simulate the remote object by generating a parquet file locally and copying it through an
     * {@link InputStream} to exercise the same code path.
     */
    public Mono<File> stageParquetFromSource(String sourceUrl, Long rows) {
        return runOnExportScheduler(() -> stageParquetFromSourceBlocking(sourceUrl, rows));
    }

    /**
     * Delete the staged parquet file and its per-request directory (best effort).
     */
    public Mono<Void> deleteStagedParquet(File stagedParquet) {
        return Mono.fromRunnable(() -> deleteStagedParquetBlocking(stagedParquet))
                .subscribeOn(exportScheduler)
                .onErrorResume(RejectedExecutionException.class, ex -> {
                    deleteStagedParquetBlocking(stagedParquet);
                    return Mono.empty();
                })
                .then();
    }

    private File stageParquetFromSourceBlocking(String sourceUrl, Long rows) throws IOException {
        Path tmpRoot = Path.of(System.getProperty("user.dir"), "data", "tmp");
        Files.createDirectories(tmpRoot);

        String baseName = sanitizeBaseNameFromUrl(sourceUrl);
        if (baseName.isBlank()) {
            baseName = randomSafeName(12);
        }

        long rowsToGenerate = (rows != null && rows > 0) ? rows : pickRandomRows();

        // Use a per-request directory so we can safely keep the local parquet name stable (e.g. abc123.parquet)
        // without worrying about collisions between concurrent requests.
        Path requestDir = Files.createTempDirectory(tmpRoot, baseName + "_");
        Path remoteParquet = requestDir.resolve(baseName + "_remote.parquet");
        Path localParquet = requestDir.resolve(baseName + ".parquet");

        try {
            // 1) Simulate remote object: generate parquet to remoteParquet.
            demoParquetGenerator.generateParquetFile(remoteParquet, rowsToGenerate, new Random(ThreadLocalRandom.current().nextLong()));

            // 2) Simulate: download stream -> local file.
            try (InputStream inputStream = Files.newInputStream(remoteParquet)) {
                Files.copy(inputStream, localParquet, StandardCopyOption.REPLACE_EXISTING);
            }

            return localParquet.toFile();
        } catch (IOException e) {
            // Best effort cleanup on failure.
            deleteDirectoryRecursively(requestDir);
            throw e;
        } finally {
            Files.deleteIfExists(remoteParquet);
        }
    }

    private void deleteStagedParquetBlocking(File stagedParquet) {
        if (stagedParquet == null) {
            return;
        }
        Path filePath = stagedParquet.toPath();
        Path parentDir = filePath.getParent();
        if (parentDir == null) {
            try {
                Files.deleteIfExists(filePath);
            } catch (IOException e) {
                log.debug("Failed to delete staged parquet: {}", filePath, e);
            }
            return;
        }

        deleteDirectoryRecursively(parentDir);
    }

    private void deleteDirectoryRecursively(Path dir) {
        if (dir == null) {
            return;
        }
        try {
            if (!Files.exists(dir)) {
                return;
            }
            Files.walkFileTree(dir, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Files.deleteIfExists(file);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path directory, IOException exc) throws IOException {
                    Files.deleteIfExists(directory);
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            log.debug("Failed to delete staged directory: {}", dir, e);
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

    private static String randomSafeName(int length) {
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

    private <T> Mono<T> runOnExportScheduler(Callable<T> task) {
        return Mono.fromCallable(task)
                .subscribeOn(exportScheduler)
                .onErrorMap(RejectedExecutionException.class, ex ->
                        new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE,
                                "Export system is busy (executor rejected the task)", ex))
                .onErrorMap(IOException.class, UncheckedIOException::new);
    }
}

