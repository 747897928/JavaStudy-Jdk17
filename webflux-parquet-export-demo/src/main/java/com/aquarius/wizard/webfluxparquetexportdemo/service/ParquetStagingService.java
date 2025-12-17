package com.aquarius.wizard.webfluxparquetexportdemo.service;

import com.aquarius.wizard.webfluxparquetexportdemo.demo.DemoParquetGenerator;
import com.aquarius.wizard.webfluxparquetexportdemo.util.RandomNameGenerator;
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

        // In this demo we always use a random base name so that the download name is stable and predictable:
        // abc123.parquet -> abc123.csv -> abc123.zip (zip contains abc123.csv).
        // Only letters and digits are used to keep filenames header/ZIP-friendly.
        String baseName = RandomNameGenerator.randomAlphaNumeric(12);

        long rowsToGenerate = (rows != null && rows > 0) ? rows : pickRandomRows();

        // Use a per-request directory so we can safely keep the local parquet name stable (e.g. abc123.parquet)
        // without worrying about collisions between concurrent requests.
        Path requestDir = Files.createTempDirectory(tmpRoot, baseName + "tmp");
        Path remoteParquet = requestDir.resolve(baseName + "Remote.parquet");
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

    private <T> Mono<T> runOnExportScheduler(Callable<T> task) {
        return Mono.fromCallable(task)
                .subscribeOn(exportScheduler)
                .onErrorMap(RejectedExecutionException.class, ex ->
                        new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE,
                                "Export system is busy (executor rejected the task)", ex))
                .onErrorMap(IOException.class, UncheckedIOException::new);
    }
}
