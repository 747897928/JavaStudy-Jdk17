package com.aquarius.wizard.webfluxparquetexportdemo.service;

import com.aquarius.wizard.webfluxparquetexportdemo.model.FileFormat;
import org.reactivestreams.Publisher;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.io.File;
import java.util.function.Function;

/**
 * Demo API: stage a "remote" parquet (S3/GCS) to local temp storage, then stream-export it as PARQUET/CSV/ZIP.
 * <p>
 * Controller stays thin; HTTP + orchestration logic lives here.
 */
@Service
public class DemoDownloadService {

    private final ParquetExportService exportService;
    private final ParquetStagingService stagingService;

    public DemoDownloadService(ParquetExportService exportService, ParquetStagingService stagingService) {
        this.exportService = exportService;
        this.stagingService = stagingService;
    }

    public Mono<Void> download(FileFormat format,
                               String source,
                               Long rows,
                               ServerHttpResponse response) {
        exportService.assertExportCapacityOrThrow();

        Function<File, Mono<Void>> cleanup = stagingService::deleteStagedParquet;

        return Mono.usingWhen(
                stagingService.stageParquetFromSource(source, rows),
                parquetFile -> exportService.validateBeforeStreaming(parquetFile.toPath(), format)
                        .then(Mono.defer(() -> writeResponse(parquetFile, format, response))),
                cleanup,
                (parquetFile, error) -> cleanup.apply(parquetFile),
                cleanup
        );
    }

    private Mono<Void> writeResponse(File parquetFile, FileFormat format, ServerHttpResponse response) {
        applyStandardDownloadHeaders(response);

        DownloadSpec spec = DownloadSpec.from(format, parquetFile);
        response.getHeaders().setContentType(spec.contentType());
        response.getHeaders().set(HttpHeaders.CONTENT_DISPOSITION,
                "attachment; filename=\"" + spec.filename() + "\"");

        Publisher<DataBuffer> body = exportService.streamExport(parquetFile.toPath(), format, response.bufferFactory(), spec.baseName());
        return response.writeWith(body);
    }

    private void applyStandardDownloadHeaders(ServerHttpResponse response) {
        response.getHeaders().set(HttpHeaders.CACHE_CONTROL, "no-store");
        response.getHeaders().set(HttpHeaders.PRAGMA, "no-cache");
        response.getHeaders().set("X-Content-Type-Options", "nosniff");
    }

    private record DownloadSpec(String baseName, String filename, MediaType contentType) {
        private static DownloadSpec from(FileFormat format, File parquetFile) {
            String parquetFilename = parquetFile.getName();
            String baseName = baseNameFromParquetFilename(parquetFilename);
            return switch (format) {
                case PARQUET -> new DownloadSpec(baseName, parquetFilename, MediaType.APPLICATION_OCTET_STREAM);
                case CSV -> new DownloadSpec(baseName, baseName + ".csv", new MediaType("text", "csv", ParquetExportService.CSV_CHARSET));
                case ZIP -> new DownloadSpec(baseName, baseName + ".zip", new MediaType("application", "zip"));
            };
        }

        private static String baseNameFromParquetFilename(String parquetFilename) {
            if (parquetFilename == null || parquetFilename.isBlank()) {
                return "data";
            }
            String name = parquetFilename;
            if (name.endsWith(".parquet")) {
                name = name.substring(0, name.length() - ".parquet".length());
            }
            return name.isBlank() ? "data" : name;
        }
    }
}
