package com.aquarius.wizard.webfluxparquetexportdemo.controller;

import com.aquarius.wizard.webfluxparquetexportdemo.model.FileFormat;
import com.aquarius.wizard.webfluxparquetexportdemo.service.ParquetExportService;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.io.File;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;

@RestController
@RequestMapping("/demo")
public class DemoController {

    private final ParquetExportService service;

    public DemoController(ParquetExportService service) {
        this.service = service;
    }

    /**
     * Generate a local parquet file at: {@code ./data/demo.parquet} (relative to user.dir).
     *
     * Example:
     * {@code POST http://localhost:8080/demo/generate?rows=800000}
     */
    @PostMapping("/generate")
    public Mono<Map<String, Object>> generate(@RequestParam(defaultValue = "500000") long rows) {
        return service.generateDemoParquet(rows)
                .map(path -> {
                    Map<String, Object> resp = new LinkedHashMap<>();
                    resp.put("parquetPath", path.toAbsolutePath().toString());
                    resp.put("rows", rows);
                    resp.put("sizeBytes", service.size(path));
                    return resp;
                });
    }

    /**
     * Download as parquet/csv/zip:
     * {@code GET /demo/download?format=parquet|csv|zip&source=s3a://bucket/key.parquet}
     * <p>
     * In production, {@code source} would be used to open a remote InputStream (S3/GCS),
     * download the Parquet to a local temp file, then export it.
     * <p>
     * In this demo we simulate that flow by generating a random parquet file and copying it via InputStream,
     * then deleting the temp parquet after the response completes (or when the client cancels).
     * <p>
     * Notes:
     * <ul>
     *   <li>We return the response body as a {@link Publisher} of {@link DataBuffer}.</li>
     *   <li>{@code response.writeWith(...)} is backpressure-aware: if the client is slow, the server writes slower.</li>
     *   <li>For CSV/ZIP formats, we use Spring's {@code DataBufferUtils.outputStreamPublisher} to bridge blocking IO
     *       (ParquetReader/ZipOutputStream) into a streaming HTTP response without keeping the whole file in memory.</li>
     * </ul>
     */
    @GetMapping("/download")
    public Mono<Void> download(@RequestParam(defaultValue = "zip") FileFormat format,
                               @RequestParam(required = false) String source,
                               @RequestParam(required = false) Long rows,
                               ServerHttpResponse response) {
        Function<ParquetExportService.TemporaryParquet, Mono<Void>> cleanup =
                temp -> service.deleteTemporaryParquet(temp.localParquetPath());

        return Mono.usingWhen(
                service.prepareTemporaryParquet(source, rows),
                temp -> writeDownloadResponse(temp.localParquetPath().toFile(), format, response),
                cleanup,
                (temp, error) -> cleanup.apply(temp),
                cleanup
        );
    }

    private Mono<Void> writeDownloadResponse(File parquetFile,
                                             FileFormat format,
                                             ServerHttpResponse response) {
        applyStandardDownloadHeaders(response);

        DownloadSpec spec = DownloadSpec.from(format, parquetFile);
        response.getHeaders().setContentType(spec.contentType());
        response.getHeaders().set(HttpHeaders.CONTENT_DISPOSITION,
                "attachment; filename=\"" + spec.filename() + "\"");

        Publisher<DataBuffer> body = service.export(parquetFile.toPath(), format, response.bufferFactory());
        return response.writeWith(body);
    }

    private static void applyStandardDownloadHeaders(ServerHttpResponse response) {
        response.getHeaders().set(HttpHeaders.CACHE_CONTROL, "no-store");
        response.getHeaders().set(HttpHeaders.PRAGMA, "no-cache");
        response.getHeaders().set("X-Content-Type-Options", "nosniff");
    }

    private record DownloadSpec(String filename, MediaType contentType) {
        private static DownloadSpec from(FileFormat format, File parquetFile) {
            String parquetFilename = parquetFile.getName();
            String baseName = baseNameFromParquetFilename(parquetFilename);
            return switch (format) {
                case PARQUET -> new DownloadSpec(parquetFilename, MediaType.APPLICATION_OCTET_STREAM);
                case CSV -> new DownloadSpec(baseName + ".csv", new MediaType("text", "csv", ParquetExportService.CSV_CHARSET));
                case ZIP -> new DownloadSpec(baseName + ".zip", new MediaType("application", "zip"));
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
