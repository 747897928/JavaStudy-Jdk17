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

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

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
     * {@code GET /demo/download?format=parquet|csv|zip}
     * <p>
     * Notes for beginners:
     * <ul>
     *   <li>We return the response body as a {@link Publisher} of {@link DataBuffer}.</li>
     *   <li>{@code response.writeWith(...)} is backpressure-aware: if the client is slow, the server writes slower.</li>
     *   <li>For CSV/ZIP formats, we use Spring's {@code DataBufferUtils.outputStreamPublisher} to bridge blocking IO
     *       (ParquetReader/ZipOutputStream) into a streaming HTTP response without keeping the whole file in memory.</li>
     * </ul>
     */
    @GetMapping("/download")
    public Mono<Void> download(@RequestParam(defaultValue = "zip") FileFormat format,
                               ServerHttpResponse response) {
        Path parquet = service.getDemoParquetOrThrow();

        response.getHeaders().set(HttpHeaders.CACHE_CONTROL, "no-store");
        response.getHeaders().set(HttpHeaders.PRAGMA, "no-cache");
        response.getHeaders().set("X-Content-Type-Options", "nosniff");

        String filename;
        MediaType contentType;

        switch (format) {
            case PARQUET -> {
                filename = "data.parquet";
                contentType = MediaType.APPLICATION_OCTET_STREAM;
            }
            case CSV -> {
                filename = "data.csv";
                contentType = new MediaType("text", "csv", ParquetExportService.CSV_CHARSET);
            }
            case ZIP -> {
                filename = "data.zip";
                contentType = new MediaType("application", "zip");
            }
            default -> throw new IllegalArgumentException("Unsupported format: " + format);
        }

        response.getHeaders().setContentType(contentType);
        response.getHeaders().set(HttpHeaders.CONTENT_DISPOSITION,
                "attachment; filename=\"" + filename + "\"");

        Publisher<DataBuffer> body = service.export(parquet, format, response.bufferFactory());
        return response.writeWith(body);
    }
}
