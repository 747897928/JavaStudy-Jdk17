package com.aquarius.wizard.webfluxparquetexportdemo.controller;

import com.aquarius.wizard.webfluxparquetexportdemo.model.FileFormat;
import com.aquarius.wizard.webfluxparquetexportdemo.service.DemoDownloadService;
import com.aquarius.wizard.webfluxparquetexportdemo.service.DemoGenerateService;
import org.reactivestreams.Publisher;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/demo")
public class DemoController {

    private final DemoGenerateService generateService;
    private final DemoDownloadService downloadService;

    public DemoController(DemoGenerateService generateService, DemoDownloadService downloadService) {
        this.generateService = generateService;
        this.downloadService = downloadService;
    }

    /**
     * Generate a Parquet file and stream it directly to the client.
     */
    @PostMapping("/generate")
    public Mono<Void> generate(@RequestParam(defaultValue = "500000") long rows,
                               ServerHttpResponse response) {
        return generateService.generate(rows, response);
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
     *   <li>WebFlux ultimately writes a {@link Publisher} of {@link DataBuffer} to the HTTP response.</li>
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
        return downloadService.download(format, source, rows, response);
    }
}
