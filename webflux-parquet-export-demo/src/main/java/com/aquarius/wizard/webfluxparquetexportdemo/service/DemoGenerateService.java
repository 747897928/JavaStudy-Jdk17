package com.aquarius.wizard.webfluxparquetexportdemo.service;

import com.aquarius.wizard.webfluxparquetexportdemo.util.RandomNameGenerator;
import org.reactivestreams.Publisher;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

/**
 * Demo API: generate a Parquet file and stream it directly to the client.
 * <p>
 * No local file is created for this endpoint.
 */
@Service
public class DemoGenerateService {

    private final ParquetGenerateService parquetGenerateService;

    public DemoGenerateService(ParquetGenerateService parquetGenerateService) {
        this.parquetGenerateService = parquetGenerateService;
    }

    public Mono<Void> generate(long rows, ServerHttpResponse response) {
        applyStandardDownloadHeaders(response);

        String baseName = RandomNameGenerator.randomAlphaNumeric(12);
        String filename = baseName + ".parquet";
        response.getHeaders().setContentType(MediaType.APPLICATION_OCTET_STREAM);
        response.getHeaders().set(HttpHeaders.CONTENT_DISPOSITION,
                "attachment; filename=\"" + filename + "\"");

        Publisher<DataBuffer> body = parquetGenerateService.streamGeneratedParquet(rows, response.bufferFactory(), filename);
        return response.writeWith(body);
    }

    private void applyStandardDownloadHeaders(ServerHttpResponse response) {
        response.getHeaders().set(HttpHeaders.CACHE_CONTROL, "no-store");
        response.getHeaders().set(HttpHeaders.PRAGMA, "no-cache");
        response.getHeaders().set("X-Content-Type-Options", "nosniff");
    }
}

