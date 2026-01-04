package com.aquarius.wizard.webfluxparquetexportdemo.service;

import com.aquarius.wizard.webfluxparquetexportdemo.config.ExportProperties;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;

class ParquetGenerateServiceTest {

    @Test
    void streamGeneratedParquetStartsWithPar1() {
        ExportProperties props = new ExportProperties();
        props.setChunkSize(1024);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        ParquetGenerateService service = new ParquetGenerateService(props, executor, new SimpleMeterRegistry());

        Flux<DataBuffer> flux = Flux.from(service.streamGeneratedParquet(10, DefaultDataBufferFactory.sharedInstance, "x.parquet"));

        try {
            StepVerifier.create(flux.take(1))
                    .assertNext(buf -> {
                        byte[] first = new byte[Math.min(buf.readableByteCount(), 4)];
                        buf.read(first);
                        assertThat(new String(first, StandardCharsets.US_ASCII)).isEqualTo("PAR1");
                    })
                    .verifyComplete();
        } finally {
            executor.shutdownNow();
        }
    }
}
