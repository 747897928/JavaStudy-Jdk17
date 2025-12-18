package com.aquarius.wizard.webfluxparquetexportdemo.service;

import com.aquarius.wizard.webfluxparquetexportdemo.config.ExportProperties;
import com.aquarius.wizard.webfluxparquetexportdemo.demo.DemoParquetGenerator;
import com.aquarius.wizard.webfluxparquetexportdemo.io.OutputStreamOutputFile;
import org.apache.parquet.io.OutputFile;
import org.reactivestreams.Publisher;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.util.StreamUtils;
import org.springframework.web.server.ResponseStatusException;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;

/**
 * Stream-generate Parquet content without creating a local file.
 * <p>
 * This is useful for demo/testing APIs where you want to produce a Parquet download on demand.
 */
@Service
public class ParquetGenerateService {

    private final ExportProperties props;
    private final ExecutorService exportExecutor;
    private final DemoParquetGenerator demoParquetGenerator;

    public ParquetGenerateService(ExportProperties props, ExecutorService exportExecutor) {
        this.props = props;
        this.exportExecutor = exportExecutor;
        this.demoParquetGenerator = new DemoParquetGenerator();
    }

    public Publisher<DataBuffer> streamGeneratedParquet(long rows, DataBufferFactory bufferFactory, String filenameHint) {
        return reactor.core.publisher.Flux
                .from(DataBufferUtils.outputStreamPublisher(
                        os -> writeParquetTo(os, rows, filenameHint),
                        bufferFactory,
                        exportExecutor,
                        props.getChunkSize()
                ))
                .onErrorMap(RejectedExecutionException.class, this::exportRejected);
    }

    private void writeParquetTo(OutputStream rawOut, long rows, String filenameHint) {
        try {
            OutputStream nonClosing = StreamUtils.nonClosing(rawOut);
            OutputFile outputFile = new OutputStreamOutputFile(nonClosing, filenameHint);
            demoParquetGenerator.generateParquetFile(outputFile, rows, new Random(1234567L));
            nonClosing.flush();
        } catch (IOException e) {
            if (isClientAbort(e)) {
                return;
            }
            throw new UncheckedIOException(e);
        }
    }

    private RuntimeException exportRejected(RejectedExecutionException ex) {
        return new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE,
                "Export system is busy (executor rejected the task)", ex);
    }

    private boolean isClientAbort(IOException e) {
        String msg = (e.getMessage() == null) ? "" : e.getMessage().toLowerCase();
        return msg.contains("broken pipe")
                || msg.contains("connection reset")
                || msg.contains("forcibly closed")
                || msg.contains("abort");
    }
}
