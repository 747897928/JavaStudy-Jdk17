package com.aquarius.wizard.webfluxparquetexportdemo.service;

import com.aquarius.wizard.webfluxparquetexportdemo.config.ExportProperties;
import com.aquarius.wizard.webfluxparquetexportdemo.demo.DemoParquetGenerator;
import com.aquarius.wizard.webfluxparquetexportdemo.io.CountingOutputStream;
import com.aquarius.wizard.webfluxparquetexportdemo.io.OutputStreamOutputFile;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.apache.parquet.io.OutputFile;
import org.reactivestreams.Publisher;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.util.StreamUtils;
import org.springframework.web.server.ResponseStatusException;
import reactor.netty.channel.AbortedException;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.channels.ClosedChannelException;
import java.util.Locale;
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
    private final MeterRegistry meterRegistry;
    private final DemoParquetGenerator demoParquetGenerator;
    private final Counter rejectedCounter;
    private final Counter bytesCounter;
    private final Timer durationTimer;

    public ParquetGenerateService(ExportProperties props, ExecutorService exportExecutor, MeterRegistry meterRegistry) {
        this.props = props;
        this.exportExecutor = exportExecutor;
        this.meterRegistry = meterRegistry;
        this.demoParquetGenerator = new DemoParquetGenerator();
        this.rejectedCounter = Counter.builder("demo.export.rejections")
                .tag("operation", "generate")
                .tag("format", "parquet")
                .register(meterRegistry);
        this.bytesCounter = Counter.builder("demo.export.bytes")
                .tag("operation", "generate")
                .tag("format", "parquet")
                .register(meterRegistry);
        this.durationTimer = Timer.builder("demo.export.duration")
                .tag("operation", "generate")
                .tag("format", "parquet")
                .register(meterRegistry);
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
        Timer.Sample sample = Timer.start(meterRegistry);
        CountingOutputStream countingOut = null;
        try {
            OutputStream nonClosing = StreamUtils.nonClosing(rawOut);
            countingOut = new CountingOutputStream(nonClosing);
            OutputFile outputFile = new OutputStreamOutputFile(countingOut, filenameHint);
            demoParquetGenerator.generateParquetFile(outputFile, rows, new Random(1234567L));
            countingOut.flush();
        } catch (IOException e) {
            if (isClientAbort(e)) {
                return;
            }
            throw new UncheckedIOException(e);
        } catch (RuntimeException e) {
            if (isClientAbort(e)) {
                return;
            }
            throw e;
        } finally {
            if (countingOut != null) {
                bytesCounter.increment(countingOut.getCount());
            }
            sample.stop(durationTimer);
        }
    }

    private RuntimeException exportRejected(RejectedExecutionException ex) {
        rejectedCounter.increment();
        return new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE,
                "Export system is busy (executor rejected the task)", ex);
    }

    private boolean isClientAbort(Throwable error) {
        for (Throwable throwable = error; throwable != null; throwable = throwable.getCause()) {
            if (isClientAbortThrowable(throwable)) {
                return true;
            }
        }
        return false;
    }

    private boolean isClientAbortThrowable(Throwable throwable) {
        if (throwable instanceof AbortedException) {
            return true;
        }
        if (AbortedException.isConnectionReset(throwable)) {
            return true;
        }
        if (throwable instanceof ClosedChannelException) {
            return true;
        }
        if (throwable instanceof IOException ioException) {
            String msg = (ioException.getMessage() == null) ? "" : ioException.getMessage().toLowerCase(Locale.ROOT);
            return msg.contains("broken pipe")
                    || msg.contains("connection reset")
                    || msg.contains("reset by peer")
                    || msg.contains("forcibly closed")
                    || msg.contains("clientabort")
                    || msg.contains("abort");
        }
        return false;
    }
}
