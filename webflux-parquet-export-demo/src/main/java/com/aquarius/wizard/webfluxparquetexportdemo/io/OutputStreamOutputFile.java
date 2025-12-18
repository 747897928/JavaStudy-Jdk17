package com.aquarius.wizard.webfluxparquetexportdemo.io;

import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Parquet {@link OutputFile} backed by a plain {@link OutputStream}.
 * <p>
 * Parquet writers close their {@link PositionOutputStream} on completion.
 * When used with WebFlux {@code DataBufferUtils.outputStreamPublisher}, the underlying HTTP response stream
 * must be managed by Spring, so callers should wrap the provided stream with {@code StreamUtils.nonClosing(...)}.
 */
public final class OutputStreamOutputFile implements OutputFile {

    private final OutputStream out;
    private final String pathHint;

    public OutputStreamOutputFile(OutputStream out, String pathHint) {
        this.out = out;
        this.pathHint = pathHint;
    }

    @Override
    public PositionOutputStream create(long blockSizeHint) {
        return new OutputStreamPositionOutputStream(out);
    }

    @Override
    public PositionOutputStream createOrOverwrite(long blockSizeHint) {
        return new OutputStreamPositionOutputStream(out);
    }

    @Override
    public boolean supportsBlockSize() {
        return false;
    }

    @Override
    public long defaultBlockSize() {
        return 0;
    }

    @Override
    public String getPath() {
        return pathHint;
    }

    private static final class OutputStreamPositionOutputStream extends PositionOutputStream {
        private final OutputStream delegate;
        private long pos;

        private OutputStreamPositionOutputStream(OutputStream delegate) {
            this.delegate = delegate;
        }

        @Override
        public long getPos() {
            return pos;
        }

        @Override
        public void write(int b) throws IOException {
            delegate.write(b);
            pos++;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            delegate.write(b, off, len);
            pos += len;
        }

        @Override
        public void flush() throws IOException {
            delegate.flush();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }
}
