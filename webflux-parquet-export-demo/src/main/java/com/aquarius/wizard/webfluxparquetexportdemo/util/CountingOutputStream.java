package com.aquarius.wizard.webfluxparquetexportdemo.util;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Simple OutputStream wrapper that counts bytes written.
 * <p>
 * Used to implement "flush every N bytes" without adding per-byte branching logic in hot paths
 * (the flush decision can be made at row boundaries).
 * <p>
 * Beginner note: the counter increases when our code calls {@code write(...)} on this stream.
 * It does not try to guess how many bytes have reached the network; it only measures how much we
 * have produced so far.
 */
public final class CountingOutputStream extends FilterOutputStream {

    private long count;

    public CountingOutputStream(OutputStream out) {
        super(out);
    }

    public long getCount() {
        return count;
    }

    @Override
    public void write(int b) throws IOException {
        out.write(b);
        count++;
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        out.write(b, off, len);
        count += len;
    }
}
