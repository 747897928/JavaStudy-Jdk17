package com.aquarius.wizard.webfluxparquetexportdemo.util;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Prevents downstream wrappers (e.g. ZipOutputStream) from closing the underlying stream.
 * <p>
 * Note:
 * <ul>
 *   <li>In this project, the HTTP response body is produced by Spring's {@code DataBufferUtils.outputStreamPublisher}.</li>
 *   <li>Spring owns the underlying {@link OutputStream} and will close it at the right time.</li>
 *   <li>Some wrappers (like {@code ZipOutputStream}) close the wrapped stream when you call {@code close()}.</li>
 *   <li>If we let them close the underlying stream early, the client will get a broken/unfinished download.</li>
 * </ul>
 */
public final class NonClosingOutputStream extends FilterOutputStream {

    public NonClosingOutputStream(OutputStream out) {
        super(out);
    }

  @Override
  public void close() throws IOException {
    // We intentionally do not close the wrapped stream here.
    // The wrapped stream is the HTTP response stream owned by Spring WebFlux; Spring will close it.
    //
    // Why do we still call flush()?
    // - Many wrappers call close() at the end of their work (e.g. ZipOutputStream in try-with-resources).
    // - We want to push any buffered bytes out, but not break the HTTP response by closing the underlying stream.
    flush();
  }
}
