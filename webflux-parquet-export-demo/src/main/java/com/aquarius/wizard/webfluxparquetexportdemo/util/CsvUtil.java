package com.aquarius.wizard.webfluxparquetexportdemo.util;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;

/**
 * CSV writer designed for streaming:
 * - no per-row Map allocation
 * - writes directly to OutputStream
 * - RFC4180 minimal escaping (comma, quote, CR/LF => quoted; quote => doubled)
 * <p>
 * Note:
 * <ul>
 *   <li>CSV is just bytes. We write bytes to the HTTP response as we read rows from Parquet.</li>
 *   <li>For {@code BINARY} columns, this demo outputs Base64 text so that the CSV stays valid UTF-8.</li>
 * </ul>
 */
public final class CsvUtil {

    private CsvUtil() {
    }

    @FunctionalInterface
    public interface ValueExtractor {
        /**
         * @return {@code null} for an empty cell; {@code String/Number/Boolean/...} for textual output.
         */
        Object getValue(Group rowGroup, Type fieldType, int fieldIndex);
    }

    public static void writeHeader(OutputStream out, MessageType schema, Charset charset) throws IOException {
        int n = schema.getFieldCount();
        for (int i = 0; i < n; i++) {
            if (i > 0) out.write(',');
            writeCsvEscapedString(out, schema.getFieldName(i), charset);
        }
        out.write('\n');
    }

    public static void writeRow(OutputStream out,
                                Group rowGroup,
                                MessageType schema,
                                Charset charset,
                                ValueExtractor extractor) throws IOException {

        int n = schema.getFieldCount();
        for (int i = 0; i < n; i++) {
            if (i > 0) out.write(',');

            Type fieldType = schema.getType(i);
            Object cellValue = extractor.getValue(rowGroup, fieldType, i);

            if (cellValue == null) {
                continue; // null -> empty
            }

            if (cellValue instanceof String text) {
                writeCsvEscapedString(out, text, charset);
            } else {
                writeCsvEscapedString(out, cellValue.toString(), charset);
            }
        }
        out.write('\n');
    }

    private static void writeCsvEscapedString(OutputStream out, String s, Charset charset) throws IOException {
        if (s == null) {
            return;
        }
        writeCsvEscapedBytes(out, s.getBytes(charset));
    }

    /**
     * RFC4180 minimal escaping:
     * - If bytes contain comma, quote, CR or LF => wrap with quotes.
     * - Quote byte (") becomes doubled ("").
     */
    private static void writeCsvEscapedBytes(OutputStream out, byte[] bytes) throws IOException {
        if (bytes == null || bytes.length == 0) {
            return;
        }

        boolean needQuote = false;
        for (byte b : bytes) {
            if (b == ',' || b == '"' || b == '\n' || b == '\r') {
                needQuote = true;
                break;
            }
        }

        if (!needQuote) {
            out.write(bytes);
            return;
        }

        out.write('"');
        for (byte b : bytes) {
            if (b == '"') {
                out.write('"');
                out.write('"');
            } else {
                out.write(b);
            }
        }
        out.write('"');
    }
}
