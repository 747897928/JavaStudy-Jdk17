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
 * Beginner note:
 * <ul>
 *   <li>CSV is just bytes. We write bytes to the HTTP response as we read rows from Parquet.</li>
 *   <li>For {@code BINARY} columns, this demo writes the raw bytes into CSV and relies on the CSV file being
 *       interpreted as ISO-8859-1 to keep a 1:1 mapping between byte values (0..255) and characters.</li>
 * </ul>
 */
public final class CsvUtil {

    private CsvUtil() {
    }

    /**
     * CSV cell content: either String (text), raw bytes, or empty (null -> empty cell).
     */
    public sealed interface Cell permits CellString, CellBytes, CellEmpty {
        static Cell ofString(String s) {
            return new CellString(s);
        }

        static Cell ofBytes(byte[] b) {
            return new CellBytes(b);
        }

        static Cell empty() {
            return new CellEmpty();
        }
    }

    public record CellString(String value) implements Cell {
    }

    public record CellBytes(byte[] bytes) implements Cell {
    }

    public record CellEmpty() implements Cell {
    }

    @FunctionalInterface
    public interface CellExtractor {
        Cell extract(Group g, int fieldIndex, Type fieldType);
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
                                Group g,
                                MessageType schema,
                                Charset charset,
                                CellExtractor extractor) throws IOException {

        int n = schema.getFieldCount();
        for (int i = 0; i < n; i++) {
            if (i > 0) out.write(',');

            Type t = schema.getType(i);
            Cell cell = extractor.extract(g, i, t);

            if (cell instanceof CellEmpty) {
                continue; // null -> empty
            }
            if (cell instanceof CellString cs) {
                writeCsvEscapedString(out, cs.value(), charset);
                continue;
            }
            if (cell instanceof CellBytes cb) {
                // "raw bytes" cell: still apply CSV escaping to keep output parseable.
                writeCsvEscapedBytes(out, cb.bytes());
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
