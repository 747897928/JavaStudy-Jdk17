package com.aquarius.wizard.webfluxparquetexportdemo.util;

import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class CsvUtilTest {

    @Test
    void writeHeaderAndRowUtf8Escaping() throws Exception {
        MessageType schema = MessageTypeParser.parseMessageType("""
                message demo {
                  optional binary a;
                  optional binary b;
                }
                """);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CsvUtil.writeHeader(out, schema, StandardCharsets.UTF_8);

        CsvUtil.writeRow(out, null, schema, StandardCharsets.UTF_8, (rowGroup, fieldType, fieldIndex) -> {
            if (fieldIndex == 0) {
                return "你好, \"x\"";
            }
            return "line1\nline2";
        });

        String csv = out.toString(StandardCharsets.UTF_8);
        String expected = "a,b\n" +
                "\"你好, \"\"x\"\"\",\"line1\n" +
                "line2\"\n";
        assertThat(csv).isEqualTo(expected);
    }
}
