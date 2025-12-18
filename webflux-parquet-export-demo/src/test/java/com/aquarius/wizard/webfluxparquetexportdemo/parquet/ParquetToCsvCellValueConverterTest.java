package com.aquarius.wizard.webfluxparquetexportdemo.parquet;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.Test;

import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

class ParquetToCsvCellValueConverterTest {

    @Test
    void binaryNonString_isBase64() {
        MessageType schema = MessageTypeParser.parseMessageType("""
                message demo {
                  optional binary bin;
                }
                """);
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);
        Group row = factory.newGroup();
        byte[] bytes = new byte[]{(byte) 0xFF, 0x00, 0x41};
        row.add("bin", Binary.fromConstantByteArray(bytes));

        Object cell = ParquetToCsvCellValueConverter.toCsvCellValue(row, schema.getType("bin"), 0);
        assertThat(cell).isEqualTo(Base64.getEncoder().encodeToString(bytes));
    }

    @Test
    void binaryUtf8_isPlainString() {
        MessageType schema = MessageTypeParser.parseMessageType("""
                message demo {
                  optional binary s (UTF8);
                }
                """);
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);
        Group row = factory.newGroup();
        row.add("s", Binary.fromString("hello"));

        Object cell = ParquetToCsvCellValueConverter.toCsvCellValue(row, schema.getType("s"), 0);
        assertThat(cell).isEqualTo("hello");
    }

    @Test
    void int32Date_isIsoDateString() {
        MessageType schema = MessageTypeParser.parseMessageType("""
                message demo {
                  optional int32 d (DATE);
                }
                """);
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);
        Group row = factory.newGroup();
        row.add("d", 0); // 1970-01-01

        Object cell = ParquetToCsvCellValueConverter.toCsvCellValue(row, schema.getType("d"), 0);
        assertThat(cell).isEqualTo("1970-01-01");
    }
}

