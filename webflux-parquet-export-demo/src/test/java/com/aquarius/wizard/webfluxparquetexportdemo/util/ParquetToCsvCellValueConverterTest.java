package com.aquarius.wizard.webfluxparquetexportdemo.util;

import com.aquarius.wizard.webfluxparquetexportdemo.parquet.ParquetToCsvCellValueConverter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Base64;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class ParquetToCsvCellValueConverterTest {

    @TempDir
    java.nio.file.Path tempDir;

    @Test
    void binaryNonStringIsBase64() {
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
    void binaryUtf8IsPlainString() {
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
    void binaryEnumIsPlainString() {
        MessageType schema = MessageTypeParser.parseMessageType("""
                message demo {
                  optional binary e (ENUM);
                }
                """);
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);
        Group row = factory.newGroup();
        row.add("e", Binary.fromString("X"));

        Object cell = ParquetToCsvCellValueConverter.toCsvCellValue(row, schema.getType("e"), 0);
        assertThat(cell).isEqualTo("X");
    }

    @Test
    void binaryDecimalIsPlainNumberString() {
        MessageType schema = Types.buildMessage()
                .optional(PrimitiveTypeName.BINARY)
                .as(LogicalTypeAnnotation.decimalType(2, 9))
                .named("dec")
                .named("demo");
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);
        Group row = factory.newGroup();
        row.add("dec", Binary.fromConstantByteArray(BigInteger.valueOf(12345).toByteArray()));

        Object cell = ParquetToCsvCellValueConverter.toCsvCellValue(row, schema.getType("dec"), schema.getFieldIndex("dec"));
        assertThat(cell).isEqualTo("123.45");
    }

    @Test
    void int32DateIsIsoDateString() {
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

    @Test
    void int32UnsignedIntLogicalTypeIsUnsignedDecimal() {
        MessageType schema = Types.buildMessage()
                .optional(PrimitiveTypeName.INT32)
                .as(LogicalTypeAnnotation.intType(16, false))
                .named("u")
                .named("demo");
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);
        Group row = factory.newGroup();
        row.add("u", -1);

        Object cell = ParquetToCsvCellValueConverter.toCsvCellValue(row, schema.getType("u"), schema.getFieldIndex("u"));
        assertThat(cell).isEqualTo("65535");
    }

    @Test
    void int32Uint16OriginalTypeIsUnsignedDecimal() {
        MessageType schema = MessageTypeParser.parseMessageType("""
                message demo {
                  optional int32 u (UINT_16);
                }
                """);
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);
        Group row = factory.newGroup();
        row.add("u", -1);

        Object cell = ParquetToCsvCellValueConverter.toCsvCellValue(row, schema.getType("u"), 0);
        assertThat(cell).isEqualTo("65535");
    }

    @Test
    void int32TimeMillisOriginalTypeIsLocalTime() {
        MessageType schema = MessageTypeParser.parseMessageType("""
                message demo {
                  optional int32 t (TIME_MILLIS);
                }
                """);
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);
        Group row = factory.newGroup();
        row.add("t", 1); // 00:00:00.001

        Object cell = ParquetToCsvCellValueConverter.toCsvCellValue(row, schema.getType("t"), 0);
        assertThat(cell).isEqualTo("00:00:00.001");
    }

    @Test
    void int64TimeMicrosOriginalTypeIsLocalTime() {
        MessageType schema = MessageTypeParser.parseMessageType("""
                message demo {
                  optional int64 t (TIME_MICROS);
                }
                """);
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);
        Group row = factory.newGroup();
        row.add("t", 1L); // 00:00:00.000001

        Object cell = ParquetToCsvCellValueConverter.toCsvCellValue(row, schema.getType("t"), 0);
        assertThat(cell).isEqualTo("00:00:00.000001");
    }

    @Test
    void int64TimestampMillisOriginalTypeIsInstantString() {
        MessageType schema = MessageTypeParser.parseMessageType("""
                message demo {
                  optional int64 ts (TIMESTAMP_MILLIS);
                }
                """);
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);
        Group row = factory.newGroup();
        row.add("ts", 1000L); // 1970-01-01T00:00:01Z

        Object cell = ParquetToCsvCellValueConverter.toCsvCellValue(row, schema.getType("ts"), 0);
        assertThat(cell).isEqualTo("1970-01-01T00:00:01Z");
    }

    @Test
    void timestampLogicalTypesAndUuidAndNarrowIntsRoundTrip() throws Exception {
        MessageType schema = Types.buildMessage()
                .optional(PrimitiveTypeName.INT64)
                .as(LogicalTypeAnnotation.timestampType(true, TimeUnit.MICROS))
                .named("tsUtcMicros")
                .optional(PrimitiveTypeName.INT64)
                .as(LogicalTypeAnnotation.timestampType(false, TimeUnit.MICROS))
                .named("tsLocalMicros")
                .optional(PrimitiveTypeName.INT64)
                .as(org.apache.parquet.schema.OriginalType.TIMESTAMP_MICROS)
                .named("tsLegacyMicros")
                .optional(PrimitiveTypeName.INT32)
                .as(LogicalTypeAnnotation.intType(8, true))
                .named("tinyInt")
                .optional(PrimitiveTypeName.INT32)
                .as(org.apache.parquet.schema.OriginalType.UINT_8)
                .named("uTinyInt")
                .optional(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
                .length(16)
                .as(LogicalTypeAnnotation.uuidType())
                .named("uuid")
                .optional(PrimitiveTypeName.INT96)
                .named("tsInt96")
                .optional(PrimitiveTypeName.BINARY)
                .named("rawBytes")
                .named("demo");

        java.nio.file.Path parquetFile = tempDir.resolve("demo.parquet");
        Configuration conf = new Configuration();
        GroupWriteSupport.setSchema(schema, conf);

        Instant instant1 = Instant.ofEpochSecond(1);
        long micros1 = instant1.getEpochSecond() * 1_000_000L;
        Instant instant2 = Instant.ofEpochSecond(2);
        long micros2 = instant2.getEpochSecond() * 1_000_000L;
        UUID uuid = UUID.fromString("123e4567-e89b-12d3-a456-426614174000");
        byte[] uuidBytes = uuidToParquetBytes(uuid);
        byte[] raw = new byte[]{(byte) 0xFF, 0x00, 0x41};

        SimpleGroupFactory factory = new SimpleGroupFactory(schema);
        Group row = factory.newGroup()
                .append("tsUtcMicros", micros1)
                .append("tsLocalMicros", micros1)
                .append("tsLegacyMicros", micros2)
                .append("tinyInt", -1)
                .append("uTinyInt", -1)
                .append("uuid", Binary.fromConstantByteArray(uuidBytes))
                .append("tsInt96", Binary.fromConstantByteArray(Int96Util.instantToInt96(Instant.ofEpochSecond(3))))
                .append("rawBytes", Binary.fromConstantByteArray(raw));

        try (org.apache.parquet.hadoop.ParquetWriter<Group> writer = ExampleParquetWriter.builder(new Path(parquetFile.toUri()))
                .withType(schema)
                .withConf(conf)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build()) {
            writer.write(row);
        }

        GroupReadSupport readSupport = new GroupReadSupport();
        try (ParquetReader<Group> reader = ParquetReader.builder(readSupport, new Path(parquetFile.toUri()))
                .withConf(conf)
                .build()) {
            Group read = reader.read();
            assertThat(read).isNotNull();

            Object tsUtcCell = ParquetToCsvCellValueConverter.toCsvCellValue(read, schema.getType("tsUtcMicros"), schema.getFieldIndex("tsUtcMicros"));
            assertThat(tsUtcCell).isEqualTo(instant1.toString());

            Object tsLocalCell = ParquetToCsvCellValueConverter.toCsvCellValue(read, schema.getType("tsLocalMicros"), schema.getFieldIndex("tsLocalMicros"));
            LocalDateTime expectedLocal = LocalDateTime.ofInstant(instant1, ZoneOffset.UTC);
            assertThat(tsLocalCell).isEqualTo(expectedLocal.toString());

            Object tsLegacyCell = ParquetToCsvCellValueConverter.toCsvCellValue(read, schema.getType("tsLegacyMicros"), schema.getFieldIndex("tsLegacyMicros"));
            assertThat(tsLegacyCell).isEqualTo(instant2.toString());

            Object tinyIntCell = ParquetToCsvCellValueConverter.toCsvCellValue(read, schema.getType("tinyInt"), schema.getFieldIndex("tinyInt"));
            assertThat(tinyIntCell).isEqualTo("-1");

            Object uTinyIntCell = ParquetToCsvCellValueConverter.toCsvCellValue(read, schema.getType("uTinyInt"), schema.getFieldIndex("uTinyInt"));
            assertThat(uTinyIntCell).isEqualTo("255");

            Object uuidCell = ParquetToCsvCellValueConverter.toCsvCellValue(read, schema.getType("uuid"), schema.getFieldIndex("uuid"));
            assertThat(uuidCell).isEqualTo(uuid.toString());

            Object rawCell = ParquetToCsvCellValueConverter.toCsvCellValue(read, schema.getType("rawBytes"), schema.getFieldIndex("rawBytes"));
            assertThat(rawCell).isEqualTo(Base64.getEncoder().encodeToString(raw));
        }

        assertThat(Files.exists(parquetFile)).isTrue();
    }

    private static byte[] uuidToParquetBytes(UUID uuid) {
        ByteBuffer buffer = ByteBuffer.allocate(16);
        buffer.putLong(uuid.getMostSignificantBits());
        buffer.putLong(uuid.getLeastSignificantBits());
        return buffer.array();
    }
}
