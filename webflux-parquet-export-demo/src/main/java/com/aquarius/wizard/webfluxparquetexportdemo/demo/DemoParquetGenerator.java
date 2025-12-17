package com.aquarius.wizard.webfluxparquetexportdemo.demo;

import com.aquarius.wizard.webfluxparquetexportdemo.util.Int96Util;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.io.IOException;
import java.time.Instant;
import java.util.Random;

/**
 * Demo Parquet file generator.
 * <p>
 * This is used in two places:
 * <ul>
 *   <li>{@code POST /demo/generate}: generate {@code ./data/demo.parquet}</li>
 *   <li>{@code GET /demo/download?source=...}: simulate remote parquet download by generating a "remote" parquet,
 *       then copying it via an {@code InputStream} to a local temp parquet</li>
 * </ul>
 */
public class DemoParquetGenerator {

    private static final String DEMO_SCHEMA = """
            message demo {
              optional int32   i32;
              optional int64   i64;
              optional int96   t96;
              optional float   f32;
              optional boolean b;
              optional double  d64;
              optional binary  bin;
            }
            """;

    public void generateParquetFile(java.nio.file.Path out, long rows, Random randomGenerator) throws IOException {
        MessageType schema = MessageTypeParser.parseMessageType(DEMO_SCHEMA);
        Configuration conf = new Configuration();
        GroupWriteSupport.setSchema(schema, conf);

        SimpleGroupFactory factory = new SimpleGroupFactory(schema);

        try (ParquetWriter<Group> writer =
                     new ParquetWriter<>(
                             new Path(out.toUri()),
                             new GroupWriteSupport(),
                             CompressionCodecName.SNAPPY,
                             ParquetWriter.DEFAULT_BLOCK_SIZE,
                             ParquetWriter.DEFAULT_PAGE_SIZE,
                             ParquetWriter.DEFAULT_PAGE_SIZE,
                             ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED,
                             ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED,
                             org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_1_0,
                             conf
                     )) {

            for (long rowIndex = 0; rowIndex < rows; rowIndex++) {
                Group rowGroup = factory.newGroup();

                // Make some columns null randomly (optional fields), to test: null -> empty
                if (randomGenerator.nextInt(10) != 0) rowGroup.add("i32", randomGenerator.nextInt());
                if (randomGenerator.nextInt(10) != 0) rowGroup.add("i64", randomGenerator.nextLong());
                if (randomGenerator.nextInt(10) != 0) {
                    Instant now = Instant.ofEpochMilli(System.currentTimeMillis() + randomGenerator.nextInt(1_000_000));
                    rowGroup.add("t96", Binary.fromConstantByteArray(Int96Util.instantToInt96(now)));
                }
                if (randomGenerator.nextInt(10) != 0) rowGroup.add("f32", randomGenerator.nextFloat());
                if (randomGenerator.nextInt(10) != 0) rowGroup.add("b", randomGenerator.nextBoolean());
                if (randomGenerator.nextInt(10) != 0) rowGroup.add("d64", randomGenerator.nextDouble());

                if (randomGenerator.nextInt(10) != 0) {
                    // ASCII bytes are easier to eyeball in CSV, but export logic supports arbitrary bytes.
                    byte[] randomAsciiBytes = new byte[16];
                    for (int byteIndex = 0; byteIndex < randomAsciiBytes.length; byteIndex++) {
                        int ch = 33 + randomGenerator.nextInt(94); // '!'..'~'
                        randomAsciiBytes[byteIndex] = (byte) ch;
                    }
                    rowGroup.add("bin", Binary.fromConstantByteArray(randomAsciiBytes));
                }

                writer.write(rowGroup);
            }
        }
    }
}

