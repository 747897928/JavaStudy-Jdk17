package com.aquarius.wizard.webfluxparquetexportdemo.parquet;

import com.aquarius.wizard.webfluxparquetexportdemo.util.Int96Util;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;

/**
 * Convert a single Parquet cell (physical + logical type) into a value suitable for CSV output.
 * <p>
 * This class is intentionally Parquet-aware. CSV escaping/quoting belongs in {@code CsvUtil};
 * Parquet type interpretation belongs here.
 */
public final class ParquetToCsvCellValueConverter {

    private ParquetToCsvCellValueConverter() {
    }

    /**
     * @return {@code null} for empty cell; {@code String/Number/Boolean/...} for text; {@code byte[]} for raw bytes.
     */
    public static Object toCsvCellValue(Group rowGroup, Type fieldType, int fieldIndex) {
        if (rowGroup.getFieldRepetitionCount(fieldIndex) == 0) {
            return null;
        }

        if (!fieldType.isPrimitive()) {
            return rowGroup.getGroup(fieldIndex, 0).toString();
        }

        PrimitiveType primitiveType = fieldType.asPrimitiveType();
        PrimitiveTypeName physicalType = primitiveType.getPrimitiveTypeName();
        LogicalTypeAnnotation logicalType = primitiveType.getLogicalTypeAnnotation();

        return switch (physicalType) {
            case INT32 -> formatInt32(rowGroup.getInteger(fieldIndex, 0), logicalType);
            case INT64 -> formatInt64(rowGroup.getLong(fieldIndex, 0), logicalType);
            case FLOAT -> Float.toString(rowGroup.getFloat(fieldIndex, 0));
            case DOUBLE -> Double.toString(rowGroup.getDouble(fieldIndex, 0));
            case BOOLEAN -> Boolean.toString(rowGroup.getBoolean(fieldIndex, 0));

            case INT96 -> {
                Binary int96Binary = rowGroup.getInt96(fieldIndex, 0);
                Instant instant = Int96Util.int96ToInstant(int96Binary.getBytes());
                yield instant.toString();
            }

            case BINARY, FIXED_LEN_BYTE_ARRAY -> {
                Binary binaryValue = rowGroup.getBinary(fieldIndex, 0);

                if (logicalType instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimal) {
                    BigInteger unscaled = new BigInteger(binaryValue.getBytes());
                    yield new BigDecimal(unscaled, decimal.getScale()).toPlainString();
                }

                if (logicalType instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation
                        || logicalType instanceof LogicalTypeAnnotation.EnumLogicalTypeAnnotation
                        || logicalType instanceof LogicalTypeAnnotation.JsonLogicalTypeAnnotation) {
                    yield binaryValue.toStringUsingUTF8();
                }

                yield binaryValue.getBytes();
            }

            default -> rowGroup.getValueToString(fieldIndex, 0);
        };
    }

    private static Object formatInt32(int value, LogicalTypeAnnotation logicalType) {
        if (logicalType instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation) {
            return LocalDate.ofEpochDay(value).toString();
        }
        if (logicalType instanceof LogicalTypeAnnotation.TimeLogicalTypeAnnotation time) {
            long nanos = switch (time.getUnit()) {
                case MILLIS -> (long) value * 1_000_000L;
                case MICROS -> (long) value * 1_000L;
                case NANOS -> (long) value;
            };
            return LocalTime.ofNanoOfDay(nanos).toString();
        }
        if (logicalType instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimal) {
            return BigDecimal.valueOf(value, decimal.getScale()).toPlainString();
        }
        return Integer.toString(value);
    }

    private static Object formatInt64(long value, LogicalTypeAnnotation logicalType) {
        if (logicalType instanceof LogicalTypeAnnotation.TimeLogicalTypeAnnotation time) {
            long nanos = switch (time.getUnit()) {
                case MILLIS -> value * 1_000_000L;
                case MICROS -> value * 1_000L;
                case NANOS -> value;
            };
            return LocalTime.ofNanoOfDay(nanos).toString();
        }
        if (logicalType instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation timestamp) {
            Instant instant = switch (timestamp.getUnit()) {
                case MILLIS -> Instant.ofEpochMilli(value);
                case MICROS -> Instant.ofEpochSecond(
                        Math.floorDiv(value, 1_000_000L),
                        Math.floorMod(value, 1_000_000L) * 1_000L
                );
                case NANOS -> Instant.ofEpochSecond(
                        Math.floorDiv(value, 1_000_000_000L),
                        Math.floorMod(value, 1_000_000_000L)
                );
            };
            return instant.toString();
        }
        if (logicalType instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimal) {
            return BigDecimal.valueOf(value, decimal.getScale()).toPlainString();
        }
        return Long.toString(value);
    }
}

