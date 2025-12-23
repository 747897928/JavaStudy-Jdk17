package com.aquarius.wizard.webfluxparquetexportdemo.parquet;

import com.aquarius.wizard.webfluxparquetexportdemo.util.Int96Util;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Base64;
import java.util.UUID;

/**
 * Convert a single Parquet cell (physical + logical type) into a value suitable for CSV output.
 * <p>
 * This class is intentionally Parquet-aware. CSV escaping/quoting belongs in {@code CsvUtil};
 * Parquet type interpretation belongs here.
 * <p>
 * Note on INT96:
 * INT96 timestamps are legacy/deprecated in the Parquet spec, but they are still common in the Spark/Hive ecosystem
 * (e.g. Spark 2.4 and older defaults). This demo keeps INT96 -> Instant formatting for compatibility.
 */
public final class ParquetToCsvCellValueConverter {

    private static final Base64.Encoder BASE64 = Base64.getEncoder();

    /**
     * Parquet's legacy ConvertedType/OriginalType is deprecated in parquet-mr but still appears in real-world
     * Parquet files (Spark/Hive/older writers). We map it to our own small enum so the deprecated type is confined
     * to one conversion method.
     */
    private enum LegacyConvertedType {
        UTF8,
        ENUM,
        JSON,
        DATE,
        TIME_MILLIS,
        TIME_MICROS,
        TIMESTAMP_MILLIS,
        TIMESTAMP_MICROS,
        INT_8,
        INT_16,
        UINT_8,
        UINT_16
    }

    private ParquetToCsvCellValueConverter() {
    }

    /**
     * @return {@code null} for empty cell; {@code String/Number/Boolean/...} for text.
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
        LegacyConvertedType legacyConvertedType = legacyConvertedTypeOrNull(primitiveType);

        return switch (physicalType) {
            case INT32 -> formatInt32(rowGroup.getInteger(fieldIndex, 0), logicalType, legacyConvertedType);
            case INT64 -> formatInt64(rowGroup.getLong(fieldIndex, 0), logicalType, legacyConvertedType);
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
                int fixedLength = (physicalType == PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY) ? primitiveType.getTypeLength() : -1;

                Object formatted = formatBinary(binaryValue, logicalType, legacyConvertedType, fixedLength);
                yield (formatted == null) ? BASE64.encodeToString(binaryValue.getBytes()) : formatted;
            }

            default -> rowGroup.getValueToString(fieldIndex, 0);
        };
    }

    private static Object formatInt32(int value, LogicalTypeAnnotation logicalType, LegacyConvertedType legacyConvertedType) {
        Object formatted = formatInt32ByLogicalType(value, logicalType);
        if (formatted != null) {
            return formatted;
        }
        if (logicalType == null) {
            Object legacyFormatted = formatInt32ByLegacyConvertedType(value, legacyConvertedType);
            if (legacyFormatted != null) {
                return legacyFormatted;
            }
        }
        return Integer.toString(value);
    }

    private static Object formatInt32ByLogicalType(int value, LogicalTypeAnnotation logicalType) {
        if (logicalType instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation) {
            return LocalDate.ofEpochDay(value).toString();
        }
        if (logicalType instanceof LogicalTypeAnnotation.TimeLogicalTypeAnnotation time) {
            return LocalTime.ofNanoOfDay(nanosFromTimeInt32(value, time.getUnit())).toString();
        }
        if (logicalType instanceof LogicalTypeAnnotation.IntLogicalTypeAnnotation intAnnotation) {
            return formatNarrowInt32(value, intAnnotation.getBitWidth(), intAnnotation.isSigned());
        }
        if (logicalType instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimal) {
            return BigDecimal.valueOf(value, decimal.getScale()).toPlainString();
        }
        return null;
    }

    private static Object formatInt32ByLegacyConvertedType(int value, LegacyConvertedType legacyConvertedType) {
        if (legacyConvertedType == null) {
            return null;
        }
        return switch (legacyConvertedType) {
            case DATE -> LocalDate.ofEpochDay(value).toString();
            case TIME_MILLIS -> LocalTime.ofNanoOfDay((long) value * 1_000_000L).toString();
            case INT_8 -> Byte.toString((byte) value);
            case UINT_8 -> Integer.toString(value & 0xFF);
            case INT_16 -> Short.toString((short) value);
            case UINT_16 -> Integer.toString(value & 0xFFFF);
            default -> null;
        };
    }

    private static long nanosFromTimeInt32(int value, TimeUnit unit) {
        return switch (unit) {
            case MILLIS -> (long) value * 1_000_000L;
            case MICROS -> (long) value * 1_000L;
            case NANOS -> (long) value;
        };
    }

    private static Object formatInt64(long value, LogicalTypeAnnotation logicalType, LegacyConvertedType legacyConvertedType) {
        if (logicalType instanceof LogicalTypeAnnotation.TimeLogicalTypeAnnotation time) {
            long nanos = switch (time.getUnit()) {
                case MILLIS -> value * 1_000_000L;
                case MICROS -> value * 1_000L;
                case NANOS -> value;
            };
            return LocalTime.ofNanoOfDay(nanos).toString();
        }
        if (logicalType instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation timestamp) {
            // Parquet TIMESTAMP has two semantics:
            // - isAdjustedToUTC=true  -> represents a point on the global time-line (Instant).
            // - isAdjustedToUTC=false -> "local" timestamp without time zone; it is NOT a unique Instant.
            //
            // For CSV output, we make the distinction explicit:
            // - adjusted -> Instant ISO-8601 string
            // - local    -> LocalDateTime ISO-8601 string
            return timestamp.isAdjustedToUTC()
                    ? formatTimestampInstant(value, timestamp.getUnit()).toString()
                    : formatTimestampLocalDateTime(value, timestamp.getUnit()).toString();
        }
        if (logicalType instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimal) {
            return BigDecimal.valueOf(value, decimal.getScale()).toPlainString();
        }

        if (logicalType == null) {
            if (legacyConvertedType == LegacyConvertedType.TIME_MICROS) {
                return LocalTime.ofNanoOfDay(value * 1_000L).toString();
            }
            if (legacyConvertedType == LegacyConvertedType.TIMESTAMP_MILLIS) {
                return formatTimestampInstant(value, TimeUnit.MILLIS).toString();
            }
            if (legacyConvertedType == LegacyConvertedType.TIMESTAMP_MICROS) {
                return formatTimestampInstant(value, TimeUnit.MICROS).toString();
            }
        }

        return Long.toString(value);
    }

    private static Object formatBinary(Binary binaryValue,
                                       LogicalTypeAnnotation logicalType,
                                       LegacyConvertedType legacyConvertedType,
                                       int fixedLength) {
        Object formatted = formatBinaryByLogicalType(binaryValue, logicalType, fixedLength);
        if (formatted != null) {
            return formatted;
        }
        if (logicalType == null) {
            return formatBinaryByLegacyConvertedType(binaryValue, legacyConvertedType);
        }
        return null;
    }

    private static Object formatBinaryByLogicalType(Binary binaryValue, LogicalTypeAnnotation logicalType, int fixedLength) {
        if (logicalType instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimal) {
            BigInteger unscaled = new BigInteger(binaryValue.getBytes());
            return new BigDecimal(unscaled, decimal.getScale()).toPlainString();
        }
        if (logicalType instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation) {
            return binaryValue.toStringUsingUTF8();
        }
        if (logicalType instanceof LogicalTypeAnnotation.EnumLogicalTypeAnnotation) {
            return binaryValue.toStringUsingUTF8();
        }
        if (logicalType instanceof LogicalTypeAnnotation.JsonLogicalTypeAnnotation) {
            return binaryValue.toStringUsingUTF8();
        }
        if (logicalType instanceof LogicalTypeAnnotation.UUIDLogicalTypeAnnotation && fixedLength == 16) {
            return formatUuid(binaryValue.getBytes());
        }
        return null;
    }

    private static Object formatBinaryByLegacyConvertedType(Binary binaryValue, LegacyConvertedType legacyConvertedType) {
        if (legacyConvertedType == null) {
            return null;
        }
        return switch (legacyConvertedType) {
            case UTF8, ENUM, JSON -> binaryValue.toStringUsingUTF8();
            default -> null;
        };
    }

    private static String formatUuid(byte[] bytes) {
        if (bytes.length != 16) {
            return BASE64.encodeToString(bytes);
        }
        long mostSignificantBits = 0L;
        long leastSignificantBits = 0L;
        for (int i = 0; i < 8; i++) {
            mostSignificantBits = (mostSignificantBits << 8) | (bytes[i] & 0xFFL);
        }
        for (int i = 8; i < 16; i++) {
            leastSignificantBits = (leastSignificantBits << 8) | (bytes[i] & 0xFFL);
        }
        return new UUID(mostSignificantBits, leastSignificantBits).toString();
    }

    private static Object formatNarrowInt32(int value, int bitWidth, boolean signed) {
        if (bitWidth == 8) {
            return signed ? Byte.toString((byte) value) : Integer.toString(value & 0xFF);
        }
        if (bitWidth == 16) {
            return signed ? Short.toString((short) value) : Integer.toString(value & 0xFFFF);
        }
        return Integer.toString(value);
    }

    private static Instant formatTimestampInstant(long value, TimeUnit unit) {
        return switch (unit) {
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
    }

    private static LocalDateTime formatTimestampLocalDateTime(long value, TimeUnit unit) {
        Instant asIfUtcInstant = formatTimestampInstant(value, unit);
        return LocalDateTime.ofInstant(asIfUtcInstant, ZoneOffset.UTC);
    }

    @SuppressWarnings("deprecation")
    private static LegacyConvertedType legacyConvertedTypeOrNull(PrimitiveType primitiveType) {
        org.apache.parquet.schema.OriginalType originalType = primitiveType.getOriginalType();
        if (originalType == null) {
            return null;
        }
        return switch (originalType) {
            case UTF8 -> LegacyConvertedType.UTF8;
            case ENUM -> LegacyConvertedType.ENUM;
            case JSON -> LegacyConvertedType.JSON;
            case DATE -> LegacyConvertedType.DATE;
            case TIME_MILLIS -> LegacyConvertedType.TIME_MILLIS;
            case TIME_MICROS -> LegacyConvertedType.TIME_MICROS;
            case TIMESTAMP_MILLIS -> LegacyConvertedType.TIMESTAMP_MILLIS;
            case TIMESTAMP_MICROS -> LegacyConvertedType.TIMESTAMP_MICROS;
            case INT_8 -> LegacyConvertedType.INT_8;
            case INT_16 -> LegacyConvertedType.INT_16;
            case UINT_8 -> LegacyConvertedType.UINT_8;
            case UINT_16 -> LegacyConvertedType.UINT_16;
            default -> null;
        };
    }
}
