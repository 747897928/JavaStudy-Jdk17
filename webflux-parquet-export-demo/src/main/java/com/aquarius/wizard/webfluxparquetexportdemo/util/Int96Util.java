package com.aquarius.wizard.webfluxparquetexportdemo.util;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;

/**
 * Parquet INT96 helper (commonly used by older Parquet writers for timestamps):
 * little-endian [nanosOfDay(8 bytes), julianDay(4 bytes)].
 */
public final class Int96Util {

    private Int96Util() {
    }

    private static final int JULIAN_DAY_OF_EPOCH = 2440588; // 1970-01-01
    private static final long NANOS_PER_SECOND = 1_000_000_000L;

    public static Instant int96ToInstant(byte[] int96) {
        if (int96 == null || int96.length != 12) {
            throw new IllegalArgumentException("INT96 must be 12 bytes");
        }
        ByteBuffer bb = ByteBuffer.wrap(int96).order(ByteOrder.LITTLE_ENDIAN);
        long nanosOfDay = bb.getLong();
        int julianDay = bb.getInt();

        long epochDay = (long) julianDay - JULIAN_DAY_OF_EPOCH;
        long epochSecond = epochDay * 86_400L + (nanosOfDay / NANOS_PER_SECOND);
        long nanoAdj = nanosOfDay % NANOS_PER_SECOND;
        return Instant.ofEpochSecond(epochSecond, nanoAdj);
    }

    public static byte[] instantToInt96(Instant instant) {
        long epochSecond = instant.getEpochSecond();
        int nano = instant.getNano();

        long epochDay = Math.floorDiv(epochSecond, 86_400L);
        long secondsOfDay = Math.floorMod(epochSecond, 86_400L);

        int julianDay = (int) (epochDay + JULIAN_DAY_OF_EPOCH);
        long nanosOfDay = secondsOfDay * NANOS_PER_SECOND + nano;

        ByteBuffer bb = ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN);
        bb.putLong(nanosOfDay);
        bb.putInt(julianDay);
        return bb.array();
    }
}

