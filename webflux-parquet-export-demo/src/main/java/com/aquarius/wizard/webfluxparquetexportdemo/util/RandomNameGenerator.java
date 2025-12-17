package com.aquarius.wizard.webfluxparquetexportdemo.util;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Generates safe random names for files/entries.
 * <p>
 * This demo uses only letters and digits to avoid escaping issues in HTTP headers and ZIP entries.
 */
public final class RandomNameGenerator {

    private static final char[] ALPHANUMERIC = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789".toCharArray();

    private RandomNameGenerator() {
    }

    public static String randomAlphaNumeric(int length) {
        int n = Math.max(1, length);
        ThreadLocalRandom random = ThreadLocalRandom.current();
        StringBuilder sb = new StringBuilder(n);
        for (int i = 0; i < n; i++) {
            sb.append(ALPHANUMERIC[random.nextInt(ALPHANUMERIC.length)]);
        }
        return sb.toString();
    }
}

