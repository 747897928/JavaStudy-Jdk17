package com.aquarius.wizard.webfluxparquetexportdemo.model;

import java.util.Locale;

public enum FileFormat {
    PARQUET,
    CSV,
    ZIP;

    public static FileFormat from(String raw) {
        if (raw == null || raw.isBlank()) {
            return ZIP;
        }
        return FileFormat.valueOf(raw.trim().toUpperCase(Locale.ROOT));
    }
}

