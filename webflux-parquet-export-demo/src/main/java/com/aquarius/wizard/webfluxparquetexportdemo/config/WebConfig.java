package com.aquarius.wizard.webfluxparquetexportdemo.config;

import com.aquarius.wizard.webfluxparquetexportdemo.model.FileFormat;
import org.springframework.core.convert.converter.Converter;
import org.springframework.format.FormatterRegistry;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.config.WebFluxConfigurer;

/**
 * Allow query params like {@code format=zip|csv|parquet} (case-insensitive).
 */
@Component
public class WebConfig implements WebFluxConfigurer {

    @Override
    public void addFormatters(FormatterRegistry registry) {
        registry.addConverter(new Converter<String, FileFormat>() {
            @Override
            public FileFormat convert(String source) {
                return FileFormat.from(source);
            }
        });
    }
}

