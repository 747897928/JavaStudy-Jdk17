package com.aquarius.wizard.springdatar2dbcdemo.dto;

public record TopologySummaryResponse(
        DatabaseNodeResponse writer,
        DatabaseNodeResponse reader,
        String consistencyHint
) {
}
