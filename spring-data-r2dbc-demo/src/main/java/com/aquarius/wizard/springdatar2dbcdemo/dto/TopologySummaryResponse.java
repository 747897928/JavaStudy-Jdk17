package com.aquarius.wizard.springdatar2dbcdemo.dto;

/**
 * writer / reader 拓扑信息汇总。
 */
public record TopologySummaryResponse(
        DatabaseNodeResponse writer,
        DatabaseNodeResponse reader,
        String consistencyHint
) {
}
