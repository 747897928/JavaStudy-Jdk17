package com.aquarius.wizard.springdatar2dbcdemo.service;

import com.aquarius.wizard.springdatar2dbcdemo.dto.DatabaseNodeResponse;
import com.aquarius.wizard.springdatar2dbcdemo.dto.TopologySummaryResponse;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

/**
 * 数据库拓扑探针服务。
 * <p>
 * 学习主从切换时，可以直接打这个接口，看 writer / reader 当前分别连到了哪个节点。
 */
@Service
public class DatabaseTopologyService {

    private static final String CONSISTENCY_HINT =
            "Reads may be eventually consistent after a fresh write when reader points to a replica.";

    private static final String TOPOLOGY_SQL = """
            SELECT
                COALESCE(inet_server_addr()::text, 'local') AS host,
                inet_server_port() AS port,
                current_database() AS database_name,
                pg_is_in_recovery() AS in_recovery,
                current_setting('transaction_read_only') AS transaction_read_only
            """;

    private final DatabaseClient writerDatabaseClient;
    private final DatabaseClient readerDatabaseClient;

    public DatabaseTopologyService(
            DatabaseClient writerDatabaseClient,
            @Qualifier("readerDatabaseClient") DatabaseClient readerDatabaseClient
    ) {
        this.writerDatabaseClient = writerDatabaseClient;
        this.readerDatabaseClient = readerDatabaseClient;
    }

    /**
     * 同时探测 writer / reader 当前连接到的 PostgreSQL 节点。
     */
    public Mono<TopologySummaryResponse> inspect() {
        return Mono.zip(
                        inspectNode("writer", writerDatabaseClient),
                        inspectNode("reader", readerDatabaseClient)
                )
                .map(tuple -> new TopologySummaryResponse(
                        tuple.getT1(),
                        tuple.getT2(),
                        CONSISTENCY_HINT
                ));
    }

    /**
     * 读取单个连接上的 PostgreSQL 角色状态。
     */
    private Mono<DatabaseNodeResponse> inspectNode(String role, DatabaseClient databaseClient) {
        return databaseClient.sql(TOPOLOGY_SQL)
                .map((row, metadata) -> new DatabaseNodeResponse(
                        role,
                        row.get("host", String.class),
                        row.get("port", Integer.class),
                        row.get("database_name", String.class),
                        Boolean.TRUE.equals(row.get("in_recovery", Boolean.class)),
                        row.get("transaction_read_only", String.class)
                ))
                .one();
    }
}
