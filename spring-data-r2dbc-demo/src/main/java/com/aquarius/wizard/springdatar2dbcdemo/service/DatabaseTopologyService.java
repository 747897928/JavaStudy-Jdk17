package com.aquarius.wizard.springdatar2dbcdemo.service;

import com.aquarius.wizard.springdatar2dbcdemo.dto.DatabaseNodeResponse;
import com.aquarius.wizard.springdatar2dbcdemo.dto.TopologySummaryResponse;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
public class DatabaseTopologyService {

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

    public Mono<TopologySummaryResponse> inspect() {
        return Mono.zip(
                        inspectNode("writer", writerDatabaseClient),
                        inspectNode("reader", readerDatabaseClient)
                )
                .map(tuple -> new TopologySummaryResponse(
                        tuple.getT1(),
                        tuple.getT2(),
                        "Reads may be eventually consistent after a fresh write when reader points to a replica."
                ));
    }

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
