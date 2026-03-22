package com.aquarius.wizard.springdatar2dbcdemo.config;

import com.aquarius.wizard.springdatar2dbcdemo.repository.PurchaseOrderWriteRepository;
import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.data.r2dbc.repository.config.EnableR2dbcRepositories;
import org.springframework.r2dbc.connection.R2dbcTransactionManager;
import org.springframework.r2dbc.connection.init.CompositeDatabasePopulator;
import org.springframework.r2dbc.connection.init.ConnectionFactoryInitializer;
import org.springframework.r2dbc.connection.init.ResourceDatabasePopulator;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.transaction.ReactiveTransactionManager;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.springframework.util.StringUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties({DemoDatabaseProperties.class, PartnerClientProperties.class})
@EnableR2dbcRepositories(
        basePackageClasses = PurchaseOrderWriteRepository.class,
        entityOperationsRef = "writerEntityTemplate"
)
public class ReactiveDatabaseConfig {

    private static final Logger log = LoggerFactory.getLogger(ReactiveDatabaseConfig.class);

    private static final String ROLE_CHECK_SQL = """
            SELECT
                pg_is_in_recovery() AS in_recovery,
                current_setting('transaction_read_only') AS transaction_read_only
            """;

    @Bean
    @Primary
    public ConnectionFactory writerConnectionFactory(DemoDatabaseProperties properties) {
        return buildPooledConnectionFactory(
                properties.getWriterUrl(),
                properties.getUsername(),
                properties.getPassword(),
                "writer-pool",
                ConnectionPurpose.WRITER,
                properties.getPool()
        );
    }

    @Bean
    public ConnectionFactory readerConnectionFactory(DemoDatabaseProperties properties) {
        return buildPooledConnectionFactory(
                properties.getReaderUrl(),
                properties.getUsername(),
                properties.getPassword(),
                "reader-pool",
                ConnectionPurpose.READER,
                properties.getPool()
        );
    }

    @Bean
    @Primary
    public DatabaseClient writerDatabaseClient(ConnectionFactory writerConnectionFactory) {
        return DatabaseClient.create(writerConnectionFactory);
    }

    @Bean
    public DatabaseClient readerDatabaseClient(@Qualifier("readerConnectionFactory") ConnectionFactory readerConnectionFactory) {
        return DatabaseClient.create(readerConnectionFactory);
    }

    @Bean
    @Primary
    public R2dbcEntityTemplate writerEntityTemplate(ConnectionFactory writerConnectionFactory) {
        return new R2dbcEntityTemplate(writerConnectionFactory);
    }

    @Bean
    public R2dbcEntityTemplate readerEntityTemplate(@Qualifier("readerConnectionFactory") ConnectionFactory readerConnectionFactory) {
        return new R2dbcEntityTemplate(readerConnectionFactory);
    }

    @Bean
    @Primary
    public ReactiveTransactionManager writerTransactionManager(ConnectionFactory writerConnectionFactory) {
        return new R2dbcTransactionManager(writerConnectionFactory);
    }

    @Bean
    public TransactionalOperator writerTransactionalOperator(ReactiveTransactionManager writerTransactionManager) {
        return TransactionalOperator.create(writerTransactionManager);
    }

    @Bean
    public ConnectionFactoryInitializer writerConnectionFactoryInitializer(
            ConnectionFactory writerConnectionFactory,
            DemoDatabaseProperties properties
    ) {
        ConnectionFactoryInitializer initializer = new ConnectionFactoryInitializer();
        initializer.setConnectionFactory(writerConnectionFactory);
        initializer.setEnabled(properties.isInitializeSchema());
        initializer.setDatabasePopulator(new CompositeDatabasePopulator(
                new ResourceDatabasePopulator(new ClassPathResource("schema.sql")),
                new ResourceDatabasePopulator(new ClassPathResource("data.sql"))
        ));
        return initializer;
    }

    private ConnectionFactory buildPooledConnectionFactory(
            String url,
            String username,
            String password,
            String poolName,
            ConnectionPurpose connectionPurpose,
            DemoDatabaseProperties.Pool poolProperties
    ) {
        ConnectionFactoryOptions.Builder optionsBuilder = ConnectionFactoryOptions.parse(url)
                .mutate()
                .option(ConnectionFactoryOptions.USER, username)
                .option(ConnectionFactoryOptions.PASSWORD, password);

        optionsBuilder.option(Option.valueOf("applicationName"), "spring-data-r2dbc-demo-" + poolName);

        ConnectionFactory delegate = ConnectionFactories.get(optionsBuilder.build());
        ConnectionPoolConfiguration.Builder poolBuilder = ConnectionPoolConfiguration.builder(delegate)
                .name(poolName)
                .acquireRetry(poolProperties.getAcquireRetry())
                .backgroundEvictionInterval(poolProperties.getBackgroundEvictionInterval())
                .initialSize(poolProperties.getInitialSize())
                .maxSize(poolProperties.getMaxSize())
                .maxAcquireTime(poolProperties.getMaxAcquireTime())
                .maxCreateConnectionTime(poolProperties.getMaxCreateConnectionTime())
                .maxIdleTime(poolProperties.getMaxIdleTime())
                .maxLifeTime(poolProperties.getMaxLifeTime())
                .maxValidationTime(poolProperties.getMaxValidationTime());

        if (StringUtils.hasText(poolProperties.getValidationQuery())) {
            poolBuilder.validationQuery(poolProperties.getValidationQuery());
        }

        if (connectionPurpose == ConnectionPurpose.WRITER && poolProperties.isValidateWriterRoleOnAcquire()) {
            poolBuilder.postAllocate(connection -> ensureWriterConnection(connection, poolName));
        }

        return new ConnectionPool(poolBuilder.build());
    }

    private Mono<Void> ensureWriterConnection(Connection connection, String poolName) {
        return Flux.from(connection.createStatement(ROLE_CHECK_SQL).execute())
                .flatMap(result -> result.map((row, metadata) -> new ServerRoleState(
                        Boolean.TRUE.equals(row.get("in_recovery", Boolean.class)),
                        row.get("transaction_read_only", String.class)
                )))
                .single()
                .flatMap(roleState -> {
                    if (!roleState.isWritablePrimary()) {
                        log.warn("Discarding stale writer connection from pool '{}' because node is no longer primary. inRecovery={}, transactionReadOnly={}",
                                poolName, roleState.inRecovery(), roleState.transactionReadOnly());
                        return Mono.error(new IllegalStateException("Writer connection no longer points to PRIMARY"));
                    }
                    return Mono.empty();
                });
    }

    private enum ConnectionPurpose {
        WRITER,
        READER
    }

    private record ServerRoleState(boolean inRecovery, String transactionReadOnly) {

        private boolean isWritablePrimary() {
            return !inRecovery && !"on".equalsIgnoreCase(transactionReadOnly);
        }
    }
}
