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

/**
 * R2DBC 核心配置。
 * <p>
 * 这个类是整个 Demo 最重要的入口之一，学习时建议重点看这里：
 * <p>
 * 1. 为什么要拆成 writer / reader 两个 {@link ConnectionFactory}
 * 2. 为什么只配 failover URL 还不够，还要配连接池角色校验
 * 3. 主从切换后，应用不重启时，新的连接是如何重新选择节点的
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties({DemoDatabaseProperties.class, PartnerClientProperties.class})
@EnableR2dbcRepositories(
        basePackageClasses = PurchaseOrderWriteRepository.class,
        entityOperationsRef = "writerEntityTemplate"
)
public class ReactiveDatabaseConfig {

    private static final Logger log = LoggerFactory.getLogger(ReactiveDatabaseConfig.class);
    private static final String APPLICATION_NAME_PREFIX = "spring-data-r2dbc-demo-";
    private static final String READ_ONLY_ON = "on";
    private static final String INVALID_WRITER_CONNECTION_MESSAGE = "Writer connection no longer points to PRIMARY";

    /**
     * 用于识别当前连接是否仍然连在可写主库上的 SQL。
     * <p>
     * PostgreSQL 中：
     * <p>
     * - {@code pg_is_in_recovery() = false} 通常表示当前是主库
     * - {@code transaction_read_only = off} 表示当前事务不是只读
     */
    private static final String ROLE_CHECK_SQL = """
            SELECT
                pg_is_in_recovery() AS in_recovery,
                current_setting('transaction_read_only') AS transaction_read_only
            """;

    /**
     * 写连接池。
     * <p>
     * 在 HA profile 下，这个连接串会带 {@code targetServerType=PRIMARY}，
     * 表示驱动在新建物理连接时应优先连接主库。
     */
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

    /**
     * 读连接池。
     * <p>
     * 在 HA profile 下，这个连接串会带 {@code targetServerType=PREFER_SECONDARY}，
     * 表示驱动在新建物理连接时优先连接从库。
     */
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

    /**
     * 显式暴露读库 {@link DatabaseClient}，方便查询服务和拓扑探针按职责使用。
     */
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
        // 只在 writer 上初始化表结构和种子数据，避免 reader 节点执行 DDL/DML。
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
        // 这里不复用驱动内部的 TargetServerType 枚举：
        // TargetServerType 解决的是“新建物理连接时驱动如何选 host”，
        // 而这里的枚举表达的是“当前这个连接池承担的是写职责还是读职责”。
        ConnectionFactoryOptions.Builder optionsBuilder = ConnectionFactoryOptions.parse(url)
                .mutate()
                .option(ConnectionFactoryOptions.USER, username)
                .option(ConnectionFactoryOptions.PASSWORD, password);

        optionsBuilder.option(Option.valueOf("applicationName"), APPLICATION_NAME_PREFIX + poolName);

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
            // 关键点：
            // 1. 驱动只会在“新建物理连接”时重新识别 primary / secondary。
            // 2. 如果连接池里缓存的是切换前的旧 writer 连接，它不会自动重新选主。
            // 3. 所以 writer 连接每次借出时，都要再校验一次当前节点角色。
            poolBuilder.postAllocate(connection -> ensureWriterConnection(connection, poolName));
        }

        return new ConnectionPool(poolBuilder.build());
    }

    /**
     * 应用层连接池职责枚举。
     * <p>
     * 注意：这不是驱动层的 host 选择枚举，而是为了让配置代码更容易读懂，
     * 明确区分“这个池是给写流量用的”还是“这个池是给读流量用的”。
     */
    private enum ConnectionPurpose {
        WRITER,
        READER
    }

    /**
     * Writer 连接借出时的二次校验。
     * <p>
     * 如果主从切换已经发生，而连接池里还残留着旧主节点的连接，
     * 那么这个连接可能已经变成只读或 standby。
     * 一旦检测到这种情况，就立刻让连接获取失败，连接池会丢弃该连接并重建。
     */
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
                        return Mono.error(new IllegalStateException(INVALID_WRITER_CONNECTION_MESSAGE));
                    }
                    return Mono.empty();
                });
    }

    /**
     * PostgreSQL 角色快照。
     * <p>
     * 这里只保留判断 writer 是否仍然可写所需的最小字段。
     */
    private record ServerRoleState(boolean inRecovery, String transactionReadOnly) {

        private boolean isWritablePrimary() {
            return !inRecovery && !READ_ONLY_ON.equalsIgnoreCase(transactionReadOnly);
        }
    }
}
