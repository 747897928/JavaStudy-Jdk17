package com.aquarius.wizard.study.sparklauncher.support;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.client.api.YarnClient;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * 这里实现的是一个轻量级 YarnClient 连接池。
 * 目标不是做成通用组件，而是避免状态查询接口在高并发下频繁创建和销毁 YarnClient。
 */
public class YarnClientPool implements Closeable {

    private final Configuration configuration;
    private final int poolSize;
    private final Duration maxIdleTime;
    private final List<PooledYarnClient> clients = new ArrayList<>();
    private final ArrayBlockingQueue<PooledYarnClient> availableClients;

    public YarnClientPool(Configuration configuration, Integer poolSize, Duration maxIdleTime) {
        this.configuration = Objects.requireNonNull(configuration, "configuration must not be null");
        this.poolSize = poolSize == null || poolSize < 1 ? 4 : poolSize;
        this.maxIdleTime = maxIdleTime == null || maxIdleTime.isNegative() || maxIdleTime.isZero()
                ? Duration.ofMinutes(5)
                : maxIdleTime;
        this.availableClients = new ArrayBlockingQueue<>(this.poolSize);
    }

    /**
     * Spring 在应用启动时预热 YarnClient，避免第一批状态查询请求被冷启动拖慢。
     */
    public void start() {
        for (int index = 0; index < poolSize; index++) {
            PooledYarnClient pooledClient = new PooledYarnClient(createClient(), Instant.now());
            clients.add(pooledClient);
            availableClients.offer(pooledClient);
        }
    }

    /**
     * 查询前先从池里借一个客户端。
     * 如果这个客户端已经空闲太久，先回收再重建，避免长时间空闲后的连接老化问题。
     */
    public ApplicationReport getApplicationReport(String applicationId, long borrowTimeoutMillis) throws Exception {
        PooledYarnClient pooledClient = availableClients.poll(borrowTimeoutMillis, TimeUnit.MILLISECONDS);
        if (pooledClient == null) {
            throw new IllegalStateException("No available YarnClient in pool within timeout: " + borrowTimeoutMillis + "ms");
        }

        PooledYarnClient activeClient = refreshIfIdle(pooledClient);
        boolean healthy = false;
        try {
            ApplicationReport report = activeClient.client().getApplicationReport(ApplicationId.fromString(applicationId));
            healthy = true;
            return report;
        } finally {
            if (healthy) {
                availableClients.offer(activeClient.touch());
            } else {
                availableClients.offer(replaceClient(activeClient));
            }
        }
    }

    @Override
    public void close() throws IOException {
        for (PooledYarnClient pooledClient : clients) {
            closeClientQuietly(pooledClient.client());
        }
        clients.clear();
        availableClients.clear();
    }

    private PooledYarnClient refreshIfIdle(PooledYarnClient pooledClient) {
        Duration idleDuration = Duration.between(pooledClient.lastUsedAt(), Instant.now());
        if (idleDuration.compareTo(maxIdleTime) <= 0) {
            return pooledClient;
        }
        return replaceClient(pooledClient);
    }

    private synchronized PooledYarnClient replaceClient(PooledYarnClient pooledClient) {
        closeClientQuietly(pooledClient.client());
        PooledYarnClient newClient = new PooledYarnClient(createClient(), Instant.now());
        clients.removeIf(existing -> existing.client() == pooledClient.client());
        clients.add(newClient);
        return newClient;
    }

    private YarnClient createClient() {
        YarnClient client = YarnClient.createYarnClient();
        client.init(new Configuration(configuration));
        client.start();
        return client;
    }

    private void closeClientQuietly(YarnClient client) {
        try {
            client.stop();
        } finally {
            try {
                client.close();
            } catch (IOException ignored) {
                // 关闭阶段只做资源回收，不再影响主流程。
            }
        }
    }

    private record PooledYarnClient(YarnClient client, Instant lastUsedAt) {

        private PooledYarnClient touch() {
            return new PooledYarnClient(client, Instant.now());
        }
    }
}
