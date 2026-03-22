package com.aquarius.wizard.study.sparklauncher.support;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.client.api.YarnClient;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

public class YarnClientPool implements Closeable {

    private final Configuration configuration;
    private final int poolSize;
    private final List<YarnClient> clients = new ArrayList<>();
    private final ArrayBlockingQueue<YarnClient> availableClients;

    public YarnClientPool(Configuration configuration, Integer poolSize) {
        this.configuration = configuration;
        this.poolSize = poolSize == null || poolSize < 1 ? 4 : poolSize;
        this.availableClients = new ArrayBlockingQueue<>(this.poolSize);
    }

    public void start() {
        for (int index = 0; index < poolSize; index++) {
            YarnClient client = YarnClient.createYarnClient();
            client.init(new Configuration(configuration));
            client.start();
            clients.add(client);
            availableClients.offer(client);
        }
    }

    public ApplicationReport getApplicationReport(String applicationId, long timeoutMillis) throws Exception {
        YarnClient client = availableClients.poll(timeoutMillis, TimeUnit.MILLISECONDS);
        if (client == null) {
            throw new IllegalStateException("No available YarnClient in pool within timeout: " + timeoutMillis + "ms");
        }
        try {
            return client.getApplicationReport(ApplicationId.fromString(applicationId));
        } finally {
            availableClients.offer(client);
        }
    }

    @Override
    public void close() throws IOException {
        for (YarnClient client : clients) {
            client.stop();
            client.close();
        }
        clients.clear();
        availableClients.clear();
    }
}
