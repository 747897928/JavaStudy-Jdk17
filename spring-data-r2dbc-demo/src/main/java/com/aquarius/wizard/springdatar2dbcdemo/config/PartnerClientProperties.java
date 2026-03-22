package com.aquarius.wizard.springdatar2dbcdemo.config;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

@Validated
@ConfigurationProperties(prefix = "demo.partner")
public class PartnerClientProperties {

    @NotBlank
    private String baseUrl;

    private Duration timeout = Duration.ofSeconds(3);

    @Min(1)
    private int maxConnections = 50;

    @Min(-1)
    private int pendingAcquireMaxCount = 200;

    private Duration pendingAcquireTimeout = Duration.ofSeconds(15);

    public String getBaseUrl() {
        return baseUrl;
    }

    public void setBaseUrl(String baseUrl) {
        this.baseUrl = baseUrl;
    }

    public Duration getTimeout() {
        return timeout;
    }

    public void setTimeout(Duration timeout) {
        this.timeout = timeout;
    }

    public int getMaxConnections() {
        return maxConnections;
    }

    public void setMaxConnections(int maxConnections) {
        this.maxConnections = maxConnections;
    }

    public int getPendingAcquireMaxCount() {
        return pendingAcquireMaxCount;
    }

    public void setPendingAcquireMaxCount(int pendingAcquireMaxCount) {
        this.pendingAcquireMaxCount = pendingAcquireMaxCount;
    }

    public Duration getPendingAcquireTimeout() {
        return pendingAcquireTimeout;
    }

    public void setPendingAcquireTimeout(Duration pendingAcquireTimeout) {
        this.pendingAcquireTimeout = pendingAcquireTimeout;
    }
}
