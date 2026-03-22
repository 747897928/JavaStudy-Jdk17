package com.aquarius.wizard.springdatar2dbcdemo.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;
import reactor.netty.resources.ConnectionProvider;

/**
 * 下游 HTTP 客户端配置。
 * <p>
 * 这里使用 Reactor Netty + WebClient，避免在 WebFlux 中退回阻塞式 HTTP 客户端。
 */
@Configuration(proxyBeanMethods = false)
public class WebClientConfig {

    /**
     * 用于访问模拟下游服务的响应式客户端。
     */
    @Bean
    public WebClient partnerWebClient(PartnerClientProperties properties) {
        ConnectionProvider connectionProvider = ConnectionProvider.builder("partner-http")
                .maxConnections(properties.getMaxConnections())
                .pendingAcquireMaxCount(properties.getPendingAcquireMaxCount())
                .pendingAcquireTimeout(properties.getPendingAcquireTimeout())
                .build();

        HttpClient httpClient = HttpClient.create(connectionProvider)
                .compress(true)
                .responseTimeout(properties.getTimeout());

        return WebClient.builder()
                .baseUrl(properties.getBaseUrl())
                .clientConnector(new ReactorClientHttpConnector(httpClient))
                .defaultHeader(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE)
                .build();
    }
}
