package com.aquarius.wizard.springdatar2dbcdemo.client;

import com.aquarius.wizard.springdatar2dbcdemo.dto.PartnerRiskDecision;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;

/**
 * 模拟下游风控/运费服务客户端。
 * <p>
 * 这个类存在的目的不是业务复杂度，而是明确演示：
 * WebFlux 项目里下游 HTTP 调用也应该尽量使用 {@link WebClient} 这类非阻塞客户端。
 */
@Component
public class PartnerRiskClient {

    private final WebClient partnerWebClient;

    public PartnerRiskClient(WebClient partnerWebClient) {
        this.partnerWebClient = partnerWebClient;
    }

    /**
     * 调用下游接口，返回订单风控等级和运费决策。
     */
    public Mono<PartnerRiskDecision> evaluate(String customerTier, BigDecimal orderAmount) {
        return partnerWebClient.get()
                .uri(uriBuilder -> uriBuilder
                        .path("/partner-api/risk-assessments")
                        .queryParam("customerTier", customerTier)
                        .queryParam("orderAmount", orderAmount)
                        .build())
                .retrieve()
                .bodyToMono(PartnerRiskDecision.class)
                .onErrorResume(ex -> Mono.error(new ResponseStatusException(
                        HttpStatus.BAD_GATEWAY,
                        "Partner risk service call failed",
                        ex
                )));
    }
}
