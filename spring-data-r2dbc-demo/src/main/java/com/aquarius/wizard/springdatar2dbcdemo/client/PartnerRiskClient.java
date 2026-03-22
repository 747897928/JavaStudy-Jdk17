package com.aquarius.wizard.springdatar2dbcdemo.client;

import com.aquarius.wizard.springdatar2dbcdemo.dto.PartnerRiskDecision;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;

@Component
public class PartnerRiskClient {

    private final WebClient partnerWebClient;

    public PartnerRiskClient(WebClient partnerWebClient) {
        this.partnerWebClient = partnerWebClient;
    }

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
