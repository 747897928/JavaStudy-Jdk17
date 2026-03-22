package com.aquarius.wizard.springdatar2dbcdemo.exception;

import com.aquarius.wizard.springdatar2dbcdemo.dto.ErrorResponse;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.support.WebExchangeBindException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.stream.Collectors;

@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(WebExchangeBindException.class)
    public Mono<ResponseEntity<ErrorResponse>> handleBindException(
            WebExchangeBindException ex,
            ServerWebExchange exchange
    ) {
        String message = ex.getFieldErrors().stream()
                .map(this::formatFieldError)
                .collect(Collectors.joining("; "));

        ErrorResponse body = new ErrorResponse(
                Instant.now(),
                400,
                "Bad Request",
                message,
                exchange.getRequest().getPath().value()
        );
        return Mono.just(ResponseEntity.badRequest().body(body));
    }

    @ExceptionHandler(ResponseStatusException.class)
    public Mono<ResponseEntity<ErrorResponse>> handleResponseStatusException(
            ResponseStatusException ex,
            ServerWebExchange exchange
    ) {
        int status = ex.getStatusCode().value();
        ErrorResponse body = new ErrorResponse(
                Instant.now(),
                status,
                ex.getStatusCode().toString(),
                ex.getReason(),
                exchange.getRequest().getPath().value()
        );
        return Mono.just(ResponseEntity.status(ex.getStatusCode()).body(body));
    }

    private String formatFieldError(FieldError fieldError) {
        return fieldError.getField() + ": " + fieldError.getDefaultMessage();
    }
}
