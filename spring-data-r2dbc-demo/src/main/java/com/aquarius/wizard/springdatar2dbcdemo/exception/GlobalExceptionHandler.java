package com.aquarius.wizard.springdatar2dbcdemo.exception;

import com.aquarius.wizard.springdatar2dbcdemo.dto.ErrorResponse;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.support.WebExchangeBindException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.HandlerMethodValidationException;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.stream.Collectors;

/**
 * WebFlux 全局异常处理器。
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    /**
     * 处理请求参数校验失败。
     */
    @ExceptionHandler(WebExchangeBindException.class)
    public Mono<ResponseEntity<ErrorResponse>> handleBindException(
            WebExchangeBindException ex,
            ServerWebExchange exchange
    ) {
        String message = ex.getFieldErrors().stream()
                .map(this::formatFieldError)
                .collect(Collectors.joining("; "));

        return Mono.just(ResponseEntity.badRequest().body(buildErrorResponse(
                400,
                "Bad Request",
                message,
                exchange
        )));
    }

    /**
     * 处理方法参数校验失败，例如 @RequestParam / @PathVariable 上的约束未通过。
     */
    @ExceptionHandler(HandlerMethodValidationException.class)
    public Mono<ResponseEntity<ErrorResponse>> handleHandlerMethodValidationException(
            HandlerMethodValidationException ex,
            ServerWebExchange exchange
    ) {
        String message = ex.getParameterValidationResults().stream()
                .flatMap(result -> result.getResolvableErrors().stream()
                        .map(error -> formatMethodValidationMessage(
                                result.getMethodParameter().getParameterName(),
                                error.getDefaultMessage()
                        )))
                .collect(Collectors.joining("; "));

        return Mono.just(ResponseEntity.badRequest().body(buildErrorResponse(
                400,
                "Bad Request",
                message,
                exchange
        )));
    }

    /**
     * 处理显式抛出的状态码异常。
     */
    @ExceptionHandler(ResponseStatusException.class)
    public Mono<ResponseEntity<ErrorResponse>> handleResponseStatusException(
            ResponseStatusException ex,
            ServerWebExchange exchange
    ) {
        int status = ex.getStatusCode().value();
        return Mono.just(ResponseEntity.status(ex.getStatusCode()).body(buildErrorResponse(
                status,
                ex.getStatusCode().toString(),
                ex.getReason(),
                exchange
        )));
    }

    private String formatFieldError(FieldError fieldError) {
        return fieldError.getField() + ": " + fieldError.getDefaultMessage();
    }

    private String formatMethodValidationMessage(String parameterName, String defaultMessage) {
        String safeParameterName = parameterName == null ? "parameter" : parameterName;
        return safeParameterName + ": " + defaultMessage;
    }

    private ErrorResponse buildErrorResponse(
            int status,
            String error,
            String message,
            ServerWebExchange exchange
    ) {
        return new ErrorResponse(
                Instant.now(),
                status,
                error,
                message,
                exchange.getRequest().getPath().value()
        );
    }
}
