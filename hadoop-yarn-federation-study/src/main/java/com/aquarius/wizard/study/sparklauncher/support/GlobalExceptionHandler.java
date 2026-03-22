package com.aquarius.wizard.study.sparklauncher.support;

import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * 统一异常出口。
 * 让调用方拿到稳定的 HTTP 错误响应，而不是直接暴露堆栈细节。
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(SubmissionNotFoundException.class)
    public ProblemDetail handleSubmissionNotFound(SubmissionNotFoundException exception) {
        ProblemDetail problemDetail = ProblemDetail.forStatus(HttpStatus.NOT_FOUND);
        problemDetail.setTitle("Submission Not Found");
        problemDetail.setDetail(exception.getMessage());
        return problemDetail;
    }

    @ExceptionHandler(Exception.class)
    public ProblemDetail handleGenericException(Exception exception) {
        ProblemDetail problemDetail = ProblemDetail.forStatus(HttpStatus.INTERNAL_SERVER_ERROR);
        problemDetail.setTitle("Gateway Request Failed");
        problemDetail.setDetail(exception.getMessage());
        return problemDetail;
    }
}
