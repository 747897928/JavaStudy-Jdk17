package com.aquarius.wizard.study.sparklauncher.support;

public class SubmissionNotFoundException extends RuntimeException {

    public SubmissionNotFoundException(String submissionId) {
        super("Submission not found: " + submissionId);
    }
}
