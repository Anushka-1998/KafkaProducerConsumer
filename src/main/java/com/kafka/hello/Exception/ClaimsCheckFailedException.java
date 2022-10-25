package com.kafka.hello.Exception;

public class ClaimsCheckFailedException extends RuntimeException {

    public ClaimsCheckFailedException(String message, Throwable e) {
        super(message, e);
    }

    public ClaimsCheckFailedException(String message) {
        super(message);
    }
}
