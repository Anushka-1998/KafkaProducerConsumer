package com.kafka.hello.service;

import java.io.IOException;
import java.util.Map;

public interface ClaimsCheckService<T> {

    void handleClaimsCheckAfterGettingMemoryIssue(Map<String,String> claimsCheckTopic,
                                                  T message) throws IOException;
}
