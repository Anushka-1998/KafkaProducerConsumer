package com.kafka.hello.validator;

import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import org.springframework.transaction.TransactionTimedOutException;

import java.util.Map;
import java.util.Objects;

@Slf4j
@Component
@NoArgsConstructor
public class ConfigValidator {

    @Autowired
    private ApplicationContext context;

    public boolean claimsCheckTopicNotPresent(String topic) {
        return (Objects.isNull(topic))
                || topic.isEmpty()
                || topic.startsWith("${");
    }

    public boolean sendToRetryTopic(String retryTopic, RuntimeException ex) {
        return retryTopicIsPresent(retryTopic)
                && ((ex instanceof TransactionTimedOutException) || (ex instanceof TimeoutException)
                || (Objects.nonNull(ex.getCause()) && (ex.getCause() instanceof TopicAuthorizationException)));
    }

    public boolean retryTopicIsPresent(String retryTopic) {
        return ((Objects.nonNull(retryTopic)) && !(retryTopic.isEmpty()) && !(retryTopic.startsWith("${")));
    }

    public boolean dltTopicIsPresent(String dltTopic) {
        return ((Objects.nonNull(dltTopic)) && !(dltTopic.isEmpty()) && !(dltTopic.startsWith("${")));
    }



}
