package com.kafka.hello.Producer;

import java.util.Map;

public interface ProducerService<T> {

    public void sendMessage(Map<String, String> topics,T message);

}
