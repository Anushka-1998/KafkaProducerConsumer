package com.kafka.hello.Consumer;

import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
@Slf4j
public class Consumer<T> {



    @Value("${kafka.avro-topic}")
    private String avroTopic;

    @Value("${kafka.json-topic}")
    private String stringTopic;

    @KafkaListener(topics = {"avro-topic","avro-retry-topic","avro-dlt-topic"}, groupId = "group_id_avro",containerFactory = "kafkaListenerContainerFactoryAvro")
    public void consumeAvro(T message) throws IOException {
        log.info(" Consumed message {}  ", message);
    }

    @KafkaListener(topics = {"json-topic","json-retry-topic","json-dlt-topic"}, groupId = "group_id_json",containerFactory = "kafkaListenerContainerFactoryJson")
    public void consumeJson(T message) throws IOException {
        log.info(" Consumed message {}  ", message);
    }
}