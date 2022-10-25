package com.kafka.hello.Controller;


import com.kafka.hello.Employee;
import com.kafka.hello.Producer.ProducerServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping(value = "/kafka")
public class KafkaController<T> {

    @Value("${kafka.avro-topic}")
    private String avroTopic;
    @Value("${kafka.avro-retry-topic}")
    private String avroRetryTopic;
    @Value("${kafka.avro-dlt-topic}")
    private String avroDltTopic;
    @Value("${kafka.avro-claims-check-topic}")
    private String avroClaimsCheckTopic;

    @Value("${kafka.json-topic}")
    private String jsonTopic;
    @Value("${kafka.json-retry-topic}")
    private String jsonRetryTopic;
    @Value("${kafka.json-dlt-topic}")
    private String jsonDltTopic;
    @Value("${kafka.json-claims-check-topic}")
    private String jsonClaimsCheckTopic;

    @Autowired
    private ProducerServiceImpl<T> producer;


    @PostMapping(value = "/string")
    public void sendStringMessage(@RequestParam("message") String message) throws IOException {

        Map<String, String> topics = new HashMap<>();
        topics.put("MAIN_TOPIC",jsonTopic);
        topics.put("RETRY_TOPIC",jsonRetryTopic);
        topics.put("DLT_TOPIC",jsonDltTopic);
        topics.put("CLAIMS_CHECK_TOPIC",jsonClaimsCheckTopic);


        this.producer.sendMessage(topics,(T) message);
    }


    @PostMapping(value = "/avro")
    public void sendAvroMessage() {
        Map<String, String> topics = new HashMap<>();
        topics.put("MAIN_TOPIC",avroTopic);
        topics.put("RETRY_TOPIC",avroRetryTopic);
        topics.put("DLT_TOPIC",avroDltTopic);
        topics.put("CLAIMS_CHECK_TOPIC",avroClaimsCheckTopic);

        Employee message = Employee.newBuilder().setFirstName("Anushka").setLastName("Mathur").build();
        this.producer.sendMessage(topics,(T) message);
    }



}