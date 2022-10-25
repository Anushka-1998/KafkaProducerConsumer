package com.kafka.hello;

import org.springframework.boot.*;
import org.springframework.boot.autoconfigure.*;
import org.springframework.retry.annotation.EnableRetry;

@SpringBootApplication
@EnableAutoConfiguration
@EnableRetry
public class KafkaApplication {

    public static void main(String[] args)  {
        SpringApplication.run(KafkaApplication.class, args);
    }

}
