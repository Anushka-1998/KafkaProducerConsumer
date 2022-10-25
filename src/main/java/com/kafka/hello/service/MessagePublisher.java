package com.kafka.hello.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

@Slf4j
@Service
public class MessagePublisher<T> {

    @Autowired
    private KafkaTemplate<String, T> kafkaTemplateAvro;

    @Autowired
    private KafkaTemplate<String, T> kafkaTemplateJson;


    public void publishMessage(ProducerRecord<String,T> producerRecord) {
        try {
            Schema schema = ReflectData.get().getSchema(producerRecord.value().getClass());
            ListenableFuture<SendResult<String, T>> future = getKafkaTemplate(schema).send(producerRecord);
            future.addCallback(new ListenableFutureCallback() {
                @Override
                public void onSuccess(Object result) {
                    System.out.println("Paylod sent successfully");
                }

                @Override
                public void onFailure(Throwable ex) {
                    System.out.println("Failure sending payload");
                }
            });
        }catch(Exception e){
            throw e;
        }
    }

    public KafkaTemplate<String, T> getKafkaTemplate(Schema schema) {
        return schema.getName().equalsIgnoreCase("String") ? kafkaTemplateJson : kafkaTemplateAvro;
    }
}
