package com.kafka.hello.Producer;


import com.kafka.hello.service.ClaimsCheckServiceImpl;
import com.kafka.hello.validator.ConfigValidator;
import com.kafka.hello.service.MessagePublisher;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TimeoutException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Recover;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.TransactionTimedOutException;

import java.util.Map;

@Service
@Slf4j
public class ProducerServiceImpl<T> implements ProducerService<T>{

    @Autowired
    private ConfigValidator inputValidator;

    @Autowired
    MessagePublisher<T> messagePublisher;

    @Autowired
    ClaimsCheckServiceImpl<T> claimsCheckService;

   @Override
   @Retryable(value = { TransactionTimedOutException.class, TimeoutException.class}, maxAttempts = 3 , backoff = @Backoff(delay = 500,multiplier = 2, maxDelay = 1000))
   public void sendMessage(Map<String, String> topics, T message) {
       try {
           ProducerRecord<String, T> producerRecord = new ProducerRecord(topics.get("MAIN_TOPIC"), (T) message);
           messagePublisher.publishMessage(producerRecord);

       }catch(Exception ex){
        /*   if (ex.getCause() instanceof RecordTooLargeException) {
               claimsCheckService.handleClaimsCheckAfterGettingMemoryIssue(topics, message);
           } else {
               log.error("Exception occurred while posting Payload to target kafka topic ", ex);
               throw ex;
           }*/
       }
    }

    @Recover
    public void sendMessageAvroRetryDltTopic(RuntimeException e,Map<String, String> topics,T message) {
        if(inputValidator.sendToRetryTopic(topics.get("RETRY_TOPIC"),e)){
            sendMessageAvroRetryTopic(e,topics, message);
        } else{
            sendMessageAvroDltTopic(e,message,topics);}
    }


    public void sendMessageAvroRetryTopic(RuntimeException e,Map<String, String> topics, T employee) {
        try {
            ProducerRecord<String, T> producerRecord = new ProducerRecord(topics.get("RETRY_TOPIC"), (T) employee);
            messagePublisher.publishMessage(producerRecord);
        }catch(Exception ex){
            sendMessageAvroDltTopic(e,employee,topics);
        }
    }

    public void sendMessageAvroDltTopic(RuntimeException e, T employee,Map<String, String> topics  ) {
        if(inputValidator.dltTopicIsPresent(topics.get("DLT_TOPIC"))) {
            ProducerRecord<String, T> producerRecord = new ProducerRecord(topics.get("DLT_TOPIC"), (T) employee);
            messagePublisher.publishMessage(producerRecord);
        }else{
            throw e;
        }
    }
}