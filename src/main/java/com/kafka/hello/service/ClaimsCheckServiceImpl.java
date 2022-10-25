package com.kafka.hello.service;

import com.kafka.hello.AzureConfig.FileService;
import com.kafka.hello.ClaimsCheckPayload;
import com.kafka.hello.validator.ConfigValidator;
import com.kafka.hello.Exception.ClaimsCheckFailedException;
import com.kafka.hello.util.CompressionUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
public class ClaimsCheckServiceImpl<T> implements ClaimsCheckService<T> {

    @Autowired
    ConfigValidator configValidator;

    @Autowired
    FileService fileService;

    @Autowired
    MessagePublisher<T> messagePublisher;

    @Value("${azure.storage.container-name}")
    private String containerName;

    @Value("${events-payload.file-name}")
    private String blobItemNamePrefix;

    @Override
    public void handleClaimsCheckAfterGettingMemoryIssue(Map<String,String> topics,
                                                         T message)  {
        ProducerRecord<String,T> producerRecord;
        ClaimsCheckPayload claimsCheckPayload = null;
        if(configValidator.claimsCheckTopicNotPresent(topics.get("CLAIMS_CHECK_TOPIC"))){
            throw new ClaimsCheckFailedException("Claims check topic not found");
        }
        try {
            String url = uploadToAzureBlob(CompressionUtil.gzipCompress(message));
            log.info("Url: "+url);
            claimsCheckPayload = ClaimsCheckPayload.newBuilder().setClaimsCheckBlobUrl(url).build();
            log.info("claimsCheckPayload: "+claimsCheckPayload);
            producerRecord = new ProducerRecord<>(topics.get("CLAIMS_CHECK_TOPIC"),(T) claimsCheckPayload);
            messagePublisher.publishMessage(producerRecord);
        }catch (Exception e){
            log.error("Error while posting data to claimsCheckTopic");
        }


    }

    public String uploadToAzureBlob(byte[] compressedPayload) throws ClaimsCheckFailedException {
        try {
            return fileService.uploadFile(compressedPayload, containerName, blobItemNamePrefix + UUID.randomUUID() + "_"
                    + TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis()));
        } catch (Exception e) {
            throw new ClaimsCheckFailedException("Claims check failed while uploading to blob", e);
        }
    }
}
