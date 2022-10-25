package com.kafka.hello.Configuration;


import java.util.HashMap;
import java.util.Map;
import java.util.Objects;


import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

@Configuration
public class KafkaProducerConfig<T> {

    @Value("${kafka.producer.bootstrap-servers}")
    private String bootstrapServers;
    @Value("${kafka.properties.schema.registry.url:}")
    private String schemaRegistryUrl;
    @Value("${kafka.properties.schema.registry.basic.auth.user.info:}")
    private String schemaRegistryUserInfo;
    @Value("${kafka.properties.basic.auth.credentials.source: USER_INFO}")
    private String schemaRegistryAuth;
    @Value("${kafka.producer.compression.type:gzip}")
    private String compressionType;
    @Value("${kafka.producer.max.request.size:1500000}")
    private int maxRequestSize;
    @Value("${kafka.properties.saslRequired}")
    private String saslRequired;
    @Value("${kafka.properties.security.protocol}")
    private String sslProtocol;
    @Value("${kafka.properties.sasl.mechanism}")
    private String saslMechanism;
    @Value("${kafka.producer.login-module}")
    private String loginModule;

    @Value("${kafka.producer.acks-config}")
    private String producerAcksConfig;
    @Value("${kafka.producer.linger}")
    private int producerLinger;
    @Value("${kafka.producer.batch-size}")
    private int producerBatchSize;
    @Value("${kafka.producer.send-buffer}")
    private int producerSendBuffer;
    @Value("${kafka.producer.request.timeout.ms}")
    private int producerRequestTimeoutMs;
    @Value("${kafka.producer.retry.backoff.ms}")
    private int retryBackoffMs;

    @Bean
    @Primary
    public ProducerFactory<String, T> producerFactoryForAvro() {
        Map<String, Object> properties = new HashMap<>();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                KafkaAvroSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                KafkaAvroSerializer.class.getName());
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,compressionType);
        properties.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG,maxRequestSize);
        properties.put(ProducerConfig.LINGER_MS_CONFIG, producerLinger);
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, producerRequestTimeoutMs);
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, producerBatchSize);
        properties.put(ProducerConfig.SEND_BUFFER_CONFIG, producerSendBuffer);
        properties.put(ProducerConfig.ACKS_CONFIG, producerAcksConfig);
        properties.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, retryBackoffMs);

        addSecurityProperties(properties, saslMechanism, sslProtocol, loginModule);
        addSchemaRegistryProperties(properties);
        return new DefaultKafkaProducerFactory<>(properties);
    }

    @Bean
    public ProducerFactory<String, T> producerFactoryForJson() {
        Map<String, Object> properties = new HashMap<>();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class);
        properties.put(ProducerConfig.LINGER_MS_CONFIG, producerLinger);
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, producerRequestTimeoutMs);
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, producerBatchSize);
        properties.put(ProducerConfig.SEND_BUFFER_CONFIG, producerSendBuffer);
        properties.put(ProducerConfig.ACKS_CONFIG, producerAcksConfig);
        properties.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, retryBackoffMs);

        addSecurityProperties(properties, saslMechanism, sslProtocol, loginModule);
        return new DefaultKafkaProducerFactory<>(properties);
    }

    @Bean
    @Primary
    public KafkaTemplate<String, T> kafkaTemplateForAvro() {
        return new KafkaTemplate<>(producerFactoryForAvro());
    }

    @Bean
    public KafkaTemplate<String, T> kafkaTemplateForJson() {
        return new KafkaTemplate<>(producerFactoryForJson());
    }

    private void addSchemaRegistryProperties(Map<String, Object> properties) {
        if (Objects.nonNull(schemaRegistryUrl) && !schemaRegistryUrl.isEmpty()) {
            properties.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
            properties.put(AbstractKafkaSchemaSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE, schemaRegistryAuth);
            properties.put(AbstractKafkaSchemaSerDeConfig.USER_INFO_CONFIG, schemaRegistryUserInfo);
            properties.put("specific.avro.reader", true);
        }
    }

    private void addSecurityProperties(Map<String, Object> properties, String saslMechanism, String securityProtocol,
                                       String loginModule) {
        if (Boolean.parseBoolean(saslRequired)) {
            properties.put("security.protocol", securityProtocol);
            properties.put("sasl.mechanism", saslMechanism);
            properties.put("sasl.jaas.config", loginModule);
        }
    }


}