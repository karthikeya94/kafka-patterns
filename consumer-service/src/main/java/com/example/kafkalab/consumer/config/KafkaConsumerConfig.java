package com.example.kafkalab.consumer.config;

import com.example.kafkalab.common.dto.KafkaDemoEvent;
import com.example.kafkalab.common.dto.RequestReplyDTOs;
import com.example.kafkalab.common.dto.EventSourcingDTOs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    // Default consumer factory for KafkaDemoEvent
    @Bean
    public ConsumerFactory<String, KafkaDemoEvent> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JacksonJsonDeserializer.class);
        props.put(JacksonJsonDeserializer.TRUSTED_PACKAGES, "com.example.kafkalab.common.dto");
        props.put(JacksonJsonDeserializer.VALUE_DEFAULT_TYPE, KafkaDemoEvent.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "default-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    // Request/Reply consumer factory
    @Bean
    public ConsumerFactory<String, RequestReplyDTOs.Request> requestReplyConsumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JacksonJsonDeserializer.class);
        props.put(JacksonJsonDeserializer.TRUSTED_PACKAGES, "com.example.kafkalab.common.dto");
        props.put(JacksonJsonDeserializer.VALUE_DEFAULT_TYPE, RequestReplyDTOs.Request.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "request-reply-demo-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    // Event Sourcing consumer factory
    @Bean
    public ConsumerFactory<String, EventSourcingDTOs.OrderEvent> eventSourcingConsumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JacksonJsonDeserializer.class);
        props.put(JacksonJsonDeserializer.TRUSTED_PACKAGES, "com.example.kafkalab.common.dto");
        props.put(JacksonJsonDeserializer.VALUE_DEFAULT_TYPE, EventSourcingDTOs.OrderEvent.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "event-sourcing-demo-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, KafkaDemoEvent> kafkaListenerContainerFactory(
            ConsumerFactory<String, KafkaDemoEvent> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, KafkaDemoEvent> factory =
            new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(1);
        return factory;
    }

    @Bean
    public ProducerFactory<String, Object> dltProducerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        props.put( ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JacksonJsonSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG,"all");
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, Object> dltKafkaTemplate(@Qualifier("dltProducerFactory")ProducerFactory<String, Object> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    // Factory with DLT for dead letter demonstrations
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, KafkaDemoEvent> dltKafkaListenerContainerFactory(
            ConsumerFactory<String, KafkaDemoEvent> consumerFactory,
            org.springframework.kafka.core.KafkaTemplate<String, Object> dltKafkaTemplate) {
        ConcurrentKafkaListenerContainerFactory<String, KafkaDemoEvent> factory =
            new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(1);
        // Retry 3 times then send to DLT
        var recoverer = new DeadLetterPublishingRecoverer(dltKafkaTemplate,
            (record, ex) -> new org.apache.kafka.common.TopicPartition(record.topic() + "-dlt", record.partition()));
        factory.setCommonErrorHandler(new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 3L)));
        return factory;
    }

    // Factory with retry for retry demonstrations
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, KafkaDemoEvent> retryKafkaListenerContainerFactory(
            ConsumerFactory<String, KafkaDemoEvent> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, KafkaDemoEvent> factory =
            new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(1);
        // Retry 3 times with 1 second fixed backoff
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.setCommonErrorHandler(new DefaultErrorHandler(new FixedBackOff(1000L, 3L)));
        return factory;
    }

    // Factory with backoff retry
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, KafkaDemoEvent> backoffRetryKafkaListenerContainerFactory(
            ConsumerFactory<String, KafkaDemoEvent> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, KafkaDemoEvent> factory =
            new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(1);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        // Retry 3 times with increasing backoff: 2s, 5s, 10s
        factory.setCommonErrorHandler(new DefaultErrorHandler(
            (record, ex) -> {}, // Exception handler
            new FixedBackOff(2000L, 3L) // Initial 2s, max 3 retries
        ));
        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, RequestReplyDTOs.Request> requestReplyListenerContainerFactory(
            ConsumerFactory<String, RequestReplyDTOs.Request> consumerFactory, KafkaTemplate<String, Object> dltKafkaTemplate) {
        ConcurrentKafkaListenerContainerFactory<String, RequestReplyDTOs.Request> factory =
            new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(1);
        factory.setReplyTemplate(dltKafkaTemplate);
        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, EventSourcingDTOs.OrderEvent> eventSourcingListenerContainerFactory(
            ConsumerFactory<String, EventSourcingDTOs.OrderEvent> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, EventSourcingDTOs.OrderEvent> factory =
            new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(1);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        return factory;
    }
}
