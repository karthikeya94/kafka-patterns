package com.example.kafkalab.streams.config;

import com.example.kafkalab.common.dto.KafkaDemoEvent;
import com.example.kafkalab.common.dto.StreamsDTOs;
import com.example.kafkalab.common.dto.EventSourcingDTOs;
import com.example.kafkalab.common.topic.KafkaTopics;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;
import org.springframework.kafka.streams.RecoveringProcessingExceptionHandler;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafkaStreams
public class StreamsConfig {

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kStreamsConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG, "kafka-lab-streams");
        props.put(org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JacksonJsonSerde.class);
        props.put(org.apache.kafka.streams.StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        props.put(org.apache.kafka.streams.StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 10 * 1024 * 1024L);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Kafka Streams processing errors are handled separately from @KafkaListener errors.
        // Spring Kafka 4.1 forwards the raw failed record, enriched with DLT headers, and
        // continues the topology instead of taking the whole Streams application down.
        props.put(org.apache.kafka.streams.StreamsConfig.PROCESSING_EXCEPTION_HANDLER_CLASS_CONFIG,
            RecoveringProcessingExceptionHandler.class);
        props.put("errors.dead.letter.queue.topic.name", KafkaTopics.STREAMS_ERROR_DLT);
        return new KafkaStreamsConfiguration(props);
    }

    // Serdes for different DTO types
    @Bean
    public JacksonJsonSerde<KafkaDemoEvent> kafkaDemoEventSerde() {
        return new JacksonJsonSerde<>(KafkaDemoEvent.class);
    }

    @Bean
    public JacksonJsonSerde<StreamsDTOs.Customer> customerSerde() {
        return new JacksonJsonSerde<>(StreamsDTOs.Customer.class);
    }

    @Bean
    public JacksonJsonSerde<StreamsDTOs.Order> orderSerde() {
        return new JacksonJsonSerde<>(StreamsDTOs.Order.class);
    }

    @Bean
    public JacksonJsonSerde<StreamsDTOs.EnrichedOrder> enrichedOrderSerde() {
        return new JacksonJsonSerde<>(StreamsDTOs.EnrichedOrder.class);
    }

    @Bean
    public JacksonJsonSerde<StreamsDTOs.AggregationResult> aggregationResultSerde() {
        return new JacksonJsonSerde<>(StreamsDTOs.AggregationResult.class);
    }

    @Bean
    public JacksonJsonSerde<StreamsDTOs.WindowResult> windowResultSerde() {
        return new JacksonJsonSerde<>(StreamsDTOs.WindowResult.class);
    }

    @Bean
    public JacksonJsonSerde<EventSourcingDTOs.OrderEvent> orderEventSerde() {
        return new JacksonJsonSerde<>(EventSourcingDTOs.OrderEvent.class);
    }
}
