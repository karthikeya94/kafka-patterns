package com.example.kafkalab.streams.topology.transform;

import com.example.kafkalab.common.dto.KafkaDemoEvent;
import com.example.kafkalab.common.logging.KafkaLabLogger;
import com.example.kafkalab.common.topic.KafkaTopics;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;

@Configuration
public class TransformTopology {

    private static final Logger log = LoggerFactory.getLogger(TransformTopology.class);
    private static final String PATTERN = "TRANSFORM";
    private static final String APP_ID = "streams-transform-demo";

    @Bean
    public KStream<String, KafkaDemoEvent> transformStream(StreamsBuilder builder, JacksonJsonSerde<KafkaDemoEvent> eventSerde) {
        KStream<String, KafkaDemoEvent> input = builder.stream(
            KafkaTopics.STREAMS_TRANSFORM_INPUT, Consumed.with(Serdes.String(), eventSerde));

        KStream<String, KafkaDemoEvent> transformed = input.mapValues((key, event) -> {
            String upperPayload = event.payload().toUpperCase();
            KafkaLabLogger.logStreamsEvent(log, PATTERN, APP_ID, KafkaTopics.STREAMS_TRANSFORM_INPUT,
                key, event.payload(),
                String.format("input=%s output=%s", event.payload(), upperPayload));

            return new KafkaDemoEvent(
                event.eventId(), event.correlationId(), event.eventType(),
                event.customerId(), event.orderId(), event.amount(),
                upperPayload, event.timestamp(), event.metadata()
            );
        });

        transformed.to(KafkaTopics.STREAMS_TRANSFORM_OUTPUT, Produced.with(Serdes.String(), eventSerde));

        return transformed;
    }
}
