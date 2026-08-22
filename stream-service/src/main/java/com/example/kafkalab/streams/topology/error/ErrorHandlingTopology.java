package com.example.kafkalab.streams.topology.error;

import com.example.kafkalab.common.dto.KafkaDemoEvent;
import com.example.kafkalab.common.logging.KafkaLabLogger;
import com.example.kafkalab.common.topic.KafkaTopics;
import org.apache.kafka.common.serialization.Serdes;
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
public class ErrorHandlingTopology {

    private static final Logger log = LoggerFactory.getLogger(ErrorHandlingTopology.class);
    private static final String PATTERN = "STREAMS-ERROR";
    private static final String APP_ID = "streams-error-demo";

    @Bean
    public KStream<String, KafkaDemoEvent> errorStream(StreamsBuilder builder, JacksonJsonSerde<KafkaDemoEvent> eventSerde) {
        KStream<String, KafkaDemoEvent> input = builder.stream(
            KafkaTopics.STREAMS_ERROR_INPUT, Consumed.with(Serdes.String(), eventSerde));

        // Process with potential failure
        KStream<String, KafkaDemoEvent> processed = input.mapValues((key, event) -> {
            String payload = event.payload();

            // Check if this is a failure-triggering event
            if (payload != null && payload.contains("FAIL")) {
                KafkaLabLogger.logStreamsEvent(log, PATTERN, APP_ID, KafkaTopics.STREAMS_ERROR_INPUT,
                    key, payload,
                    "Processing will fail - sending to DLT");
                throw new RuntimeException("Intentional processing failure for DLT demonstration");
            }

            KafkaLabLogger.logStreamsEvent(log, PATTERN, APP_ID, KafkaTopics.STREAMS_ERROR_INPUT,
                key, payload,
                "Processed successfully");

            return new KafkaDemoEvent(
                event.eventId(), event.correlationId(), event.eventType(),
                event.customerId(), event.orderId(), event.amount(),
                "PROCESSED: " + payload, event.timestamp(), event.metadata()
            );
        });

        processed.to(KafkaTopics.STREAMS_ERROR_OUTPUT, Produced.with(Serdes.String(), eventSerde));

        return processed;
    }
}
