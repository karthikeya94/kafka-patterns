package com.example.kafkalab.streams.topology.filter;

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

import java.math.BigDecimal;

@Configuration
public class FilterTopology {

    private static final Logger log = LoggerFactory.getLogger(FilterTopology.class);
    private static final String PATTERN = "FILTER";
    private static final String APP_ID = "streams-filter-demo";

    private static final BigDecimal THRESHOLD = new BigDecimal("1000");

    @Bean
    public KStream<String, KafkaDemoEvent> filterStream(StreamsBuilder builder, JacksonJsonSerde<KafkaDemoEvent> eventSerde) {
        KStream<String, KafkaDemoEvent> input = builder.stream(
            KafkaTopics.STREAMS_FILTER_INPUT, Consumed.with(Serdes.String(), eventSerde));

        KStream<String, KafkaDemoEvent> filtered = input.filter((key, event) -> {
            boolean passes = event.amount().compareTo(THRESHOLD) > 0;
            if (passes) {
                KafkaLabLogger.logStreamsEvent(log, PATTERN, APP_ID, KafkaTopics.STREAMS_FILTER_INPUT,
                    key, event.amount().toString(),
                    String.format("amount=%s PASSES filter (threshold=%s)", event.amount(), THRESHOLD));
            } else {
                KafkaLabLogger.logStreamsEvent(log, PATTERN, APP_ID, KafkaTopics.STREAMS_FILTER_INPUT,
                    key, event.amount().toString(),
                    String.format("amount=%s FILTERED OUT (threshold=%s)", event.amount(), THRESHOLD));
            }
            return passes;
        });

        filtered.to(KafkaTopics.STREAMS_FILTER_OUTPUT, Produced.with(Serdes.String(), eventSerde));

        return filtered;
    }
}
