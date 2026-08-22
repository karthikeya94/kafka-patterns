package com.example.kafkalab.streams.topology.branch;

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
public class BranchTopology {

    private static final Logger log = LoggerFactory.getLogger(BranchTopology.class);
    private static final String PATTERN = "BRANCH";
    private static final String APP_ID = "streams-branch-demo";

    private static final BigDecimal HIGH_VALUE_THRESHOLD = new BigDecimal("10000");

    @Bean
    public KStream<String, KafkaDemoEvent> branchStream(StreamsBuilder builder, JacksonJsonSerde<KafkaDemoEvent> eventSerde) {
        KStream<String, KafkaDemoEvent> input = builder.stream(
            KafkaTopics.STREAMS_BRANCH_INPUT, Consumed.with(Serdes.String(), eventSerde));

        // The predicates are mutually exclusive: negative values are suspicious,
        // then high-value values, and the remaining non-negative values are normal.
        input.filter((key, event) -> event.amount().compareTo(BigDecimal.ZERO) < 0)
            .peek((key, event) -> KafkaLabLogger.logStreamsEvent(log, PATTERN, APP_ID, KafkaTopics.STREAMS_BRANCH_INPUT,
                key, event.amount().toString(), "SUSPICIOUS: amount=" + event.amount() + " < 0"))
            .to(KafkaTopics.STREAMS_BRANCH_SUSPICIOUS, Produced.with(Serdes.String(), eventSerde));

        input.filter((key, event) -> event.amount().compareTo(HIGH_VALUE_THRESHOLD) >= 0)
            .peek((key, event) ->
            KafkaLabLogger.logStreamsEvent(log, PATTERN, APP_ID, KafkaTopics.STREAMS_BRANCH_INPUT,
                key, event.amount().toString(),
                String.format("HIGH-VALUE: amount=%s >= %s", event.amount(), HIGH_VALUE_THRESHOLD))
            ).to(KafkaTopics.STREAMS_BRANCH_HIGH_VALUE, Produced.with(Serdes.String(), eventSerde));

        input.filter((key, event) -> event.amount().compareTo(BigDecimal.ZERO) >= 0
                && event.amount().compareTo(HIGH_VALUE_THRESHOLD) < 0)
            .peek((key, event) ->
            KafkaLabLogger.logStreamsEvent(log, PATTERN, APP_ID, KafkaTopics.STREAMS_BRANCH_INPUT,
                key, event.amount().toString(),
                String.format("NORMAL: amount=%s", event.amount()))
            ).to(KafkaTopics.STREAMS_BRANCH_NORMAL, Produced.with(Serdes.String(), eventSerde));

        return input;
    }
}
