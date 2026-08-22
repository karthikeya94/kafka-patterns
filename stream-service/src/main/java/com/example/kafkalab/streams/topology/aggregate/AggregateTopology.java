package com.example.kafkalab.streams.topology.aggregate;

import com.example.kafkalab.common.dto.KafkaDemoEvent;
import com.example.kafkalab.common.dto.StreamsDTOs;
import com.example.kafkalab.common.logging.KafkaLabLogger;
import com.example.kafkalab.common.topic.KafkaTopics;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;

import java.math.BigDecimal;
import java.time.Instant;

@Configuration
public class AggregateTopology {

    private static final Logger log = LoggerFactory.getLogger(AggregateTopology.class);
    private static final String PATTERN = "AGGREGATION";
    private static final String APP_ID = "streams-aggregate-demo";
    private static final String STATE_STORE = "customer-aggregates";

    @Bean
    public KStream<String, KafkaDemoEvent> aggregateStream(
            StreamsBuilder builder,
            JacksonJsonSerde<KafkaDemoEvent> eventSerde,
            JacksonJsonSerde<StreamsDTOs.AggregationResult> aggSerde) {

        KStream<String, KafkaDemoEvent> input = builder.stream(
            KafkaTopics.STREAMS_AGGREGATE_INPUT, Consumed.with(Serdes.String(), eventSerde));

        // Group by customerId and aggregate
        KTable<String, StreamsDTOs.AggregationResult> aggregated = input
            .groupBy((key, event) -> event.customerId())
            .aggregate(
                () -> new StreamsDTOs.AggregationResult(null, BigDecimal.ZERO, 0, Instant.now(), Instant.now()),
                (customerId, event, agg) -> {
                    BigDecimal newTotal = agg.totalAmount().add(event.amount());
                    long newCount = agg.orderCount() + 1;
                    StreamsDTOs.AggregationResult updated = new StreamsDTOs.AggregationResult(
                        customerId, newTotal, newCount,
                        agg.windowStart(), Instant.now()
                    );

                    KafkaLabLogger.logStreamsEvent(log, PATTERN, APP_ID, KafkaTopics.STREAMS_AGGREGATE_INPUT,
                        customerId, event.amount().toString(),
                        String.format("Aggregated: total=%s count=%d", newTotal, newCount));

                    return updated;
                },
                Materialized.<String, StreamsDTOs.AggregationResult, KeyValueStore<org.apache.kafka.common.utils.Bytes, byte[]>>as(STATE_STORE)
                    .withKeySerde(Serdes.String())
                    .withValueSerde(aggSerde)
            );

        // Output to topic
        aggregated.toStream().to(KafkaTopics.STREAMS_AGGREGATE_OUTPUT, Produced.with(Serdes.String(), aggSerde));

        return input;
    }
}
