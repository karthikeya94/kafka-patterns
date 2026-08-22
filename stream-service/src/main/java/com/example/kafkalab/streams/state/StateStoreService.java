package com.example.kafkalab.streams.state;

import com.example.kafkalab.common.dto.KafkaDemoEvent;
import com.example.kafkalab.common.dto.StreamsDTOs;
import com.example.kafkalab.common.logging.KafkaLabLogger;
import com.example.kafkalab.common.topic.KafkaTopics;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;

import java.math.BigDecimal;
import java.time.Instant;

@Configuration
public class StateStoreService {

    private static final Logger log = LoggerFactory.getLogger(StateStoreService.class);
    private static final String PATTERN = "STATE-STORE";
    private static final String APP_ID = "streams-state-demo";
    private static final String STATE_STORE = "customer-order-counts";

    @Bean
    public StoreBuilder<KeyValueStore<String, StreamsDTOs.StateResult>> stateStore() {
        return Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(STATE_STORE),
            Serdes.String(),
            new JacksonJsonSerde<>(StreamsDTOs.StateResult.class)
        );
    }

    @Bean
    public KStream<String, KafkaDemoEvent> stateStream(
            StreamsBuilder builder,
            JacksonJsonSerde<KafkaDemoEvent> eventSerde) {

        KStream<String, KafkaDemoEvent> input = builder.stream(
            KafkaTopics.STREAMS_STATE_INPUT, Consumed.with(Serdes.String(), eventSerde));

        // Group by customerId and aggregate using a state store
        KTable<String, StreamsDTOs.StateResult> aggregated = input
            .groupBy((key, event) -> event.customerId())
            .aggregate(
                () -> new StreamsDTOs.StateResult(null, 0, BigDecimal.ZERO),
                (customerId, event, state) -> {
                    long newCount = state.orderCount() + 1;
                    BigDecimal newTotal = state.totalAmount().add(event.amount());
                    StreamsDTOs.StateResult updated = new StreamsDTOs.StateResult(
                        customerId, newCount, newTotal
                    );

                    KafkaLabLogger.logStreamsEvent(log, PATTERN, APP_ID, KafkaTopics.STREAMS_STATE_INPUT,
                        customerId, event.amount().toString(),
                        String.format("State updated: count=%d total=%s", newCount, newTotal));

                    return updated;
                },
                Materialized.<String, StreamsDTOs.StateResult, KeyValueStore<org.apache.kafka.common.utils.Bytes, byte[]>>as(STATE_STORE)
                    .withKeySerde(Serdes.String())
                    .withValueSerde(new JacksonJsonSerde<>(StreamsDTOs.StateResult.class))
            );

        // Also output to a topic for observation
        aggregated.toStream().to(KafkaTopics.STREAMS_STATE_OUTPUT,
            Produced.with(Serdes.String(), new JacksonJsonSerde<>(StreamsDTOs.StateResult.class)));

        return input;
    }
}
