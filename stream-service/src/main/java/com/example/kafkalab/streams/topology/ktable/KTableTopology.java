package com.example.kafkalab.streams.topology.ktable;

import com.example.kafkalab.common.logging.KafkaLabLogger;
import com.example.kafkalab.common.topic.KafkaTopics;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;

@Configuration
public class KTableTopology {

    private static final Logger log = LoggerFactory.getLogger(KTableTopology.class);
    private static final String PATTERN = "KTABLE";
    private static final String APP_ID = "streams-ktable-demo";
    private static final String STATE_STORE = "customer-tiers";

    @Bean
    public KTable<String, String> ktableStream(StreamsBuilder builder) {

        // Create KTable from input topic - KTable represents latest value per key
        KTable<String, String> customerTiers = builder.table(
            KafkaTopics.STREAMS_KTABLE_INPUT,
            Consumed.with(Serdes.String(), Serdes.String()),
            Materialized.<String, String, KeyValueStore<org.apache.kafka.common.utils.Bytes, byte[]>>as(STATE_STORE)
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.String())
        );

        // Log changes to the KTable (latest value per key)
        customerTiers.toStream().peek((key, tier) ->
            KafkaLabLogger.logStreamsEvent(log, PATTERN, APP_ID, KafkaTopics.STREAMS_KTABLE_INPUT,
                key, tier,
                String.format("KTable latest value for customer %s: %s", key, tier))
        ).to(KafkaTopics.STREAMS_KTABLE_OUTPUT, Produced.with(Serdes.String(), Serdes.String()));

        return customerTiers;
    }
}
