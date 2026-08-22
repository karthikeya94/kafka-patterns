package com.example.kafkalab.streams.controller;

import com.example.kafkalab.common.dto.StreamsDTOs;
import com.example.kafkalab.common.logging.KafkaLabLogger;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.Map;

@RestController
@RequestMapping("/api/kafka/streams")
public class StreamsQueryController {

    private static final Logger log = LoggerFactory.getLogger(StreamsQueryController.class);

    private final StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    public StreamsQueryController(@Qualifier("&kafkaStreamsBuilder") StreamsBuilderFactoryBean streamsBuilderFactoryBean) {
        this.streamsBuilderFactoryBean = streamsBuilderFactoryBean;
    }

    @GetMapping("/aggregate/{customerId}")
    public Map<String, Object> getAggregate(@PathVariable String customerId) {
        KafkaStreams streams = streamsBuilderFactoryBean.getKafkaStreams();
        if (streams == null) {
            return Map.of("error", "Streams not running");
        }

        ReadOnlyKeyValueStore<String, StreamsDTOs.AggregationResult> store =
            streams.store(StoreQueryParameters.fromNameAndType("customer-aggregates", QueryableStoreTypes.keyValueStore()));

        StreamsDTOs.AggregationResult result = store.get(customerId);
        if (result == null) {
            return Map.of("customerId", customerId, "found", false);
        }

        return Map.of(
            "customerId", result.customerId(),
            "totalAmount", result.totalAmount(),
            "orderCount", result.orderCount(),
            "windowStart", result.windowStart(),
            "windowEnd", result.windowEnd()
        );
    }

    @GetMapping("/ktable/{customerId}")
    public Map<String, Object> getKTable(@PathVariable String customerId) {
        KafkaStreams streams = streamsBuilderFactoryBean.getKafkaStreams();
        if (streams == null) {
            return Map.of("error", "Streams not running");
        }

        ReadOnlyKeyValueStore<String, String> store =
            streams.store(StoreQueryParameters.fromNameAndType("customer-tiers", QueryableStoreTypes.keyValueStore()));

        String tier = store.get(customerId);
        if (tier == null) {
            return Map.of("customerId", customerId, "found", false);
        }

        return Map.of(
            "customerId", customerId,
            "tier", tier
        );
    }

    @GetMapping("/state/{customerId}")
    public Map<String, Object> getState(@PathVariable String customerId) {
        KafkaStreams streams = streamsBuilderFactoryBean.getKafkaStreams();
        if (streams == null) {
            return Map.of("error", "Streams not running");
        }

        ReadOnlyKeyValueStore<String, StreamsDTOs.StateResult> store =
            streams.store(StoreQueryParameters.fromNameAndType("customer-order-counts", QueryableStoreTypes.keyValueStore()));

        StreamsDTOs.StateResult result = store.get(customerId);
        if (result == null) {
            return Map.of("customerId", customerId, "found", false);
        }

        return Map.of(
            "customerId", result.customerId(),
            "orderCount", result.orderCount(),
            "totalAmount", result.totalAmount()
        );
    }

    @GetMapping("/state/all")
    public Map<String, Object> getAllState() {
        KafkaStreams streams = streamsBuilderFactoryBean.getKafkaStreams();
        if (streams == null) {
            return Map.of("error", "Streams not running");
        }

        ReadOnlyKeyValueStore<String, StreamsDTOs.StateResult> store =
            streams.store(StoreQueryParameters.fromNameAndType("customer-order-counts", QueryableStoreTypes.keyValueStore()));

        try (KeyValueIterator<String, StreamsDTOs.StateResult> iter = store.all()) {
            Map<String, Object> results = new java.util.LinkedHashMap<>();
            while (iter.hasNext()) {
                var entry = iter.next();
                results.put(entry.key, Map.of(
                    "orderCount", entry.value.orderCount(),
                    "totalAmount", entry.value.totalAmount()
                ));
            }
            return Map.of("states", results);
        }
    }
}
