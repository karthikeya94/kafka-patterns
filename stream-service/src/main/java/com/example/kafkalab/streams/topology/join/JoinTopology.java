package com.example.kafkalab.streams.topology.join;

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

import java.time.Instant;

@Configuration
public class JoinTopology {

    private static final Logger log = LoggerFactory.getLogger(JoinTopology.class);
    private static final String PATTERN = "JOIN";
    private static final String APP_ID = "streams-join-demo";

    // Demonstrates KStream-KTable join (stream of orders joined with table of customers)
    @Bean
    public KStream<String, StreamsDTOs.EnrichedOrder> joinStream(
            StreamsBuilder builder,
            JacksonJsonSerde<StreamsDTOs.Customer> customerSerde,
            JacksonJsonSerde<StreamsDTOs.Order> orderSerde,
            JacksonJsonSerde<StreamsDTOs.EnrichedOrder> enrichedSerde) {

        // KTable for customers (latest customer info per customerId)
        KTable<String, StreamsDTOs.Customer> customers = builder.table(
            KafkaTopics.STREAMS_JOIN_CUSTOMERS,
            Consumed.with(Serdes.String(), customerSerde),
            Materialized.<String, StreamsDTOs.Customer, KeyValueStore<org.apache.kafka.common.utils.Bytes, byte[]>>as("customer-table")
                .withKeySerde(Serdes.String())
                .withValueSerde(customerSerde)
        );

        // KStream for orders
        KStream<String, StreamsDTOs.Order> orders = builder.stream(
            KafkaTopics.STREAMS_JOIN_ORDERS, Consumed.with(Serdes.String(), orderSerde));

        // Join orders with customer table - KStream-KTable join
        KStream<String, StreamsDTOs.EnrichedOrder> enrichedOrders = orders
            .join(customers,
                (order, customer) -> new StreamsDTOs.EnrichedOrder(
                    order.orderId(),
                    order.customerId(),
                    customer != null ? customer.name() : "Unknown",
                    customer != null ? customer.tier() : "Unknown",
                    order.amount(),
                    order.product(),
                    order.timestamp()
                ));

        enrichedOrders.peek((key, enriched) ->
            KafkaLabLogger.logStreamsEvent(log, PATTERN, APP_ID, KafkaTopics.STREAMS_JOIN_ORDERS,
                key, enriched.amount().toString(),
                String.format("Enriched Order: customer=%s name=%s tier=%s amount=%s",
                    enriched.customerId(), enriched.customerName(), enriched.customerTier(), enriched.amount()))
        ).to(KafkaTopics.STREAMS_JOIN_OUTPUT, Produced.with(Serdes.String(), enrichedSerde));

        return enrichedOrders;
    }
}
