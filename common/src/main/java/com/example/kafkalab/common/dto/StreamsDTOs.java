package com.example.kafkalab.common.dto;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * DTOs for Kafka Streams patterns.
 */
public final class StreamsDTOs {

    /**
     * Customer for join demonstrations.
     */
    public record Customer(
        String customerId,
        String name,
        String tier,
        Instant updatedAt
    ) {
        public Customer {
            if (updatedAt == null) updatedAt = Instant.now();
        }
    }

    /**
     * Order for join demonstrations.
     */
    public record Order(
        String orderId,
        String customerId,
        BigDecimal amount,
        String product,
        Instant timestamp
    ) {
        public Order {
            if (timestamp == null) timestamp = Instant.now();
        }
    }

    /**
     * Enriched order after joining order with customer.
     */
    public record EnrichedOrder(
        String orderId,
        String customerId,
        String customerName,
        String customerTier,
        BigDecimal amount,
        String product,
        Instant timestamp
    ) {
        public EnrichedOrder {
            if (timestamp == null) timestamp = Instant.now();
        }
    }

    /**
     * Aggregation result per customer.
     */
    public record AggregationResult(
        String customerId,
        BigDecimal totalAmount,
        long orderCount,
        Instant windowStart,
        Instant windowEnd
    ) {}

    /**
     * Window aggregation result.
     */
    public record WindowResult(
        String windowKey,
        long count,
        BigDecimal sum,
        Instant windowStart,
        Instant windowEnd
    ) {}

    /**
     * State store query result.
     */
    public record StateResult(
        String customerId,
        long orderCount,
        BigDecimal totalAmount
    ) {}

    private StreamsDTOs() {}
}