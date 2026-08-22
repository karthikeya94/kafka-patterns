package com.example.kafkalab.common.dto;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;

/**
 * Core demo event used across most communication patterns.
 * Contains fields for demonstrating routing, correlation, and payload.
 */
public record KafkaDemoEvent(
    String eventId,
    String correlationId,
    String eventType,
    String customerId,
    String orderId,
    BigDecimal amount,
    String payload,
    Instant timestamp,
    Map<String, String> metadata
) {
    public KafkaDemoEvent {
        if (eventId == null) {
            eventId = java.util.UUID.randomUUID().toString();
        }
        if (correlationId == null) {
            correlationId = eventId;
        }
        if (timestamp == null) {
            timestamp = Instant.now();
        }
    }

    public static KafkaDemoEventBuilder builder() {
        return new KafkaDemoEventBuilder();
    }

    public static class KafkaDemoEventBuilder {
        private String eventId;
        private String correlationId;
        private String eventType;
        private String customerId;
        private String orderId;
        private BigDecimal amount;
        private String payload;
        private Instant timestamp;
        private Map<String, String> metadata;

        public KafkaDemoEventBuilder eventId(String eventId) { this.eventId = eventId; return this; }
        public KafkaDemoEventBuilder correlationId(String correlationId) { this.correlationId = correlationId; return this; }
        public KafkaDemoEventBuilder eventType(String eventType) { this.eventType = eventType; return this; }
        public KafkaDemoEventBuilder customerId(String customerId) { this.customerId = customerId; return this; }
        public KafkaDemoEventBuilder orderId(String orderId) { this.orderId = orderId; return this; }
        public KafkaDemoEventBuilder amount(BigDecimal amount) { this.amount = amount; return this; }
        public KafkaDemoEventBuilder payload(String payload) { this.payload = payload; return this; }
        public KafkaDemoEventBuilder timestamp(Instant timestamp) { this.timestamp = timestamp; return this; }
        public KafkaDemoEventBuilder metadata(Map<String, String> metadata) { this.metadata = metadata; return this; }

        public KafkaDemoEvent build() {
            return new KafkaDemoEvent(eventId, correlationId, eventType, customerId, orderId, amount, payload, timestamp, metadata);
        }
    }
}