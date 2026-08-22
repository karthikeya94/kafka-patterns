package com.example.kafkalab.producer.controller;

import com.example.kafkalab.common.dto.KafkaDemoEvent;
import com.example.kafkalab.common.dto.StreamsDTOs;
import com.example.kafkalab.common.logging.KafkaLabLogger;
import com.example.kafkalab.common.topic.KafkaTopics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@RestController
@RequestMapping("/api/kafka/streams")
public class KafkaStreamsTriggerController {

    private static final Logger log = LoggerFactory.getLogger(KafkaStreamsTriggerController.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public KafkaStreamsTriggerController(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping("/transform")
    public Map<String, Object> sendTransformMessage(@RequestBody Map<String, String> request) {
        String text = request.getOrDefault("text", "hello kafka streams");
        String customerId = request.getOrDefault("customerId", "CUST-" + System.currentTimeMillis());

        KafkaDemoEvent event = KafkaDemoEvent.builder()
            .eventType("STREAMS_TRANSFORM")
            .customerId(customerId)
            .orderId("ORD-" + System.currentTimeMillis())
            .amount(new BigDecimal("100.00"))
            .payload(text)
            .metadata(Map.of("source", "producer-service", "pattern", "streams-transform"))
            .build();

        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(KafkaTopics.STREAMS_TRANSFORM_INPUT, customerId, event);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                KafkaLabLogger.logError(log, "STREAMS-TRANSFORM", "Failed to send transform message", ex);
            } else {
                KafkaLabLogger.logProducerSend(log, "STREAMS-TRANSFORM", KafkaTopics.STREAMS_TRANSFORM_INPUT,
                    event.eventId(), customerId, result.getRecordMetadata().partition());
            }
        });

        return Map.of(
            "eventId", event.eventId(),
            "topic", KafkaTopics.STREAMS_TRANSFORM_INPUT,
            "customerId", customerId,
            "status", "sent"
        );
    }

    @PostMapping("/filter")
    public Map<String, Object> sendFilterMessage(@RequestBody Map<String, Object> request) {
        String customerId = (String) request.getOrDefault("customerId", "CUST-" + System.currentTimeMillis());
        BigDecimal amount = new BigDecimal(request.getOrDefault("amount", "500").toString());

        KafkaDemoEvent event = KafkaDemoEvent.builder()
            .eventType("STREAMS_FILTER")
            .customerId(customerId)
            .orderId("ORD-" + System.currentTimeMillis())
            .amount(amount)
            .payload("Filter test: amount=" + amount)
            .metadata(Map.of("source", "producer-service", "pattern", "streams-filter"))
            .build();

        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(KafkaTopics.STREAMS_FILTER_INPUT, customerId, event);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                KafkaLabLogger.logError(log, "STREAMS-FILTER", "Failed to send filter message", ex);
            } else {
                KafkaLabLogger.logProducerSend(log, "STREAMS-FILTER", KafkaTopics.STREAMS_FILTER_INPUT,
                    event.eventId(), customerId, result.getRecordMetadata().partition());
            }
        });

        return Map.of(
            "eventId", event.eventId(),
            "topic", KafkaTopics.STREAMS_FILTER_INPUT,
            "customerId", customerId,
            "amount", amount,
            "status", "sent"
        );
    }

    @PostMapping("/branch")
    public Map<String, Object> sendBranchMessage(@RequestBody Map<String, Object> request) {
        String customerId = (String) request.getOrDefault("customerId", "CUST-" + System.currentTimeMillis());
        BigDecimal amount = new BigDecimal(request.getOrDefault("amount", "5000").toString());

        KafkaDemoEvent event = KafkaDemoEvent.builder()
            .eventType("STREAMS_BRANCH")
            .customerId(customerId)
            .orderId("ORD-" + System.currentTimeMillis())
            .amount(amount)
            .payload("Branch test: amount=" + amount)
            .metadata(Map.of("source", "producer-service", "pattern", "streams-branch"))
            .build();

        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(KafkaTopics.STREAMS_BRANCH_INPUT, customerId, event);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                KafkaLabLogger.logError(log, "STREAMS-BRANCH", "Failed to send branch message", ex);
            } else {
                KafkaLabLogger.logProducerSend(log, "STREAMS-BRANCH", KafkaTopics.STREAMS_BRANCH_INPUT,
                    event.eventId(), customerId, result.getRecordMetadata().partition());
            }
        });

        return Map.of(
            "eventId", event.eventId(),
            "topic", KafkaTopics.STREAMS_BRANCH_INPUT,
            "customerId", customerId,
            "amount", amount,
            "status", "sent"
        );
    }

    @PostMapping("/aggregate")
    public Map<String, Object> sendAggregateMessage(@RequestBody Map<String, Object> request) {
        String customerId = (String) request.getOrDefault("customerId", "CUST-" + System.currentTimeMillis());
        BigDecimal amount = new BigDecimal(request.getOrDefault("amount", "100").toString());

        KafkaDemoEvent event = KafkaDemoEvent.builder()
            .eventType("STREAMS_AGGREGATE")
            .customerId(customerId)
            .orderId("ORD-" + System.currentTimeMillis())
            .amount(amount)
            .payload("Aggregate test: amount=" + amount)
            .metadata(Map.of("source", "producer-service", "pattern", "streams-aggregate"))
            .build();

        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(KafkaTopics.STREAMS_AGGREGATE_INPUT, customerId, event);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                KafkaLabLogger.logError(log, "STREAMS-AGGREGATE", "Failed to send aggregate message", ex);
            } else {
                KafkaLabLogger.logProducerSend(log, "STREAMS-AGGREGATE", KafkaTopics.STREAMS_AGGREGATE_INPUT,
                    event.eventId(), customerId, result.getRecordMetadata().partition());
            }
        });

        return Map.of(
            "eventId", event.eventId(),
            "topic", KafkaTopics.STREAMS_AGGREGATE_INPUT,
            "customerId", customerId,
            "amount", amount,
            "status", "sent"
        );
    }

    @PostMapping("/ktable")
    public Map<String, Object> sendKTableMessage(@RequestBody Map<String, String> request) {
        String customerId = request.getOrDefault("customerId", "CUST-" + System.currentTimeMillis());
        String tier = request.getOrDefault("tier", "GOLD");

        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(
            KafkaTopics.STREAMS_KTABLE_INPUT, customerId, tier);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                KafkaLabLogger.logError(log, "STREAMS-KTABLE", "Failed to send ktable message", ex);
            } else {
                KafkaLabLogger.logProducerSend(log, "STREAMS-KTABLE", KafkaTopics.STREAMS_KTABLE_INPUT,
                    customerId, customerId, result.getRecordMetadata().partition());
            }
        });

        return Map.of(
            "topic", KafkaTopics.STREAMS_KTABLE_INPUT,
            "customerId", customerId,
            "tier", tier,
            "status", "sent"
        );
    }

    @PostMapping("/join")
    public Map<String, Object> sendJoinMessages(@RequestBody Map<String, Object> request) {
        String customerId = (String) request.getOrDefault("customerId", "CUST-" + System.currentTimeMillis());
        String customerName = (String) request.getOrDefault("customerName", "John Doe");
        String customerTier = (String) request.getOrDefault("customerTier", "GOLD");
        BigDecimal amount = new BigDecimal(request.getOrDefault("amount", "500").toString());
        String product = (String) request.getOrDefault("product", "Widget");

        // Send customer first
        StreamsDTOs.Customer customer = new StreamsDTOs.Customer(
            customerId, customerName, customerTier, Instant.now()
        );

        kafkaTemplate.send(KafkaTopics.STREAMS_JOIN_CUSTOMERS, customerId, customer);

        // Send order
        StreamsDTOs.Order order = new StreamsDTOs.Order(
            "ORD-" + System.currentTimeMillis(), customerId, amount, product, Instant.now()
        );

        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(KafkaTopics.STREAMS_JOIN_ORDERS, customerId, order);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                KafkaLabLogger.logError(log, "STREAMS-JOIN", "Failed to send join messages", ex);
            } else {
                KafkaLabLogger.logProducerSend(log, "STREAMS-JOIN", KafkaTopics.STREAMS_JOIN_ORDERS,
                    order.orderId(), customerId, result.getRecordMetadata().partition());
            }
        });

        return Map.of(
            "customerId", customerId,
            "orderId", order.orderId(),
            "customerTopic", KafkaTopics.STREAMS_JOIN_CUSTOMERS,
            "orderTopic", KafkaTopics.STREAMS_JOIN_ORDERS,
            "status", "sent"
        );
    }

    @PostMapping("/window")
    public Map<String, Object> sendWindowMessage(@RequestBody Map<String, Object> request) {
        String customerId = (String) request.getOrDefault("customerId", "CUST-" + System.currentTimeMillis());
        BigDecimal amount = new BigDecimal(request.getOrDefault("amount", "100").toString());

        KafkaDemoEvent event = KafkaDemoEvent.builder()
            .eventType("STREAMS_WINDOW")
            .customerId(customerId)
            .orderId("ORD-" + System.currentTimeMillis())
            .amount(amount)
            .payload("Window test: amount=" + amount)
            .metadata(Map.of("source", "producer-service", "pattern", "streams-window"))
            .build();

        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(KafkaTopics.STREAMS_WINDOW_INPUT, customerId, event);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                KafkaLabLogger.logError(log, "STREAMS-WINDOW", "Failed to send window message", ex);
            } else {
                KafkaLabLogger.logProducerSend(log, "STREAMS-WINDOW", KafkaTopics.STREAMS_WINDOW_INPUT,
                    event.eventId(), customerId, result.getRecordMetadata().partition());
            }
        });

        return Map.of(
            "eventId", event.eventId(),
            "topic", KafkaTopics.STREAMS_WINDOW_INPUT,
            "customerId", customerId,
            "amount", amount,
            "status", "sent"
        );
    }

    @PostMapping("/state")
    public Map<String, Object> sendStateMessage(@RequestBody Map<String, Object> request) {
        String customerId = (String) request.getOrDefault("customerId", "CUST-" + System.currentTimeMillis());
        BigDecimal amount = new BigDecimal(request.getOrDefault("amount", "100").toString());

        KafkaDemoEvent event = KafkaDemoEvent.builder()
            .eventType("STREAMS_STATE")
            .customerId(customerId)
            .orderId("ORD-" + System.currentTimeMillis())
            .amount(amount)
            .payload("State test: amount=" + amount)
            .metadata(Map.of("source", "producer-service", "pattern", "streams-state"))
            .build();

        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(KafkaTopics.STREAMS_STATE_INPUT, customerId, event);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                KafkaLabLogger.logError(log, "STREAMS-STATE", "Failed to send state message", ex);
            } else {
                KafkaLabLogger.logProducerSend(log, "STREAMS-STATE", KafkaTopics.STREAMS_STATE_INPUT,
                    event.eventId(), customerId, result.getRecordMetadata().partition());
            }
        });

        return Map.of(
            "eventId", event.eventId(),
            "topic", KafkaTopics.STREAMS_STATE_INPUT,
            "customerId", customerId,
            "amount", amount,
            "status", "sent"
        );
    }

    /**
     * Sends a record that intentionally fails inside the Streams topology. Spring Kafka's
     * RecoveringProcessingExceptionHandler forwards it to streams-error-demo-dlt.
     */
    @PostMapping("/error")
    public Map<String, Object> sendErrorMessage(@RequestBody Map<String, String> request) {
        String customerId = request.getOrDefault("customerId", "CUST-" + System.currentTimeMillis());
        String payload = request.getOrDefault("payload", "FAIL-STREAMS");
        KafkaDemoEvent event = KafkaDemoEvent.builder()
            .eventType("STREAMS_ERROR")
            .customerId(customerId)
            .payload(payload)
            .amount(BigDecimal.ZERO)
            .metadata(Map.of("source", "producer-service", "pattern", "streams-error"))
            .build();

        kafkaTemplate.send(KafkaTopics.STREAMS_ERROR_INPUT, customerId, event)
            .whenComplete((result, exception) -> {
                if (exception != null) {
                    KafkaLabLogger.logError(log, "STREAMS-ERROR", "Failed to send error demonstration", exception);
                } else {
                    KafkaLabLogger.logProducerSend(log, "STREAMS-ERROR", KafkaTopics.STREAMS_ERROR_INPUT,
                        event.eventId(), customerId, result.getRecordMetadata().partition());
                }
            });

        return Map.of("eventId", event.eventId(), "topic", KafkaTopics.STREAMS_ERROR_INPUT,
            "dltTopic", KafkaTopics.STREAMS_ERROR_DLT, "status", "sent");
    }
}
