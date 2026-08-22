package com.example.kafkalab.producer.controller;

import com.example.kafkalab.common.dto.KafkaDemoEvent;
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
@RequestMapping("/api/kafka")
public class KafkaPubSubController {

    private static final Logger log = LoggerFactory.getLogger(KafkaPubSubController.class);
    private static final String PATTERN_PUBSUB = "PUB/SUB";
    private static final String PATTERN_LB = "PUB/SUB+LB";

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public KafkaPubSubController(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping("/pubsub")
    public Map<String, Object> sendPubSubMessage(@RequestBody Map<String, String> request) {
        String message = request.getOrDefault("message", "Pub/Sub test message");
        String customerId = request.getOrDefault("customerId", "CUST-" + System.currentTimeMillis());

        KafkaDemoEvent event = KafkaDemoEvent.builder()
            .eventType("PUBSUB_DEMO")
            .customerId(customerId)
            .orderId("ORD-" + System.currentTimeMillis())
            .amount(new BigDecimal("250.00"))
            .payload(message)
            .metadata(Map.of("source", "producer-service", "pattern", "pubsub"))
            .build();

        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(KafkaTopics.PUBSUB, customerId, event);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                KafkaLabLogger.logError(log, PATTERN_PUBSUB, "Failed to send pub/sub message", ex);
            } else {
                KafkaLabLogger.logProducerSend(log, PATTERN_PUBSUB, KafkaTopics.PUBSUB, event.eventId(), customerId,
                    result.getRecordMetadata().partition());
            }
        });

        return Map.of(
            "eventId", event.eventId(),
            "topic", KafkaTopics.PUBSUB,
            "customerId", customerId,
            "status", "sent"
        );
    }

    @PostMapping("/pubsub-loadbalance")
    public Map<String, Object> sendPubSubLoadBalanceMessage(@RequestBody Map<String, String> request) {
        String message = request.getOrDefault("message", "Pub/Sub + LoadBalance test message");
        String customerId = request.getOrDefault("customerId", "CUST-" + System.currentTimeMillis());

        KafkaDemoEvent event = KafkaDemoEvent.builder()
            .eventType("PUBSUB_LB_DEMO")
            .customerId(customerId)
            .orderId("ORD-" + System.currentTimeMillis())
            .amount(new BigDecimal("500.00"))
            .payload(message)
            .metadata(Map.of("source", "producer-service", "pattern", "pubsub-loadbalance"))
            .build();

        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(KafkaTopics.PUBSUB_LOADBALANCE, customerId, event);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                KafkaLabLogger.logError(log, PATTERN_LB, "Failed to send pub/sub+LB message", ex);
            } else {
                KafkaLabLogger.logProducerSend(log, PATTERN_LB, KafkaTopics.PUBSUB_LOADBALANCE, event.eventId(), customerId,
                    result.getRecordMetadata().partition());
            }
        });

        return Map.of(
            "eventId", event.eventId(),
            "topic", KafkaTopics.PUBSUB_LOADBALANCE,
            "customerId", customerId,
            "status", "sent"
        );
    }
}