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
public class KafkaRetryController {

    private static final Logger log = LoggerFactory.getLogger(KafkaRetryController.class);
    private static final String PATTERN = "RETRY";

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public KafkaRetryController(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping("/retry")
    public Map<String, Object> sendRetryMessage(@RequestBody Map<String, Object> request) {
        String message = (String) request.getOrDefault("message", "Retry test message");
        String failMessage = (String) request.getOrDefault("failMessage", "");
        Integer failAttempts = (Integer) request.getOrDefault("failAttempts", 0);
        String customerId = (String) request.getOrDefault("customerId", "CUST-" + System.currentTimeMillis());

        KafkaDemoEvent event = KafkaDemoEvent.builder()
            .eventType("RETRY_DEMO")
            .customerId(customerId)
            .orderId("ORD-" + System.currentTimeMillis())
            .amount(new BigDecimal("100.00"))
            .payload(message)
            .metadata(Map.of(
                "source", "producer-service",
                "pattern", "retry",
                "failMessage", failMessage,
                "failAttempts", String.valueOf(failAttempts)
            ))
            .build();

        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(KafkaTopics.RETRY, customerId, event);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                KafkaLabLogger.logError(log, PATTERN, "Failed to send retry message", ex);
            } else {
                KafkaLabLogger.logProducerSend(log, PATTERN, KafkaTopics.RETRY, event.eventId(), customerId,
                    result.getRecordMetadata().partition());
            }
        });

        return Map.of(
            "eventId", event.eventId(),
            "topic", KafkaTopics.RETRY,
            "customerId", customerId,
            "status", "sent"
        );
    }
}