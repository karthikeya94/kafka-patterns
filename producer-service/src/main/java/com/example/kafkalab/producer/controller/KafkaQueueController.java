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
public class KafkaQueueController {

    private static final Logger log = LoggerFactory.getLogger(KafkaQueueController.class);
    private static final String PATTERN = "QUEUE";

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public KafkaQueueController(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping("/queue")
    public Map<String, Object> sendQueueMessage(@RequestBody Map<String, String> request) {
        String message = request.getOrDefault("message", "Queue test message");
        String customerId = request.getOrDefault("customerId", "CUST-" + System.currentTimeMillis());

        KafkaDemoEvent event = KafkaDemoEvent.builder()
            .eventType("QUEUE_DEMO")
            .customerId(customerId)
            .orderId("ORD-" + System.currentTimeMillis())
            .amount(new BigDecimal("100.00"))
            .payload(message)
            .metadata(Map.of("source", "producer-service", "pattern", "queue"))
            .build();

        // Use customerId as key to demonstrate partitioning
        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(KafkaTopics.QUEUE, customerId, event);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                KafkaLabLogger.logError(log, PATTERN, "Failed to send queue message", ex);
            } else {
                KafkaLabLogger.logProducerSend(log, PATTERN, KafkaTopics.QUEUE, event.eventId(), customerId,
                    result.getRecordMetadata().partition());
            }
        });

        return Map.of(
            "eventId", event.eventId(),
            "topic", KafkaTopics.QUEUE,
            "customerId", customerId,
            "status", "sent"
        );
    }
}