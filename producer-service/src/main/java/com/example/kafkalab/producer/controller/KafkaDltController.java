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
public class KafkaDltController {

    private static final Logger log = LoggerFactory.getLogger(KafkaDltController.class);
    private static final String PATTERN = "DLT";

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public KafkaDltController(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping("/dlt")
    public Map<String, Object> sendDltMessage(@RequestBody Map<String, Object> request) {
        String message = (String) request.getOrDefault("message", "DLT test message - will fail permanently");
        String customerId = (String) request.getOrDefault("customerId", "CUST-" + System.currentTimeMillis());

        KafkaDemoEvent event = KafkaDemoEvent.builder()
            .eventType("DLT_DEMO")
            .customerId(customerId)
            .orderId("ORD-" + System.currentTimeMillis())
            .amount(new BigDecimal("300.00"))
            .payload(message)
            .metadata(Map.of("source", "producer-service", "pattern", "dlt"))
            .build();

        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(KafkaTopics.DLT, customerId, event);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                KafkaLabLogger.logError(log, PATTERN, "Failed to send DLT message", ex);
            } else {
                KafkaLabLogger.logProducerSend(log, PATTERN, KafkaTopics.DLT, event.eventId(), customerId,
                    result.getRecordMetadata().partition());
            }
        });

        return Map.of(
            "eventId", event.eventId(),
            "topic", KafkaTopics.DLT,
            "customerId", customerId,
            "status", "sent"
        );
    }
}