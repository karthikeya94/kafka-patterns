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
public class KafkaReplayController {

    private static final Logger log = LoggerFactory.getLogger(KafkaReplayController.class);
    private static final String PATTERN = "REPLAY";

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public KafkaReplayController(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping("/replay")
    public Map<String, Object> sendReplayMessage(@RequestBody Map<String, Object> request) {
        String message = (String) request.getOrDefault("message", "Replay test message");
        String customerId = (String) request.getOrDefault("customerId", "CUST-" + System.currentTimeMillis());
        Integer count = (Integer) request.getOrDefault("count", 3);

        for (int i = 0; i < count; i++) {
            KafkaDemoEvent event = KafkaDemoEvent.builder()
                .eventType("REPLAY_DEMO")
                .customerId(customerId)
                .orderId("ORD-" + System.currentTimeMillis() + "-" + i)
                .amount(new BigDecimal(100 + i * 10))
                .payload(message + " #" + (i + 1))
                .metadata(Map.of("source", "producer-service", "pattern", "replay", "sequence", String.valueOf(i)))
                .build();

            CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(KafkaTopics.EVENT_REPLAY, customerId, event);

            future.whenComplete((result, ex) -> {
                if (ex != null) {
                    KafkaLabLogger.logError(log, PATTERN, "Failed to send replay message", ex);
                } else {
                    KafkaLabLogger.logProducerSend(log, PATTERN, KafkaTopics.EVENT_REPLAY, event.eventId(), customerId,
                        result.getRecordMetadata().partition());
                }
            });
        }

        return Map.of(
            "topic", KafkaTopics.EVENT_REPLAY,
            "customerId", customerId,
            "count", count,
            "status", "sent"
        );
    }
}