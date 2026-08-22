package com.example.kafkalab.consumer.kafka.retry;

import com.example.kafkalab.common.dto.KafkaDemoEvent;
import com.example.kafkalab.common.logging.KafkaLabLogger;
import com.example.kafkalab.common.topic.KafkaTopics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class RetryConsumer {

    private static final Logger log = LoggerFactory.getLogger(RetryConsumer.class);
    private static final String PATTERN = "RETRY";
    private static final String GROUP = "retry-demo-group";

    // Track attempt counts per eventId
    private final Map<String, AtomicInteger> attemptCounts = new ConcurrentHashMap<>();

    @KafkaListener(
        topics = KafkaTopics.RETRY,
        groupId = GROUP,
        containerFactory = "retryKafkaListenerContainerFactory"
    )
    public void consume(KafkaDemoEvent event, Acknowledgment ack) {
        String eventId = event.eventId();
        int attempt = attemptCounts.computeIfAbsent(eventId, k -> new AtomicInteger(0)).incrementAndGet();

        String failMessage = event.metadata() != null ? event.metadata().getOrDefault("failMessage", "") : "";
        int failAttempts = event.metadata() != null ?
            Integer.parseInt(event.metadata().getOrDefault("failAttempts", "0")) : 0;

        KafkaLabLogger.logPatternEvent(log, PATTERN, "consumer-service", GROUP,
            KafkaTopics.RETRY, eventId, 0, 0,
            String.format("Attempt %d for event: %s (failMessage=%s, failAttempts=%d)",
                attempt, event.payload(), failMessage, failAttempts));

        // Simulate failure based on payload or metadata
        boolean shouldFail = "FAIL-ONCE".equals(failMessage) && attempt == 1 ||
                             "FAIL-TWICE".equals(failMessage) && attempt <= 2 ||
                             failAttempts > 0 && attempt <= failAttempts;

        if (shouldFail) {
            KafkaLabLogger.logError(log, PATTERN, "Attempt " + attempt + " failed intentionally", null);
            // Don't acknowledge - will retry
            throw new RuntimeException("Simulated failure on attempt " + attempt);
        }

        // Success - acknowledge
        attemptCounts.remove(eventId);
        ack.acknowledge();
        KafkaLabLogger.logPatternEvent(log, PATTERN, "consumer-service", GROUP,
            KafkaTopics.RETRY, eventId, 0, 0,
            String.format("Attempt %d succeeded for event: %s", attempt, event.payload()));
    }
}