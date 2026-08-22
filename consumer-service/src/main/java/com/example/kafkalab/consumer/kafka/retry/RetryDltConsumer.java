package com.example.kafkalab.consumer.kafka.retry;

import com.example.kafkalab.common.dto.KafkaDemoEvent;
import com.example.kafkalab.common.logging.KafkaLabLogger;
import com.example.kafkalab.common.topic.KafkaTopics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class RetryDltConsumer {

    private static final Logger log = LoggerFactory.getLogger(RetryDltConsumer.class);
    private static final String PATTERN = "RETRY+DLT";
    private static final String GROUP = "retry-dlt-demo-group";
    private static final int MAX_ATTEMPTS = 3;

    private final Map<String, AtomicInteger> attemptCounts = new ConcurrentHashMap<>();

    // Main consumer with retry then DLT
    @KafkaListener(
        topics = KafkaTopics.RETRY_DLT,
        groupId = GROUP,
        containerFactory = "dltKafkaListenerContainerFactory"
    )
    public void consume(KafkaDemoEvent event, Acknowledgment ack) {
        String eventId = event.eventId();
        int attempt = attemptCounts.computeIfAbsent(eventId, k -> new AtomicInteger(0)).incrementAndGet();

        KafkaLabLogger.logPatternEvent(log, PATTERN, "consumer-service", GROUP,
            KafkaTopics.RETRY_DLT, eventId, 0, 0,
            String.format("Attempt %d/%d for event: %s", attempt, MAX_ATTEMPTS, event.payload()));

        if (attempt < MAX_ATTEMPTS) {
            KafkaLabLogger.logError(log, PATTERN, "Attempt " + attempt + " failed, will retry", null);
            throw new RuntimeException("Simulated failure on attempt " + attempt);
        }

        // Max attempts reached - will go to DLT
        KafkaLabLogger.logError(log, PATTERN, "Max attempts (" + MAX_ATTEMPTS + ") reached, sending to DLT", null);
        attemptCounts.remove(eventId);
        throw new RuntimeException("Max retries exceeded - sending to DLT");
    }

    // DLT consumer
    @KafkaListener(
        topics = KafkaTopics.RETRY_DLT_DLT,
        groupId = "retry-dlt-demo-dlt-group",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeDlt(KafkaDemoEvent event,
                           @Header(KafkaHeaders.RECEIVED_TOPIC) String originalTopic,
                           @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                           @Header(KafkaHeaders.OFFSET) long offset,
                           @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timestamp) {

        KafkaLabLogger.logPatternEvent(log, PATTERN, "consumer-service", "retry-dlt-demo-dlt-group",
            KafkaTopics.RETRY_DLT_DLT, event.eventId(), partition, offset,
            String.format("[DLT] Received permanently failed event after %d attempts from topic=%s partition=%d offset=%d",
                MAX_ATTEMPTS, originalTopic, partition, offset));
    }
}