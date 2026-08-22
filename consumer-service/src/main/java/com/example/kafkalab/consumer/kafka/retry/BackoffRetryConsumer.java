package com.example.kafkalab.consumer.kafka.retry;

import com.example.kafkalab.common.dto.KafkaDemoEvent;
import com.example.kafkalab.common.logging.KafkaLabLogger;
import com.example.kafkalab.common.topic.KafkaTopics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.time.LocalTime;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class BackoffRetryConsumer {

    private static final Logger log = LoggerFactory.getLogger(BackoffRetryConsumer.class);
    private static final String PATTERN = "RETRY-BACKOFF";
    private static final String GROUP = "retry-backoff-demo-group";

    private final Map<String, AtomicInteger> attemptCounts = new ConcurrentHashMap<>();

    // Fixed backoff delays in milliseconds: attempt 1=0, attempt 2=2000, attempt 3=5000, attempt 4=10000
    private static final long[] BACKOFF_DELAYS = {0, 2000, 5000, 10000};

    @KafkaListener(
        topics = KafkaTopics.RETRY_BACKOFF,
        groupId = GROUP,
        containerFactory = "backoffRetryKafkaListenerContainerFactory"
    )
    public void consume(KafkaDemoEvent event, Acknowledgment ack) {
        String eventId = event.eventId();
        int attempt = attemptCounts.computeIfAbsent(eventId, k -> new AtomicInteger(0)).incrementAndGet();

        String time = LocalTime.now().toString();

        KafkaLabLogger.logPatternEvent(log, PATTERN, "consumer-service", GROUP,
            KafkaTopics.RETRY_BACKOFF, eventId, 0, 0,
            String.format("attempt=%d time=%s delay=%dms", attempt, time,
                attempt <= BACKOFF_DELAYS.length ? BACKOFF_DELAYS[attempt - 1] : BACKOFF_DELAYS[BACKOFF_DELAYS.length - 1]));

        // Fail first 3 attempts, succeed on 4th
        if (attempt < 4) {
            KafkaLabLogger.logError(log, PATTERN, "Attempt " + attempt + " failed, will retry with backoff", null);
            throw new RuntimeException("Simulated failure on attempt " + attempt);
        }

        // Success on 4th attempt
        attemptCounts.remove(eventId);
        ack.acknowledge();
        KafkaLabLogger.logPatternEvent(log, PATTERN, "consumer-service", GROUP,
            KafkaTopics.RETRY_BACKOFF, eventId, 0, 0,
            String.format("Attempt %d succeeded at %s", attempt, time));
    }
}