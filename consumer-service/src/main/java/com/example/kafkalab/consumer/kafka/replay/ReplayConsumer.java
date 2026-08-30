package com.example.kafkalab.consumer.kafka.replay;

import com.example.kafkalab.common.dto.KafkaDemoEvent;
import com.example.kafkalab.common.logging.KafkaLabLogger;
import com.example.kafkalab.common.topic.KafkaTopics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.Collection;

@Component
public class ReplayConsumer implements ConsumerSeekAware {

    private static final Logger log = LoggerFactory.getLogger(ReplayConsumer.class);
    private static final String PATTERN = "REPLAY";
    private static final String GROUP = "event-replay-demo-group";

    private ConsumerSeekCallback seekCallback;

    // 1. Spring automatically injects the callback when the listener container starts
    @Override
    public void registerSeekCallback(ConsumerSeekCallback callback) {
        this.seekCallback = callback;
    }

    @KafkaListener(
            topics = KafkaTopics.EVENT_REPLAY,
            groupId = GROUP,
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(KafkaDemoEvent event, Acknowledgment ack) {
        KafkaLabLogger.logPatternEvent(log, PATTERN, "consumer-service", GROUP,
                KafkaTopics.EVENT_REPLAY, event.eventId(), 0, 0,
                String.format("Processed replay event: %s (sequence=%s)", event.payload(),
                        event.metadata() != null ? event.metadata().get("sequence") : "N/A"));

        ack.acknowledge();
    }

    // 2. Custom method to trigger the replay on demand
    public void replayFromBeginning() {
        if (this.seekCallback != null) {
            log.info("Initiating Replay: Rewinding consumer offsets to the beginning for all assigned partitions...");
            // This instructs the consumer to reset its offset to 0 for all partitions it is assigned to
            this.seekCallback.seekToBeginning(KafkaTopics.EVENT_REPLAY,0);
        } else {
            log.warn("Cannot initiate replay: Consumer is not fully initialized.");
        }
    }

    // Optional: Replay from a specific point in time
    public void replayFromTimestamp(long timestamp) {
        if (this.seekCallback != null) {
            log.info("Initiating Replay: Rewinding consumer to timestamp: {}", timestamp);
            this.seekCallback.seekToTimestamp(KafkaTopics.EVENT_REPLAY,0,timestamp);
        }
    }
}