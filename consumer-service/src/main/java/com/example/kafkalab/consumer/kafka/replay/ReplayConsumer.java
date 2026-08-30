package com.example.kafkalab.consumer.kafka.replay;

import com.example.kafkalab.common.dto.KafkaDemoEvent;
import com.example.kafkalab.common.logging.KafkaLabLogger;
import com.example.kafkalab.common.topic.KafkaTopics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AbstractConsumerSeekAware;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.Collection;

@Component
public class ReplayConsumer extends AbstractConsumerSeekAware {

    private static final Logger log = LoggerFactory.getLogger(ReplayConsumer.class);
    private static final String PATTERN = "REPLAY";
    private static final String GROUP = "event-replay-demo-group";

    /*private ConsumerSeekCallback seekCallback;

    // 1. Spring automatically injects the callback when the listener container starts
    @Override
    public void registerSeekCallback(ConsumerSeekCallback callback) {
        this.seekCallback = callback;
    }*/

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

    public void replayFromBeginning() {
        seekToBeginning();
    }

    public void replayFromTimestamp(long timestamp) {
        seekToTimestamp(timestamp);
    }
}