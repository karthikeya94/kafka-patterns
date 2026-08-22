package com.example.kafkalab.consumer.kafka.pubsub;

import com.example.kafkalab.common.dto.KafkaDemoEvent;
import com.example.kafkalab.common.logging.KafkaLabLogger;
import com.example.kafkalab.common.topic.KafkaTopics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class PubSubConsumers {

    private static final Logger log = LoggerFactory.getLogger(PubSubConsumers.class);
    private static final String PATTERN = "PUB/SUB";
    private static final String TOPIC = KafkaTopics.PUBSUB;

    @KafkaListener(
        topics = TOPIC,
        groupId = "notification-service-group",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void notificationConsumer(KafkaDemoEvent event) {
        KafkaLabLogger.logPatternEvent(log, PATTERN, "consumer-service", "notification-service-group",
            TOPIC, event.eventId(), 0, 0,
            String.format("[NOTIFICATION] received: %s", event.payload()));
    }

    @KafkaListener(
        topics = TOPIC,
        groupId = "audit-service-group",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void auditConsumer(KafkaDemoEvent event) {
        KafkaLabLogger.logPatternEvent(log, PATTERN, "consumer-service", "audit-service-group",
            TOPIC, event.eventId(), 0, 0,
            String.format("[AUDIT] received: %s", event.payload()));
    }

    @KafkaListener(
        topics = TOPIC,
        groupId = "analytics-service-group",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void analyticsConsumer(KafkaDemoEvent event) {
        KafkaLabLogger.logPatternEvent(log, PATTERN, "consumer-service", "analytics-service-group",
            TOPIC, event.eventId(), 0, 0,
            String.format("[ANALYTICS] received: %s", event.payload()));
    }
}