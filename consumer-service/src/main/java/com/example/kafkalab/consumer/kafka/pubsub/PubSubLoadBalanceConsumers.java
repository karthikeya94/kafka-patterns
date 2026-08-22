package com.example.kafkalab.consumer.kafka.pubsub;

import com.example.kafkalab.common.dto.KafkaDemoEvent;
import com.example.kafkalab.common.logging.KafkaLabLogger;
import com.example.kafkalab.common.topic.KafkaTopics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class PubSubLoadBalanceConsumers {

    private static final Logger log = LoggerFactory.getLogger(PubSubLoadBalanceConsumers.class);
    private static final String PATTERN = "PUB/SUB+LB";
    private static final String TOPIC = KafkaTopics.PUBSUB_LOADBALANCE;

    // Inventory group - 2 consumers
    @KafkaListener(
        topics = TOPIC,
        groupId = "inventory-group",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void inventoryConsumerA(KafkaDemoEvent event) {
        KafkaLabLogger.logPatternEvent(log, PATTERN, "consumer-service", "inventory-group",
            TOPIC, event.eventId(), 0, 0,
            String.format("[INVENTORY-A] received: %s", event.payload()));
    }

    @KafkaListener(
        topics = TOPIC,
        groupId = "inventory-group",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void inventoryConsumerB(KafkaDemoEvent event) {
        KafkaLabLogger.logPatternEvent(log, PATTERN, "consumer-service", "inventory-group",
            TOPIC, event.eventId(), 0, 0,
            String.format("[INVENTORY-B] received: %s", event.payload()));
    }

    // Payment group - 2 consumers
    @KafkaListener(
        topics = TOPIC,
        groupId = "payment-group",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void paymentConsumerA(KafkaDemoEvent event) {
        KafkaLabLogger.logPatternEvent(log, PATTERN, "consumer-service", "payment-group",
            TOPIC, event.eventId(), 0, 0,
            String.format("[PAYMENT-A] received: %s", event.payload()));
    }

    @KafkaListener(
        topics = TOPIC,
        groupId = "payment-group",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void paymentConsumerB(KafkaDemoEvent event) {
        KafkaLabLogger.logPatternEvent(log, PATTERN, "consumer-service", "payment-group",
            TOPIC, event.eventId(), 0, 0,
            String.format("[PAYMENT-B] received: %s", event.payload()));
    }

    // Notification group - 2 consumers
    @KafkaListener(
        topics = TOPIC,
        groupId = "notification-group",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void notificationConsumerA(KafkaDemoEvent event) {
        KafkaLabLogger.logPatternEvent(log, PATTERN, "consumer-service", "notification-group",
            TOPIC, event.eventId(), 0, 0,
            String.format("[NOTIFICATION-A] received: %s", event.payload()));
    }

    @KafkaListener(
        topics = TOPIC,
        groupId = "notification-group",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void notificationConsumerB(KafkaDemoEvent event) {
        KafkaLabLogger.logPatternEvent(log, PATTERN, "consumer-service", "notification-group",
            TOPIC, event.eventId(), 0, 0,
            String.format("[NOTIFICATION-B] received: %s", event.payload()));
    }
}