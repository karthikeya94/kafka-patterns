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
        /*
            The key in the kafka.send method determines which partition a message is sent to within a topic and guarantees the order of execution for related messages.
            If you do not provide a key, Kafka sends messages using a default round-robin or sticky partitioning strategy, meaning related data will be scattered across different partitions.
            Here is a breakdown of why the key is critical:
            ## 1. Strictly Guarantees Message Ordering
            Kafka only guarantees message ordering within a single partition.

            * With a Key: Kafka hashes the key (e.g., user_id_123) to calculate a specific partition number. Every single message sent with user_id_123 will always land in that exact same partition. This ensures that an Order_Created event is processed before an Order_Cancelled event for that specific user.
            * Without a Key: The creation event might go to Partition 1 and the cancellation event to Partition 2. If the listener reading Partition 2 is faster, your system will try to cancel an order that doesn't exist yet.

            ## 2. Enables Log Compaction
            If you enable log compaction on a Kafka topic, Kafka acts like a database table. It retains only the latest message value for any given key and discards older, duplicate updates. This relies entirely on you passing a unique identifier as the key (e.g., a product ID or customer ID).
            ## 3. powers Stream Joins and Aggregations
            When using processing frameworks like Kafka Streams or Flink, data from different topics can only be joined or aggregated together if they share the exact same key. This ensures the matching data lands on the same processing node.
         */
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