package com.example.kafkalab.consumer.kafka.dlt;

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

@Component
public class DltConsumer {

    private static final Logger log = LoggerFactory.getLogger(DltConsumer.class);
    private static final String PATTERN = "DLT";
    private static final String GROUP = "dlt-demo-group";

    // Main consumer - always fails to send to DLT
    @KafkaListener(
        topics = KafkaTopics.DLT,
        groupId = GROUP,
        containerFactory = "dltKafkaListenerContainerFactory"
    )
    public void consume(KafkaDemoEvent event, Acknowledgment ack) {
        KafkaLabLogger.logPatternEvent(log, PATTERN, "consumer-service", GROUP,
            KafkaTopics.DLT, event.eventId(), 0, 0,
            String.format("Processing event: %s - will fail to send to DLT", event.payload()));

        // Always fail to demonstrate DLT
        throw new RuntimeException("Intentional failure for DLT demonstration");
    }

    // DLT consumer - receives permanently failed events
    @KafkaListener(
        topics = KafkaTopics.DLT_DLT,
        groupId = "dlt-demo-dlt-group",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeDlt(KafkaDemoEvent event,
                           @Header(KafkaHeaders.RECEIVED_TOPIC) String originalTopic,
                           @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                           @Header(KafkaHeaders.OFFSET) long offset,
                           @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timestamp) {

        KafkaLabLogger.logPatternEvent(log, PATTERN, "consumer-service", "dlt-demo-dlt-group",
            KafkaTopics.DLT_DLT, event.eventId(), partition, offset,
            String.format("[DLT] Received permanently failed event from topic=%s partition=%d offset=%d timestamp=%d",
                originalTopic, partition, offset, timestamp));
    }
}