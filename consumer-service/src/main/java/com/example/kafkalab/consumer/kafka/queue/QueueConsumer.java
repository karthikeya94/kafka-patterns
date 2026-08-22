package com.example.kafkalab.consumer.kafka.queue;

import com.example.kafkalab.common.dto.KafkaDemoEvent;
import com.example.kafkalab.common.logging.KafkaLabLogger;
import com.example.kafkalab.common.topic.KafkaTopics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class QueueConsumer {

    private static final Logger log = LoggerFactory.getLogger(QueueConsumer.class);
    private static final String PATTERN = "QUEUE";
    private static final String GROUP = "queue-demo-group";

    @KafkaListener(
        topics = KafkaTopics.QUEUE,
        groupId = GROUP,
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(KafkaDemoEvent event) {
        String consumerId = "Consumer-" + Thread.currentThread().getName().split("-")[1];

        KafkaLabLogger.logPatternEvent(log, PATTERN, "consumer-service", GROUP,
            KafkaTopics.QUEUE, event.eventId(), 0, 0,
            String.format("Consumer %s processed event: %s", consumerId, event.payload()));
    }
}