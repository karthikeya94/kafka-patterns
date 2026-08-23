package com.example.kafkalab.consumer.kafka.dlt;

import com.example.kafkalab.common.dto.KafkaDemoEvent;
import com.example.kafkalab.common.logging.KafkaLabLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.kafka.annotation.BackOff;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;

import org.springframework.kafka.retrytopic.DltStrategy;

import org.springframework.kafka.support.KafkaHeaders;

import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Component
public class DltConsumer {

    private static final Logger log =
            LoggerFactory.getLogger(DltConsumer.class);

    private static final String MANUAL_TOPIC = "manual-dlt-demo";
    private static final String MANUAL_DLT_TOPIC = "manual-dlt-demo-dlt";

    private static final String ANNOTATION_TOPIC = "annotation-dlt-demo";
    private static final String ANNOTATION_DLT_TOPIC =
            "annotation-dlt-demo-dlt";

    @KafkaListener(topics = MANUAL_TOPIC, groupId = "manual-dlt-consumer-group", containerFactory = "dltKafkaListenerContainerFactory")
    public void consumeManualDlt(KafkaDemoEvent event) {
        KafkaLabLogger.logPatternEvent(log, "DLT-MANUAL","consumer-service", "manual-dlt-consumer-group", MANUAL_TOPIC, event.eventId(),
                0, 0, String.format("Processing event: %s - intentionally failing", event.payload())
        );
        throw new RuntimeException("Intentional failure for MANUAL DLT demo");
    }

    @KafkaListener(topics = MANUAL_DLT_TOPIC,groupId = "manual-dlt-dead-letter-group",containerFactory = "kafkaListenerContainerFactory")
    public void consumeManualDltTopic(KafkaDemoEvent event, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,@Header(KafkaHeaders.RECEIVED_PARTITION) int partition,@Header(KafkaHeaders.OFFSET) long offset,
            @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timestamp) {
        KafkaLabLogger.logPatternEvent(log,"DLT-MANUAL","consumer-service","manual-dlt-dead-letter-group",topic,event.eventId(),partition,offset,
                String.format("[MANUAL DLT] Received failed event. topic=%s partition=%d offset=%d timestamp=%d",topic,partition,offset,timestamp)
        );
    }

    @RetryableTopic(attempts = "4", backOff = @BackOff(delay = 1000L),dltStrategy = DltStrategy.FAIL_ON_ERROR,dltTopicSuffix = "-dlt")
    @KafkaListener(id = "annotation-dlt-consumer",topics = ANNOTATION_TOPIC,groupId = "annotation-dlt-consumer-group",containerFactory = "kafkaListenerContainerFactory")
    public void consumeAnnotationDlt(KafkaDemoEvent event) {
        KafkaLabLogger.logPatternEvent(log,"DLT-ANNOTATION","consumer-service","annotation-dlt-consumer-group",ANNOTATION_TOPIC,
                event.eventId(),0,0,String.format("Processing annotation event: %s - intentionally failing",event.payload())
        );

        throw new RuntimeException("Intentional failure for @RetryableTopic demo");
    }

    @DltHandler
    public void handleAnnotationDlt(KafkaDemoEvent event,@Header(KafkaHeaders.RECEIVED_TOPIC) String topic,@Header(KafkaHeaders.RECEIVED_PARTITION) int partition,@Header(KafkaHeaders.OFFSET) long offset,@Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timestamp) {
        KafkaLabLogger.logPatternEvent(log,"DLT-ANNOTATION","consumer-service","annotation-dlt-consumer-group",topic,
                event.eventId(),partition,offset,String.format("[ANNOTATION DLT] Received permanently failed event. topic=%s partition=%d offset=%d timestamp=%d",
                        topic,partition,offset,timestamp
                )
        );
    }
}