package com.example.kafkalab.producer.controller;

import com.example.kafkalab.common.dto.KafkaDemoEvent;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@RestController
@RequestMapping("/api/kafka/dlt")
public class KafkaDltController {

    private static final String MANUAL_TOPIC = "manual-dlt-demo";
    private static final String ANNOTATION_TOPIC = "annotation-dlt-demo";

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public KafkaDltController(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping("/manual")
    public ResponseEntity<Map<String, Object>> sendManualDltMessage(@RequestBody(required = false) Map<String, Object> request) {

        String message = getValue(request, "message", "Manual DLT test message - will fail");

        String customerId = getValue(request, "customerId", "CUST-" + System.currentTimeMillis());

        KafkaDemoEvent event = buildEvent(message, customerId);

        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(MANUAL_TOPIC, customerId, event);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                System.err.println("Failed to send manual DLT message. eventId=" + event.eventId());
            } else {
                System.out.println("Manual DLT message sent. topic=" + result.getRecordMetadata().topic() + ", partition=" + result.getRecordMetadata().partition() + ", offset=" + result.getRecordMetadata().offset() + ", eventId=" + event.eventId());
            }
        });

        return ResponseEntity.ok(Map.of("pattern", "MANUAL_DLT", "topic", MANUAL_TOPIC, "eventId", event.eventId(), "customerId", customerId, "status", "sent"));
    }

    @PostMapping("/annotation")
    public ResponseEntity<Map<String, Object>> sendAnnotationDltMessage(@RequestBody(required = false) Map<String, Object> request) {

        String message = getValue(request, "message", "Annotation DLT test message - will fail");

        String customerId = getValue(request, "customerId", "CUST-" + System.currentTimeMillis());

        KafkaDemoEvent event = buildEvent(message, customerId);

        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(ANNOTATION_TOPIC, customerId, event);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                System.err.println("Failed to send annotation DLT message. eventId=" + event.eventId());
            } else {
                System.out.println("Annotation DLT message sent. topic=" + result.getRecordMetadata().topic() + ", partition=" + result.getRecordMetadata().partition() + ", offset=" + result.getRecordMetadata().offset() + ", eventId=" + event.eventId());
            }
        });

        return ResponseEntity.ok(Map.of("pattern", "ANNOTATION_RETRYABLE_TOPIC", "topic", ANNOTATION_TOPIC, "eventId", event.eventId(), "customerId", customerId, "status", "sent"));
    }

    private KafkaDemoEvent buildEvent(String message, String customerId) {

        return KafkaDemoEvent.builder().eventType("DLT_DEMO").customerId(customerId).orderId("ORD-" + System.currentTimeMillis()).amount(new BigDecimal("300.00")).payload(message).metadata(Map.of("source", "producer-service", "pattern", "dlt", "createdAt", Instant.now().toString())).build();
    }

    private String getValue(Map<String, Object> request, String key, String defaultValue) {

        if (request == null) {
            return defaultValue;
        }

        Object value = request.get(key);

        return value == null ? defaultValue : value.toString();
    }
}