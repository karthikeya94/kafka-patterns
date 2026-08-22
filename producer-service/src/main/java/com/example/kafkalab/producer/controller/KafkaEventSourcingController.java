package com.example.kafkalab.producer.controller;

import com.example.kafkalab.common.dto.EventSourcingDTOs;
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
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@RestController
@RequestMapping("/api/kafka")
public class KafkaEventSourcingController {

    private static final Logger log = LoggerFactory.getLogger(KafkaEventSourcingController.class);
    private static final String PATTERN = "EVENT-SOURCING";

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public KafkaEventSourcingController(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping("/event-sourcing")
    public Map<String, Object> sendEventSourcingEvents(@RequestBody Map<String, Object> request) {
        String orderId = (String) request.getOrDefault("orderId", "ORD-" + System.currentTimeMillis());
        String customerId = (String) request.getOrDefault("customerId", "CUST-" + System.currentTimeMillis());
        BigDecimal amount = new BigDecimal(request.getOrDefault("amount", "999.99").toString());
        String product = (String) request.getOrDefault("product", "Premium Widget");

        // Send OrderCreated
        EventSourcingDTOs.OrderCreated created = new EventSourcingDTOs.OrderCreated(
            orderId, customerId, amount, product, Instant.now()
        );
        kafkaTemplate.send(KafkaTopics.EVENT_SOURCING, orderId, created);

        // Send PaymentReceived
        EventSourcingDTOs.PaymentReceived payment = new EventSourcingDTOs.PaymentReceived(
            orderId, "PAY-" + System.currentTimeMillis(), amount, Instant.now()
        );
        kafkaTemplate.send(KafkaTopics.EVENT_SOURCING, orderId, payment);

        // Send OrderPacked
        EventSourcingDTOs.OrderPacked packed = new EventSourcingDTOs.OrderPacked(
            orderId, "WH-" + System.currentTimeMillis(), Instant.now()
        );
        kafkaTemplate.send(KafkaTopics.EVENT_SOURCING, orderId, packed);

        // Send OrderShipped
        EventSourcingDTOs.OrderShipped shipped = new EventSourcingDTOs.OrderShipped(
            orderId, "TRK-" + System.currentTimeMillis(), "FastShip", Instant.now()
        );
        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(KafkaTopics.EVENT_SOURCING, orderId, shipped);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                KafkaLabLogger.logError(log, PATTERN, "Failed to send event sourcing events", ex);
            } else {
                KafkaLabLogger.logProducerSend(log, PATTERN, KafkaTopics.EVENT_SOURCING,
                    orderId, orderId, result.getRecordMetadata().partition());
            }
        });

        return Map.of(
            "orderId", orderId,
            "customerId", customerId,
            "topic", KafkaTopics.EVENT_SOURCING,
            "events", 4,
            "status", "sent"
        );
    }
}