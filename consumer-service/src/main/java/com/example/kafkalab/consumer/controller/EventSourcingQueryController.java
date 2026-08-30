package com.example.kafkalab.consumer.controller;

import com.example.kafkalab.common.dto.EventSourcingDTOs;
import com.example.kafkalab.common.logging.KafkaLabLogger;
import com.example.kafkalab.consumer.kafka.eventsource.EventSourcingConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api/kafka/event-sourcing")
public class EventSourcingQueryController {

    private static final Logger log = LoggerFactory.getLogger(EventSourcingQueryController.class);
    private static final String PATTERN = "EVENT-SOURCING";

    private final EventSourcingConsumer eventSourcingConsumer;

    public EventSourcingQueryController(EventSourcingConsumer eventSourcingConsumer) {
        this.eventSourcingConsumer = eventSourcingConsumer;
    }

    @GetMapping("/orders/{orderId}")
    public Map<String, Object> getOrderState(@PathVariable(name = "orderId") String orderId) {
        EventSourcingDTOs.OrderState state = eventSourcingConsumer.getOrderState(orderId);

        if (state == null) {
            KafkaLabLogger.logInfo(log, PATTERN, "Order not found: " + orderId);
            return Map.of("error", "Order not found", "orderId", orderId);
        }

        KafkaLabLogger.logInfo(log, PATTERN, "Query order state: " + orderId + " -> " + state.status());
        return Map.ofEntries(
            Map.entry("orderId", state.orderId()),
            Map.entry("customerId", state.customerId()),
            Map.entry("amount", state.amount()),
            Map.entry("product", state.product()),
            Map.entry("status", state.status()),
            Map.entry("paymentId", state.paymentId()),
            Map.entry("warehouseId", state.warehouseId()),
            Map.entry("trackingNumber", state.trackingNumber()),
            Map.entry("carrier", state.carrier()),
            Map.entry("createdAt", state.createdAt()),
            Map.entry("updatedAt", state.updatedAt())
        );
    }
}
