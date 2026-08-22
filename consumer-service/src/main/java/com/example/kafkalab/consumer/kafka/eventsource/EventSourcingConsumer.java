package com.example.kafkalab.consumer.kafka.eventsource;

import com.example.kafkalab.common.dto.EventSourcingDTOs;
import com.example.kafkalab.common.logging.KafkaLabLogger;
import com.example.kafkalab.common.topic.KafkaTopics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class EventSourcingConsumer {

    private static final Logger log = LoggerFactory.getLogger(EventSourcingConsumer.class);
    private static final String PATTERN = "EVENT-SOURCING";
    private static final String GROUP = "event-sourcing-demo-group";

    // In-memory state store for reconstructed orders
    private final Map<String, EventSourcingDTOs.OrderState> orderStates = new ConcurrentHashMap<>();

    @KafkaListener(
        topics = KafkaTopics.EVENT_SOURCING,
        groupId = GROUP,
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(EventSourcingDTOs.OrderEvent event, Acknowledgment ack) {
        String orderId = event.orderId();

        // Get or create order state
        EventSourcingDTOs.OrderState state = orderStates.computeIfAbsent(orderId, id ->
            new EventSourcingDTOs.OrderState(
                id, null, null, null, "CREATED", null, null, null, null,
                Instant.now(), Instant.now()
            )
        );

        // Apply event to reconstruct state
        EventSourcingDTOs.OrderState updatedState;
        if (event instanceof EventSourcingDTOs.OrderCreated created) {
            updatedState = new EventSourcingDTOs.OrderState(
                created.orderId(), created.customerId(), created.amount(), created.product(),
                "CREATED", null, null, null, null,
                created.timestamp(), created.timestamp()
            );
        } else if (event instanceof EventSourcingDTOs.PaymentReceived payment) {
            updatedState = new EventSourcingDTOs.OrderState(
                state.orderId(), state.customerId(), state.amount(), state.product(),
                "PAID", payment.paymentId(), state.warehouseId(), state.trackingNumber(), state.carrier(),
                state.createdAt(), payment.timestamp()
            );
        } else if (event instanceof EventSourcingDTOs.OrderPacked packed) {
            updatedState = new EventSourcingDTOs.OrderState(
                state.orderId(), state.customerId(), state.amount(), state.product(),
                "PACKED", state.paymentId(), packed.warehouseId(), state.trackingNumber(), state.carrier(),
                state.createdAt(), packed.timestamp()
            );
        } else if (event instanceof EventSourcingDTOs.OrderShipped shipped) {
            updatedState = new EventSourcingDTOs.OrderState(
                state.orderId(), state.customerId(), state.amount(), state.product(),
                "SHIPPED", state.paymentId(), state.warehouseId(), shipped.trackingNumber(), shipped.carrier(),
                state.createdAt(), shipped.timestamp()
            );
        } else {
            updatedState = state;
        }

        orderStates.put(orderId, updatedState);

        KafkaLabLogger.logPatternEvent(log, PATTERN, "consumer-service", GROUP,
            KafkaTopics.EVENT_SOURCING, orderId, 0, 0,
            String.format("Event applied: %s -> State: %s", event.getClass().getSimpleName(), updatedState.status()));

        ack.acknowledge();
    }

    // Query endpoint would be in a controller
    public EventSourcingDTOs.OrderState getOrderState(String orderId) {
        return orderStates.get(orderId);
    }
}