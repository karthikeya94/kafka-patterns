package com.example.kafkalab.common.dto;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Event Sourcing pattern DTOs.
 */
public final class EventSourcingDTOs {

    /**
     * Base event for event sourcing.
     */
    public sealed interface OrderEvent permits OrderCreated, PaymentReceived, OrderPacked, OrderShipped {
        String orderId();
        Instant timestamp();
    }

    public record OrderCreated(
        String orderId,
        String customerId,
        BigDecimal amount,
        String product,
        Instant timestamp
    ) implements OrderEvent {}

    public record PaymentReceived(
        String orderId,
        String paymentId,
        BigDecimal amount,
        Instant timestamp
    ) implements OrderEvent {}

    public record OrderPacked(
        String orderId,
        String warehouseId,
        Instant timestamp
    ) implements OrderEvent {}

    public record OrderShipped(
        String orderId,
        String trackingNumber,
        String carrier,
        Instant timestamp
    ) implements OrderEvent {}

    /**
     * Reconstructed order state from events.
     */
    public record OrderState(
        String orderId,
        String customerId,
        BigDecimal amount,
        String product,
        String status,
        String paymentId,
        String warehouseId,
        String trackingNumber,
        String carrier,
        Instant createdAt,
        Instant updatedAt
    ) {}

    private EventSourcingDTOs() {}
}