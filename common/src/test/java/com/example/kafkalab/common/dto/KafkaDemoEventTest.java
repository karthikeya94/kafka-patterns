package com.example.kafkalab.common.dto;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class KafkaDemoEventTest {

    @Test
    void suppliesEventMetadataDefaults() {
        KafkaDemoEvent event = new KafkaDemoEvent(null, null, "TEST", "customer-1", null,
            null, "payload", null, null);

        assertNotNull(event.eventId());
        assertEquals(event.eventId(), event.correlationId());
        assertNotNull(event.timestamp());
    }
}
