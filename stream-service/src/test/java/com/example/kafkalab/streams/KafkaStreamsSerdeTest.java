package com.example.kafkalab.streams;

import com.example.kafkalab.common.dto.KafkaDemoEvent;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;

class KafkaStreamsSerdeTest {

    @Test
    void jacksonThreeSerdeRoundTripsTheCoreEvent() {
        KafkaDemoEvent event = KafkaDemoEvent.builder()
            .eventId("event-1")
            .customerId("customer-1")
            .amount(BigDecimal.TEN)
            .payload("hello")
            .build();

        try (JacksonJsonSerde<KafkaDemoEvent> serde = new JacksonJsonSerde<>(KafkaDemoEvent.class)) {
            byte[] bytes = serde.serializer().serialize("test-topic", event);
            KafkaDemoEvent restored = serde.deserializer().deserialize("test-topic", bytes);
            assertEquals(event, restored);
        }
    }
}
