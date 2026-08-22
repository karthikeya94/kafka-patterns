package com.example.kafkalab.streams.topology.window;

import com.example.kafkalab.common.dto.KafkaDemoEvent;
import com.example.kafkalab.common.dto.StreamsDTOs;
import com.example.kafkalab.common.logging.KafkaLabLogger;
import com.example.kafkalab.common.topic.KafkaTopics;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;

@Configuration
public class WindowTopology {

    private static final Logger log = LoggerFactory.getLogger(WindowTopology.class);
    private static final String PATTERN = "WINDOW";
    private static final String APP_ID = "streams-window-demo";
    private static final String STATE_STORE = "window-counts";

    // 10-second tumbling window for educational purposes
    private static final Duration WINDOW_SIZE = Duration.ofSeconds(10);

    @Bean
    public KStream<String, KafkaDemoEvent> windowStream(
            StreamsBuilder builder,
            JacksonJsonSerde<KafkaDemoEvent> eventSerde,
            JacksonJsonSerde<StreamsDTOs.WindowResult> windowSerde) {

        KStream<String, KafkaDemoEvent> input = builder.stream(
            KafkaTopics.STREAMS_WINDOW_INPUT, Consumed.with(Serdes.String(), eventSerde));

        // Tumbling window aggregation
        KTable<Windowed<String>, StreamsDTOs.WindowResult> windowed = input
            .groupBy((key, event) -> event.customerId())
            .windowedBy(TimeWindows.ofSizeWithNoGrace(WINDOW_SIZE))
            .aggregate(
                () -> new StreamsDTOs.WindowResult(null, 0, BigDecimal.ZERO, null, null),
                (customerId, event, agg) -> {
                    long newCount = agg.count() + 1;
                    BigDecimal newSum = agg.sum().add(event.amount());
                    Instant windowStart = Instant.ofEpochMilli(
                        (event.timestamp().toEpochMilli() / WINDOW_SIZE.toMillis()) * WINDOW_SIZE.toMillis()
                    );
                    Instant windowEnd = windowStart.plusMillis(WINDOW_SIZE.toMillis());

                    StreamsDTOs.WindowResult updated = new StreamsDTOs.WindowResult(
                        customerId, newCount, newSum, windowStart, windowEnd
                    );

                    KafkaLabLogger.logStreamsEvent(log, PATTERN, APP_ID, KafkaTopics.STREAMS_WINDOW_INPUT,
                        customerId, event.amount().toString(),
                        String.format("Window agg: count=%d sum=%s window=[%s - %s]",
                            newCount, newSum, windowStart, windowEnd));

                    return updated;
                },
                Materialized.<String, StreamsDTOs.WindowResult, WindowStore<org.apache.kafka.common.utils.Bytes, byte[]>>as(STATE_STORE)
                    .withKeySerde(Serdes.String())
                    .withValueSerde(windowSerde)
            );

        // Output window results
        windowed.toStream().peek((windowedKey, result) ->
            KafkaLabLogger.logStreamsEvent(log, PATTERN, APP_ID, KafkaTopics.STREAMS_WINDOW_INPUT,
                windowedKey.key(), result.toString(),
                String.format("Window result: key=%s count=%d sum=%s window=[%s - %s]",
                    windowedKey.key(), result.count(), result.sum(), result.windowStart(), result.windowEnd()))
        ).map((windowedKey, result) -> KeyValue.pair(windowedKey.key(), result))
            .to(KafkaTopics.STREAMS_WINDOW_OUTPUT, Produced.with(Serdes.String(), windowSerde));

        return input;
    }
}
