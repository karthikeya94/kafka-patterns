package com.example.kafkalab.consumer.controller;

import com.example.kafkalab.consumer.kafka.replay.ReplayConsumer;

import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.Map;

@RestController
@RequestMapping("/api/replay")
public class ReplayController {

    private final ReplayConsumer replayConsumer;

    public ReplayController(ReplayConsumer replayConsumer) {
        this.replayConsumer = replayConsumer;
    }

    @PostMapping("/beginning")
    public Map<String, Object> replayFromBeginning() {

        replayConsumer.replayFromBeginning();

        return Map.of(
                "operation", "REPLAY_FROM_BEGINNING",
                "status", "requested"
        );
    }

    @PostMapping("/timestamp")
    public Map<String, Object> replayFromTimestamp(
            @RequestParam long timestamp) {

        replayConsumer.replayFromTimestamp(timestamp);

        return Map.of(
                "operation", "REPLAY_FROM_TIMESTAMP",
                "timestamp", timestamp,
                "timestampReadable",
                Instant.ofEpochMilli(timestamp).toString(),
                "status", "requested"
        );
    }
}