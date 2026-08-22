package com.example.kafkalab.producer.controller;

import com.example.kafkalab.common.dto.RequestReplyDTOs;
import com.example.kafkalab.common.logging.KafkaLabLogger;
import com.example.kafkalab.common.topic.KafkaTopics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.RequestReplyMessageFuture;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@RestController
@RequestMapping("/api/kafka")
public class KafkaRequestReplyController {

    private static final Logger log = LoggerFactory.getLogger(KafkaRequestReplyController.class);
    private static final String PATTERN = "REQUEST/REPLY";

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ReplyingKafkaTemplate<String, Object, Object> replyingKafkaTemplate;

    public KafkaRequestReplyController(KafkaTemplate<String, Object> kafkaTemplate,
                                        ReplyingKafkaTemplate<String, Object, Object> replyingKafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
        this.replyingKafkaTemplate = replyingKafkaTemplate;
    }

    @PostMapping("/request-reply")
    public Map<String, Object> sendRequestReply(@RequestBody Map<String, String> request) {
        String operation = request.getOrDefault("operation", "echo");
        String payload = request.getOrDefault("payload", "Request-Reply test message");

        RequestReplyDTOs.Request req = new RequestReplyDTOs.Request(
            null, null, operation, payload, Instant.now()
        );

        // Send request and wait for reply
        RequestReplyMessageFuture<String, Object> future = replyingKafkaTemplate.sendAndReceive(
            MessageBuilder.withPayload(req)
                .setHeader(KafkaHeaders.TOPIC, KafkaTopics.REQUEST_REPLY_REQUEST)
                .setHeader(KafkaHeaders.KEY, req.correlationId())
                .build());

        try {
            SendResult<String, Object> sendResult = future.getSendFuture().get(10, TimeUnit.SECONDS);
            KafkaLabLogger.logProducerSend(log, PATTERN, KafkaTopics.REQUEST_REPLY_REQUEST,
                req.requestId(), req.correlationId(), sendResult.getRecordMetadata().partition());

            // Wait for response
            var responseFuture = future.get(30, TimeUnit.SECONDS);
            RequestReplyDTOs.Response response = (RequestReplyDTOs.Response) responseFuture.getPayload();

            return Map.of(
                "requestId", req.requestId(),
                "correlationId", req.correlationId(),
                "responseId", response.responseId(),
                "success", response.success(),
                "result", response.result(),
                "errorMessage", response.errorMessage(),
                "topic", KafkaTopics.REQUEST_REPLY_RESPONSE,
                "status", "completed"
            );
        } catch (Exception e) {
            KafkaLabLogger.logError(log, PATTERN, "Request/Reply failed", e);
            return Map.of(
                "requestId", req.requestId(),
                "correlationId", req.correlationId(),
                "status", "failed",
                "error", e.getMessage()
            );
        }
    }
}
