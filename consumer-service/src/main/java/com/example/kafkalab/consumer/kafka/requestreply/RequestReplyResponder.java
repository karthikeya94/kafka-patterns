package com.example.kafkalab.consumer.kafka.requestreply;

import com.example.kafkalab.common.dto.KafkaDemoEvent;
import com.example.kafkalab.common.dto.RequestReplyDTOs;
import com.example.kafkalab.common.logging.KafkaLabLogger;
import com.example.kafkalab.common.topic.KafkaTopics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class RequestReplyResponder {

    private static final Logger log = LoggerFactory.getLogger(RequestReplyResponder.class);
    private static final String PATTERN = "REQUEST/REPLY";
    private static final String GROUP = "request-reply-demo-group";

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public RequestReplyResponder(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(
        topics = KafkaTopics.REQUEST_REPLY_REQUEST,
        groupId = GROUP,
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void handleRequest(RequestReplyDTOs.Request request) {
        KafkaLabLogger.logPatternEvent(log, PATTERN, "consumer-service", GROUP,
            KafkaTopics.REQUEST_REPLY_REQUEST, request.requestId(), 0, 0,
            String.format("Received request: operation=%s payload=%s", request.operation(), request.payload()));

        // Process the request
        String result;
        boolean success = true;
        String errorMessage = null;

        try {
            switch (request.operation().toLowerCase()) {
                case "echo" -> result = "Echo: " + request.payload();
                case "uppercase" -> result = request.payload().toUpperCase();
                case "lowercase" -> result = request.payload().toLowerCase();
                case "reverse" -> result = new StringBuilder(request.payload()).reverse().toString();
                default -> {
                    result = "Unknown operation: " + request.operation();
                    success = false;
                    errorMessage = "Unsupported operation";
                }
            }
        } catch (Exception e) {
            result = "Error processing request";
            success = false;
            errorMessage = e.getMessage();
        }

        // Send response
        RequestReplyDTOs.Response response = new RequestReplyDTOs.Response(
            null, request.correlationId(), request.requestId(),
            success, result, errorMessage, java.time.Instant.now()
        );

        kafkaTemplate.send(KafkaTopics.REQUEST_REPLY_RESPONSE, request.correlationId(), response);

        KafkaLabLogger.logPatternEvent(log, PATTERN, "consumer-service", GROUP,
            KafkaTopics.REQUEST_REPLY_RESPONSE, request.requestId(), 0, 0,
            String.format("Sent response: success=%s result=%s", success, result));
    }
}