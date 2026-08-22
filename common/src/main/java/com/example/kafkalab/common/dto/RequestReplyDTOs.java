package com.example.kafkalab.common.dto;

import java.time.Instant;

/**
 * DTOs for Request/Reply pattern.
 */
public final class RequestReplyDTOs {

    /**
     * Request sent to the request topic.
     */
    public record Request(
        String requestId,
        String correlationId,
        String operation,
        String payload,
        Instant timestamp
    ) {
        public Request {
            if (requestId == null) requestId = java.util.UUID.randomUUID().toString();
            if (correlationId == null) correlationId = requestId;
            if (timestamp == null) timestamp = Instant.now();
        }
    }

    /**
     * Response sent to the response topic.
     */
    public record Response(
        String responseId,
        String correlationId,
        String requestId,
        boolean success,
        String result,
        String errorMessage,
        Instant timestamp
    ) {
        public Response {
            if (responseId == null) responseId = java.util.UUID.randomUUID().toString();
            if (timestamp == null) timestamp = Instant.now();
        }
    }

    private RequestReplyDTOs() {}
}