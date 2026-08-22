package com.example.kafkalab.common.topic;

/**
 * Centralized topic names for all communication patterns.
 * Each pattern uses dedicated, isolated topics.
 * Topic naming convention: {pattern}-demo[-suffix]
 */
public final class KafkaTopics {

    // Pattern 1: Queue / Competing Consumers
    public static final String QUEUE = "queue-demo";

    // Pattern 2: Pub/Sub
    public static final String PUBSUB = "pubsub-demo";

    // Pattern 3: Pub/Sub + Competing Consumers (Load Balanced)
    public static final String PUBSUB_LOADBALANCE = "pubsub-loadbalance-demo";

    // Pattern 4: Retry
    public static final String RETRY = "retry-demo";

    // Pattern 5: Delayed / Backoff Retry
    public static final String RETRY_BACKOFF = "retry-backoff-demo";
    public static final String RETRY_BACKOFF_DLT = "retry-backoff-demo-dlt";

    // Pattern 6: Dead Letter Topic
    public static final String DLT = "dlt-demo";
    public static final String DLT_DLT = "dlt-demo-dlt";

    // Pattern 7: Retry + DLT
    public static final String RETRY_DLT = "retry-dlt-demo";
    public static final String RETRY_DLT_DLT = "retry-dlt-demo-dlt";

    // Pattern 8: Request / Reply
    public static final String REQUEST_REPLY_REQUEST = "request-reply-demo-request";
    public static final String REQUEST_REPLY_RESPONSE = "request-reply-demo-response";

    // Pattern 9: Streams Transform
    public static final String STREAMS_TRANSFORM_INPUT = "streams-transform-demo-input";
    public static final String STREAMS_TRANSFORM_OUTPUT = "streams-transform-demo-output";

    // Pattern 10: Streams Filter
    public static final String STREAMS_FILTER_INPUT = "streams-filter-demo-input";
    public static final String STREAMS_FILTER_OUTPUT = "streams-filter-demo-output";

    // Pattern 11: Streams Branch
    public static final String STREAMS_BRANCH_INPUT = "streams-branch-demo-input";
    public static final String STREAMS_BRANCH_HIGH_VALUE = "streams-branch-demo-high-value";
    public static final String STREAMS_BRANCH_NORMAL = "streams-branch-demo-normal";
    public static final String STREAMS_BRANCH_SUSPICIOUS = "streams-branch-demo-suspicious";

    // Pattern 12: Streams Aggregation
    public static final String STREAMS_AGGREGATE_INPUT = "streams-aggregate-demo-input";
    public static final String STREAMS_AGGREGATE_OUTPUT = "streams-aggregate-demo-output";

    // Pattern 13: Streams KTable
    public static final String STREAMS_KTABLE_INPUT = "streams-ktable-demo-input";
    public static final String STREAMS_KTABLE_OUTPUT = "streams-ktable-demo-output";

    // Pattern 14: Streams Join
    public static final String STREAMS_JOIN_ORDERS = "streams-join-orders";
    public static final String STREAMS_JOIN_CUSTOMERS = "streams-join-customers";
    public static final String STREAMS_JOIN_OUTPUT = "streams-join-output";

    // Pattern 15: Streams Windowing
    public static final String STREAMS_WINDOW_INPUT = "streams-window-demo-input";
    public static final String STREAMS_WINDOW_OUTPUT = "streams-window-demo-output";

    // Pattern 16: Streams State Store
    public static final String STREAMS_STATE_INPUT = "streams-state-demo-input";
    public static final String STREAMS_STATE_OUTPUT = "streams-state-demo-output";

    // Pattern 17: Event Replay
    public static final String EVENT_REPLAY = "event-replay-demo";

    // Pattern 18: Event Sourcing
    public static final String EVENT_SOURCING = "event-sourcing-demo";

    // Pattern 19: Streams Error Handling / DLT
    public static final String STREAMS_ERROR_INPUT = "streams-error-demo-input";
    public static final String STREAMS_ERROR_OUTPUT = "streams-error-demo-output";
    public static final String STREAMS_ERROR_DLT = "streams-error-demo-dlt";

    // Partition counts for learning demonstrations
    public static final int PARTITIONS_DEMO = 3;
    public static final int PARTITIONS_SINGLE = 1;
    public static final short REPLICATION_FACTOR = 1;

    private KafkaTopics() {
        // Utility class
    }
}
