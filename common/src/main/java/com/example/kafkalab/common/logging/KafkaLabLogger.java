package com.example.kafkalab.common.logging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Centralized logging format for all services.
 * Produces the consistent boxed output format specified in the requirements.
 */
public final class KafkaLabLogger {

    private static final String BORDER = "==========================================================";
    private static final String PREFIX = "[KAFKA LAB]";
    private static final String STREAMS_PREFIX = "[KAFKA STREAMS]";

    private KafkaLabLogger() {}

    /**
     * Log a standard Kafka consumer pattern event.
     */
    public static void logPatternEvent(Logger logger, String pattern, String service, String consumerGroup,
                                       String topic, String eventId, int partition, long offset, String detail) {
        logger.info("""

            {}
            {}
            Pattern      : {}
            Service      : {}
            ConsumerGroup: {}
            Topic        : {}
            EventId      : {}
            Partition    : {}
            Offset       : {}
            Detail       : {}
            {}""",
            BORDER, PREFIX, pattern, service, consumerGroup, topic, eventId, partition, offset, detail, BORDER);
    }

    /**
     * Log a Kafka Streams pattern event.
     */
    public static void logStreamsEvent(Logger logger, String pattern, String applicationId, String inputTopic,
                                       String key, String value, String detail) {
        logger.info("""

            {}
            {}
            Pattern       : {}
            ApplicationId : {}
            InputTopic    : {}
            Key           : {}
            Value         : {}
            Detail        : {}
            {}""",
            BORDER, STREAMS_PREFIX, pattern, applicationId, inputTopic, key, value, detail, BORDER);
    }

    /**
     * Log a simple message with the standard prefix.
     */
    public static void logInfo(Logger logger, String pattern, String message) {
        logger.info("{} {} {}", PREFIX, pattern, message);
    }

    /**
     * Log an error with the standard prefix.
     */
    public static void logError(Logger logger, String pattern, String message, Throwable throwable) {
        logger.error("{} {} {}", PREFIX, pattern, message, throwable);
    }

    /**
     * Log producer send event.
     */
    public static void logProducerSend(Logger logger, String pattern, String topic, String eventId, String key, int partition) {
        logger.info("""

            {}
            {}
            Pattern   : {}
            Action    : PRODUCE
            Topic     : {}
            EventId   : {}
            Key       : {}
            Partition : {}
            {}""",
            BORDER, PREFIX, pattern, topic, eventId, key, partition, BORDER);
    }
}