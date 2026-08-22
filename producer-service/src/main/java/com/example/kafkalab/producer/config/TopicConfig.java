package com.example.kafkalab.producer.config;

import com.example.kafkalab.common.topic.KafkaTopics;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class TopicConfig {

    @Bean
    public NewTopic queueTopic() {
        return TopicBuilder.name(KafkaTopics.QUEUE)
            .partitions(KafkaTopics.PARTITIONS_DEMO)
            .replicas(KafkaTopics.REPLICATION_FACTOR)
            .build();
    }

    @Bean
    public NewTopic pubsubTopic() {
        return TopicBuilder.name(KafkaTopics.PUBSUB)
            .partitions(KafkaTopics.PARTITIONS_DEMO)
            .replicas(KafkaTopics.REPLICATION_FACTOR)
            .build();
    }

    @Bean
    public NewTopic pubsubLoadbalanceTopic() {
        return TopicBuilder.name(KafkaTopics.PUBSUB_LOADBALANCE)
            .partitions(KafkaTopics.PARTITIONS_DEMO)
            .replicas(KafkaTopics.REPLICATION_FACTOR)
            .build();
    }

    @Bean
    public NewTopic retryTopic() {
        return TopicBuilder.name(KafkaTopics.RETRY)
            .partitions(KafkaTopics.PARTITIONS_DEMO)
            .replicas(KafkaTopics.REPLICATION_FACTOR)
            .build();
    }

    @Bean
    public NewTopic retryBackoffTopic() {
        return TopicBuilder.name(KafkaTopics.RETRY_BACKOFF)
            .partitions(KafkaTopics.PARTITIONS_DEMO)
            .replicas(KafkaTopics.REPLICATION_FACTOR)
            .build();
    }

    @Bean
    public NewTopic retryBackoffDltTopic() {
        return TopicBuilder.name(KafkaTopics.RETRY_BACKOFF_DLT)
            .partitions(KafkaTopics.PARTITIONS_SINGLE)
            .replicas(KafkaTopics.REPLICATION_FACTOR)
            .build();
    }

    @Bean
    public NewTopic dltTopic() {
        return TopicBuilder.name(KafkaTopics.DLT)
            .partitions(KafkaTopics.PARTITIONS_DEMO)
            .replicas(KafkaTopics.REPLICATION_FACTOR)
            .build();
    }

    @Bean
    public NewTopic dltDltTopic() {
        return TopicBuilder.name(KafkaTopics.DLT_DLT)
            .partitions(KafkaTopics.PARTITIONS_SINGLE)
            .replicas(KafkaTopics.REPLICATION_FACTOR)
            .build();
    }

    @Bean
    public NewTopic retryDltTopic() {
        return TopicBuilder.name(KafkaTopics.RETRY_DLT)
            .partitions(KafkaTopics.PARTITIONS_DEMO)
            .replicas(KafkaTopics.REPLICATION_FACTOR)
            .build();
    }

    @Bean
    public NewTopic retryDltDltTopic() {
        return TopicBuilder.name(KafkaTopics.RETRY_DLT_DLT)
            .partitions(KafkaTopics.PARTITIONS_SINGLE)
            .replicas(KafkaTopics.REPLICATION_FACTOR)
            .build();
    }

    @Bean
    public NewTopic requestReplyRequestTopic() {
        return TopicBuilder.name(KafkaTopics.REQUEST_REPLY_REQUEST)
            .partitions(KafkaTopics.PARTITIONS_DEMO)
            .replicas(KafkaTopics.REPLICATION_FACTOR)
            .build();
    }

    @Bean
    public NewTopic requestReplyResponseTopic() {
        return TopicBuilder.name(KafkaTopics.REQUEST_REPLY_RESPONSE)
            .partitions(KafkaTopics.PARTITIONS_DEMO)
            .replicas(KafkaTopics.REPLICATION_FACTOR)
            .build();
    }

    @Bean
    public NewTopic streamsTransformInputTopic() {
        return TopicBuilder.name(KafkaTopics.STREAMS_TRANSFORM_INPUT)
            .partitions(KafkaTopics.PARTITIONS_DEMO)
            .replicas(KafkaTopics.REPLICATION_FACTOR)
            .build();
    }

    @Bean
    public NewTopic streamsTransformOutputTopic() {
        return TopicBuilder.name(KafkaTopics.STREAMS_TRANSFORM_OUTPUT)
            .partitions(KafkaTopics.PARTITIONS_DEMO)
            .replicas(KafkaTopics.REPLICATION_FACTOR)
            .build();
    }

    @Bean
    public NewTopic streamsFilterInputTopic() {
        return TopicBuilder.name(KafkaTopics.STREAMS_FILTER_INPUT)
            .partitions(KafkaTopics.PARTITIONS_DEMO)
            .replicas(KafkaTopics.REPLICATION_FACTOR)
            .build();
    }

    @Bean
    public NewTopic streamsFilterOutputTopic() {
        return TopicBuilder.name(KafkaTopics.STREAMS_FILTER_OUTPUT)
            .partitions(KafkaTopics.PARTITIONS_DEMO)
            .replicas(KafkaTopics.REPLICATION_FACTOR)
            .build();
    }

    @Bean
    public NewTopic streamsBranchInputTopic() {
        return TopicBuilder.name(KafkaTopics.STREAMS_BRANCH_INPUT)
            .partitions(KafkaTopics.PARTITIONS_DEMO)
            .replicas(KafkaTopics.REPLICATION_FACTOR)
            .build();
    }

    @Bean
    public NewTopic streamsBranchHighValueTopic() {
        return TopicBuilder.name(KafkaTopics.STREAMS_BRANCH_HIGH_VALUE)
            .partitions(KafkaTopics.PARTITIONS_SINGLE)
            .replicas(KafkaTopics.REPLICATION_FACTOR)
            .build();
    }

    @Bean
    public NewTopic streamsBranchNormalTopic() {
        return TopicBuilder.name(KafkaTopics.STREAMS_BRANCH_NORMAL)
            .partitions(KafkaTopics.PARTITIONS_SINGLE)
            .replicas(KafkaTopics.REPLICATION_FACTOR)
            .build();
    }

    @Bean
    public NewTopic streamsBranchSuspiciousTopic() {
        return TopicBuilder.name(KafkaTopics.STREAMS_BRANCH_SUSPICIOUS)
            .partitions(KafkaTopics.PARTITIONS_SINGLE)
            .replicas(KafkaTopics.REPLICATION_FACTOR)
            .build();
    }

    @Bean
    public NewTopic streamsAggregateInputTopic() {
        return TopicBuilder.name(KafkaTopics.STREAMS_AGGREGATE_INPUT)
            .partitions(KafkaTopics.PARTITIONS_DEMO)
            .replicas(KafkaTopics.REPLICATION_FACTOR)
            .build();
    }

    @Bean
    public NewTopic streamsAggregateOutputTopic() {
        return TopicBuilder.name(KafkaTopics.STREAMS_AGGREGATE_OUTPUT)
            .partitions(KafkaTopics.PARTITIONS_DEMO)
            .replicas(KafkaTopics.REPLICATION_FACTOR)
            .build();
    }

    @Bean
    public NewTopic streamsKTableInputTopic() {
        return TopicBuilder.name(KafkaTopics.STREAMS_KTABLE_INPUT)
            .partitions(KafkaTopics.PARTITIONS_DEMO)
            .replicas(KafkaTopics.REPLICATION_FACTOR)
            .build();
    }

    @Bean
    public NewTopic streamsKTableOutputTopic() {
        return TopicBuilder.name(KafkaTopics.STREAMS_KTABLE_OUTPUT)
            .partitions(KafkaTopics.PARTITIONS_DEMO)
            .replicas(KafkaTopics.REPLICATION_FACTOR)
            .build();
    }

    @Bean
    public NewTopic streamsJoinOrdersTopic() {
        return TopicBuilder.name(KafkaTopics.STREAMS_JOIN_ORDERS)
            .partitions(KafkaTopics.PARTITIONS_DEMO)
            .replicas(KafkaTopics.REPLICATION_FACTOR)
            .build();
    }

    @Bean
    public NewTopic streamsJoinCustomersTopic() {
        return TopicBuilder.name(KafkaTopics.STREAMS_JOIN_CUSTOMERS)
            .partitions(KafkaTopics.PARTITIONS_DEMO)
            .replicas(KafkaTopics.REPLICATION_FACTOR)
            .build();
    }

    @Bean
    public NewTopic streamsJoinOutputTopic() {
        return TopicBuilder.name(KafkaTopics.STREAMS_JOIN_OUTPUT)
            .partitions(KafkaTopics.PARTITIONS_DEMO)
            .replicas(KafkaTopics.REPLICATION_FACTOR)
            .build();
    }

    @Bean
    public NewTopic streamsWindowInputTopic() {
        return TopicBuilder.name(KafkaTopics.STREAMS_WINDOW_INPUT)
            .partitions(KafkaTopics.PARTITIONS_DEMO)
            .replicas(KafkaTopics.REPLICATION_FACTOR)
            .build();
    }

    @Bean
    public NewTopic streamsWindowOutputTopic() {
        return TopicBuilder.name(KafkaTopics.STREAMS_WINDOW_OUTPUT)
            .partitions(KafkaTopics.PARTITIONS_DEMO)
            .replicas(KafkaTopics.REPLICATION_FACTOR)
            .build();
    }

    @Bean
    public NewTopic streamsStateInputTopic() {
        return TopicBuilder.name(KafkaTopics.STREAMS_STATE_INPUT)
            .partitions(KafkaTopics.PARTITIONS_DEMO)
            .replicas(KafkaTopics.REPLICATION_FACTOR)
            .build();
    }

    @Bean
    public NewTopic streamsStateOutputTopic() {
        return TopicBuilder.name(KafkaTopics.STREAMS_STATE_OUTPUT)
            .partitions(KafkaTopics.PARTITIONS_DEMO)
            .replicas(KafkaTopics.REPLICATION_FACTOR)
            .build();
    }

    @Bean
    public NewTopic eventReplayTopic() {
        return TopicBuilder.name(KafkaTopics.EVENT_REPLAY)
            .partitions(KafkaTopics.PARTITIONS_DEMO)
            .replicas(KafkaTopics.REPLICATION_FACTOR)
            .build();
    }

    @Bean
    public NewTopic eventSourcingTopic() {
        return TopicBuilder.name(KafkaTopics.EVENT_SOURCING)
            .partitions(KafkaTopics.PARTITIONS_DEMO)
            .replicas(KafkaTopics.REPLICATION_FACTOR)
            .build();
    }

    @Bean
    public NewTopic streamsErrorInputTopic() {
        return TopicBuilder.name(KafkaTopics.STREAMS_ERROR_INPUT)
            .partitions(KafkaTopics.PARTITIONS_DEMO)
            .replicas(KafkaTopics.REPLICATION_FACTOR)
            .build();
    }

    @Bean
    public NewTopic streamsErrorOutputTopic() {
        return TopicBuilder.name(KafkaTopics.STREAMS_ERROR_OUTPUT)
            .partitions(KafkaTopics.PARTITIONS_DEMO)
            .replicas(KafkaTopics.REPLICATION_FACTOR)
            .build();
    }

    @Bean
    public NewTopic streamsErrorDltTopic() {
        return TopicBuilder.name(KafkaTopics.STREAMS_ERROR_DLT)
            .partitions(KafkaTopics.PARTITIONS_SINGLE)
            .replicas(KafkaTopics.REPLICATION_FACTOR)
            .build();
    }
}
