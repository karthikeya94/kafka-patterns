# Kafka Communication Patterns Lab

An educational Java 25 / Spring Boot 4.1 Kafka lab. `producer-service` exposes REST triggers, `consumer-service` demonstrates listener patterns, and `stream-service` runs a single Kafka Streams topology with isolated input and output topics.

## Start and stop

```bash
cd kafka-communication-lab
docker compose up -d
./mvnw clean verify
./mvnw -pl producer-service spring-boot:run
./mvnw -pl consumer-service spring-boot:run
./mvnw -pl stream-service spring-boot:run
```

Use a separate terminal for each service. Stop the broker with `docker compose down`; add `--volumes` only when you intentionally want to discard Kafka data. Kafka runs in single-node KRaft mode (no ZooKeeper). `KAFKA_BOOTSTRAP_SERVERS` overrides the local default of `localhost:9092`.

Ports: producer `8081`, consumer `8082`, streams `8083`, optional Kafka UI `8080` (`docker compose --profile ui up -d`). Actuator health is at `/actuator/health` on every application.

In IntelliJ IDEA, open `kafka-communication-lab/pom.xml` as a Maven project, select JDK 25, and run `ProducerServiceApplication`, `ConsumerServiceApplication`, and `StreamServiceApplication` in three run configurations. Start Kafka first.

## Architecture

```text
REST client -> producer-service -> Kafka topics -> consumer-service
                                          \-> stream-service -> output topics/state stores
```

Messages are JSON and are keyed by `customerId` where ordering matters. A key consistently selects a partition, so order is preserved for one key within one partition—not across the whole topic. Queue and pub/sub topics have three partitions so consumer assignment is observable.

## Pattern matrix

| Pattern | Dedicated topic(s) | Consumer group/application |
|---|---|---|
| Queue | `queue-demo` | `queue-demo-group` |
| Pub/sub | `pubsub-demo` | notification, audit, analytics groups |
| Pub/sub + load balance | `pubsub-loadbalance-demo` | inventory, payment, notification groups |
| Retry | `retry-demo` | retry-demo-group |
| Backoff retry | `retry-backoff-demo`, `retry-backoff-demo-dlt` | retry-backoff-demo-group |
| DLT | `dlt-demo`, `dlt-demo-dlt` | dlt-demo-group |
| Retry + DLT | `retry-dlt-demo`, `retry-dlt-demo-dlt` | retry-dlt-demo-group |
| Request/reply | `request-reply-demo-request`, `request-reply-demo-response` | request-reply-demo-group |
| Transform | `streams-transform-demo-input/output` | `kafka-lab-streams` |
| Filter | `streams-filter-demo-input/output` | `kafka-lab-streams` |
| Branch | `streams-branch-demo-input/high-value/normal/suspicious` | `kafka-lab-streams` |
| Aggregate | `streams-aggregate-demo-input/output` | `kafka-lab-streams` |
| KTable | `streams-ktable-demo-input/output` | `kafka-lab-streams` |
| Join | `streams-join-orders/customers/output` | `kafka-lab-streams` |
| Window | `streams-window-demo-input/output` | `kafka-lab-streams` |
| State store | `streams-state-demo-input/output` | `kafka-lab-streams` |
| Replay | `event-replay-demo` | replay-demo-group |
| Event sourcing | `event-sourcing-demo` | event-sourcing-demo-group |
| Streams error/DLT | `streams-error-demo-input/output/dlt` | `kafka-lab-streams` |

All names live in `common/.../KafkaTopics.java`; `TopicConfig` creates them explicitly with replication factor 1.
See [the pattern reference](docs/kafka-patterns.md) for the consumer, Kafka Streams, partitioning, and replay explanations.

## REST demonstrations

All requests go to `http://localhost:8081` and accept JSON.

```bash
curl -X POST localhost:8081/api/kafka/queue -H 'Content-Type: application/json' -d '{"message":"Queue test"}'
curl -X POST localhost:8081/api/kafka/pubsub -H 'Content-Type: application/json' -d '{"message":"Hello pubsub"}'
curl -X POST localhost:8081/api/kafka/pubsub-loadbalance -H 'Content-Type: application/json' -d '{"message":"Load balance"}'
curl -X POST localhost:8081/api/kafka/retry -H 'Content-Type: application/json' -d '{"message":"FAIL-ONCE","failAttempts":1}'
curl -X POST localhost:8081/api/kafka/retry-backoff -H 'Content-Type: application/json' -d '{"message":"FAIL"}'
curl -X POST localhost:8081/api/kafka/dlt -H 'Content-Type: application/json' -d '{"message":"FAIL"}'
curl -X POST localhost:8081/api/kafka/retry-dlt -H 'Content-Type: application/json' -d '{"message":"FAIL","maxAttempts":3}'
curl -X POST localhost:8081/api/kafka/request-reply -H 'Content-Type: application/json' -d '{"operation":"echo","payload":"Hello"}'
curl -X POST localhost:8081/api/kafka/streams/transform -H 'Content-Type: application/json' -d '{"text":"hello kafka"}'
curl -X POST localhost:8081/api/kafka/streams/filter -H 'Content-Type: application/json' -d '{"customerId":"C1","amount":1500}'
curl -X POST localhost:8081/api/kafka/streams/branch -H 'Content-Type: application/json' -d '{"customerId":"C1","amount":10000}'
curl -X POST localhost:8081/api/kafka/streams/aggregate -H 'Content-Type: application/json' -d '{"customerId":"C1","amount":100}'
curl -X POST localhost:8081/api/kafka/streams/ktable -H 'Content-Type: application/json' -d '{"customerId":"C1","tier":"PLATINUM"}'
curl -X POST localhost:8081/api/kafka/streams/join -H 'Content-Type: application/json' -d '{"customerId":"C1","customerName":"John","amount":500}'
curl -X POST localhost:8081/api/kafka/streams/window -H 'Content-Type: application/json' -d '{"customerId":"C1","amount":100}'
curl -X POST localhost:8081/api/kafka/streams/state -H 'Content-Type: application/json' -d '{"customerId":"C1","amount":100}'
curl -X POST localhost:8081/api/kafka/streams/error -H 'Content-Type: application/json' -d '{"payload":"FAIL-STREAMS"}'
curl -X POST localhost:8081/api/kafka/replay -H 'Content-Type: application/json' -d '{"message":"Replay me"}'
curl -X POST localhost:8081/api/kafka/event-sourcing -H 'Content-Type: application/json' -d '{"orderId":"O1"}'
```

Query materialized state at `GET http://localhost:8083/api/kafka/streams/aggregate/C1`, `/ktable/C1`, or `/state/C1`. Reconstructed event-sourcing state is at `GET http://localhost:8082/api/kafka/event-sourcing/orders/O1`.

## What to watch in logs

The services use boxed `[KAFKA LAB]` and `[KAFKA STREAMS]` log records. Queue listeners show only one consumer in `queue-demo-group` handling each partition record. Pub/sub listeners show all three independent groups receiving it. Retry logs show attempts; after exhausted attempts, DLT listeners show the original topic, partition, offset, timestamp, and exception headers.

`/streams/transform` logs `input=hello kafka output=HELLO KAFKA`; filter only emits values greater than 1000. Branching sends negatives to suspicious, values at least 10000 to high-value, and other non-negative values to normal. Aggregation and state-store queries are backed by local RocksDB state and Kafka changelog topics. The join is a KStream–KTable join: the order stream is joined to the latest customer record.

The Streams error example is intentionally different from listener DLT handling: `RecoveringProcessingExceptionHandler` handles a Kafka Streams processor exception, copies the failed raw record to `streams-error-demo-dlt` with DLT headers, and lets the topology continue.

## Replay and concepts

Kafka is a retained log: `earliest` consumes retained history, `latest` starts at new records, and a consumer group can be reset to a chosen offset. For a deliberate replay, stop the consumer and use `kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group replay-demo-group --topic event-replay-demo --reset-offsets --to-earliest --execute`; then restart it. Do not delete offsets automatically from the app.

A DLT is a Dead Letter Topic (often casually called a DLQ). Retry is consumer processing policy, not a Kafka queue feature. Request/reply is intentionally synchronous only for the lab; normal HTTP or gRPC is usually a better fit when a service genuinely needs a synchronous response.

Spring Kafka provides several AckMode options inside ContainerProperties. They control exactly when and how often the consumer sends offset commits back to the Kafka broker.
------------------------------
## The 7 Available AckModes
Here is the complete list of available AckMode settings:

| AckMode | Behavior |
|---|---|
| BATCH (Default) | Commits all offsets from the current batch after all records returned by the poll are processed. |
| RECORD | Commits the offset automatically immediately after the individual listener method returns. |
| TIME | Commits container offsets only after a user-defined time interval has passed. |
| COUNT | Commits container offsets only after a user-defined number of records have been processed. |
| COUNT_TIME | Commits offsets whenever either the TIME or COUNT threshold is hit first. |
| MANUAL | Standard manual mode. Acknowledges are queued in memory and committed on the next poll batch. |
| MANUAL_IMMEDIATE | Strict manual mode. Commits the offset to Kafka synchronously and instantly on the listener thread. |

------------------------------
## When to Use Which One## 1. Use BATCH (The Default)

* When to use: For 90% of standard applications. It provides the best performance balance.
* Why: It minimizes network round-trips to the Kafka broker. It processes a batch of messages in memory and sends one single commit for the highest offset at the end.
* Risk: If your application crashes halfway through a batch, Kafka will re-deliver the entire batch when the application restarts. Your business logic must be idempotent (safe to run twice).

## 2. Use RECORD

* When to use: When your processing logic is slow, or you want to minimize duplicate processing if a crash happens.
* Why: If you poll 50 messages and the application crashes on message #25, Kafka knows you finished up to #24.
* Risk: High network overhead because it writes a commit to the broker for every single message.

## 3. Use MANUAL_IMMEDIATE

* When to use: When you need absolute, programmatic control over the message lifecycle—such as database transaction pinning or custom orchestration workflows (like your retry mechanism).
* Why: Spring hands you the Acknowledgment object. The offset is never committed unless your code explicitly calls ack.acknowledge().
* Risk: If your developer forgets to call ack.acknowledge(), the consumer will get stuck processing the exact same message over and over again on every application restart.

## 4. Use MANUAL

* When to use: Similar to MANUAL_IMMEDIATE, but you want slightly better performance.
* Why: Instead of stopping the thread to tell Kafka "I am done" after every message, calling ack.acknowledge() simply flags the message as done in your application memory. Spring aggregates these flags and sends them in one batch during the next poll cycle.

## 5. Use TIME, COUNT, or COUNT_TIME

* When to use: Micro-optimisations for extreme high-throughput pipelines.
* Why: You can configure Spring to only commit offsets every 5 seconds (TIME) or every 10,000 processed messages (COUNT), drastically reducing the load on your Kafka brokers.

------------------------------
## Summary Rule of Thumb

* If you want simplicity and performance, stick to BATCH and let Spring's error handler do the work.
* If your method signature includes Acknowledgment ack, you must use MANUAL or MANUAL_IMMEDIATE, or your application will crash on startup.

Here is the complete configuration and consumer code for the primary AckMode options.
To keep the examples clean, they all use a standard Java POJO class named KafkaDemoEvent.
------------------------------
## 1. BATCH Mode (Default / Automated)
When to use: Standard high-throughput processing. Offsets are committed after the entire batch of polled messages finishes processing.
## Configuration

@Beanpublic ConcurrentKafkaListenerContainerFactory<String, KafkaDemoEvent> batchKafkaListenerContainerFactory(
ConsumerFactory<String, KafkaDemoEvent> consumerFactory) {
ConcurrentKafkaListenerContainerFactory<String, KafkaDemoEvent> factory =
new ConcurrentKafkaListenerContainerFactory<>();
factory.setConsumerFactory(consumerFactory);

    // Explicitly setting BATCH (though it is the default)
    factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.BATCH);
    return factory;
}

## Consumer Code

@KafkaListener(topics = "batch-topic", containerFactory = "batchKafkaListenerContainerFactory")public void consumeBatch(KafkaDemoEvent event) {
// No acknowledgment parameter needed.
// If this method finishes without throwing an exception, Spring marks it as successful.
System.out.println("Processed event: " + event.payload());
}

------------------------------
## 2. RECORD Mode (Automated per Message)
When to use: When processing takes a long time, and you want to commit progress immediately after each message to minimize duplicates if the app crashes.
## Configuration

@Beanpublic ConcurrentKafkaListenerContainerFactory<String, KafkaDemoEvent> recordKafkaListenerContainerFactory(
ConsumerFactory<String, KafkaDemoEvent> consumerFactory) {
ConcurrentKafkaListenerContainerFactory<String, KafkaDemoEvent> factory =
new ConcurrentKafkaListenerContainerFactory<>();
factory.setConsumerFactory(consumerFactory);

    // Commit after every single record completes
    factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
    return factory;
}

## Consumer Code

@KafkaListener(topics = "record-topic", containerFactory = "recordKafkaListenerContainerFactory")public void consumeRecord(KafkaDemoEvent event) {
// Spring automatically sends a commit to Kafka the millisecond this method exits
System.out.println("Processed single record: " + event.payload());
}

------------------------------
## 3. MANUAL_IMMEDIATE Mode (Strict Manual Control)
When to use: When you need absolute control. The offset is sent to Kafka synchronously right inside your executing thread.
## Configuration

@Beanpublic ConcurrentKafkaListenerContainerFactory<String, KafkaDemoEvent> manualImmediateKafkaListenerContainerFactory(
ConsumerFactory<String, KafkaDemoEvent> consumerFactory) {
ConcurrentKafkaListenerContainerFactory<String, KafkaDemoEvent> factory =
new ConcurrentKafkaListenerContainerFactory<>();
factory.setConsumerFactory(consumerFactory);

    // Required to allow the Acknowledgment parameter in your method signature
    factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
    return factory;
}

## Consumer Code

@KafkaListener(topics = "manual-immediate-topic", containerFactory = "manualImmediateKafkaListenerContainerFactory")public void consumeManualImmediate(KafkaDemoEvent event, Acknowledgment ack) {
try {
System.out.println("Processing event: " + event.payload());

        // Explicitly commit now. Blocks the thread until Kafka confirms receipt.
        ack.acknowledge(); 
        
    } catch (Exception e) {
        System.err.println("Failed processing, offset NOT committed: " + e.getMessage());
    }
}

------------------------------
## 4. MANUAL Mode (Batched Manual Control)
When to use: When you want programmatic control, but want better performance than MANUAL_IMMEDIATE.
## Configuration

@Beanpublic ConcurrentKafkaListenerContainerFactory<String, KafkaDemoEvent> manualKafkaListenerContainerFactory(
ConsumerFactory<String, KafkaDemoEvent> consumerFactory) {
ConcurrentKafkaListenerContainerFactory<String, KafkaDemoEvent> factory =
new ConcurrentKafkaListenerContainerFactory<>();
factory.setConsumerFactory(consumerFactory);

    // Acknowledgment acts as a memory flag; actual commit happens on the next poll cycle
    factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
    return factory;
}

## Consumer Code

@KafkaListener(topics = "manual-topic", containerFactory = "manualKafkaListenerContainerFactory")public void consumeManual(KafkaDemoEvent event, Acknowledgment ack) {
System.out.println("Processing event: " + event.payload());

    // Flags this message as ready to commit in memory. 
    // Does not block the current thread with a network call to Kafka.
    ack.acknowledge(); 
}

------------------------------
## 5. COUNT / TIME Mode (Throttled Automated Commits)
When to use: High-throughput processing pipelines where minimizing Kafka broker network load is your highest priority.
## Configuration

@Beanpublic ConcurrentKafkaListenerContainerFactory<String, KafkaDemoEvent> countTimeKafkaListenerContainerFactory(
ConsumerFactory<String, KafkaDemoEvent> consumerFactory) {
ConcurrentKafkaListenerContainerFactory<String, KafkaDemoEvent> factory =
new ConcurrentKafkaListenerContainerFactory<>();
factory.setConsumerFactory(consumerFactory);

    // Commit whenever 1000 messages pass OR 5 seconds elapse (whichever happens first)
    factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.COUNT_TIME);
    factory.getContainerProperties().setAckCount(1000);
    factory.getContainerProperties().setAckTime(5000L); // 5000 ms
    
    return factory;
}

## Consumer Code

@KafkaListener(topics = "count-time-topic", containerFactory = "countTimeKafkaListenerContainerFactory")public void consumeCountTime(KafkaDemoEvent event) {
// Normal, simple consumer code.
// Offsets are batched and flushed quietly based on the time/count limits configured above.
System.out.println("Throttled processing: " + event.payload());
}




