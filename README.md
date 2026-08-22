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
