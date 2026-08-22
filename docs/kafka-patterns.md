# Kafka patterns reference

This lab intentionally gives each demonstration its own topic set and consumer group. That keeps records, offsets, retries, and log output from one exercise from changing another.

## Consumer patterns

- **Queue / competing consumers:** consumer instances in the same group divide partitions. A partition has one active consumer in that group, so ordering is preserved for a key on that partition.
- **Pub/sub:** each consumer group maintains an independent offset, so notification, audit, and analytics all receive the same record.
- **Retries:** Kafka retains records; retry is a listener processing policy. The retry examples make failures deterministic so the attempt logs are repeatable.
- **Dead Letter Topic (DLT):** after listener retries are exhausted, the recoverer publishes the original record to a `-dlt` topic with exception, topic, partition, offset, and timestamp headers. “DLQ” is a common informal name.
- **Request/reply:** the requester publishes a correlation ID and waits for a reply-topic record with the same correlation ID. It is deliberately a learning example; ordinary HTTP/gRPC is commonly preferable for synchronous service calls.
- **Replay:** `earliest` reads retained history, `latest` starts at new records, and an operator can reset a group to a selected offset. The lab never deletes group offsets automatically.
- **Event sourcing:** events are the source of truth; the consumer reconstructs an order state by applying `OrderCreated`, `PaymentReceived`, `OrderPacked`, and `OrderShipped` in order.

## Kafka Streams patterns

Kafka Streams builds a topology over topics rather than invoking an `@KafkaListener`. It can materialize local state stores, which are backed by changelog topics and restored after a restart.

- **Transform:** `mapValues` upper-cases the payload.
- **Filter:** only amounts greater than 1000 are emitted.
- **Branch:** mutually exclusive predicates route negative, high-value, and normal amounts to dedicated topics.
- **Aggregate:** records are grouped by customer ID and materialized in `customer-aggregates`.
- **KTable:** the input value is the tier string; multiple records for a customer produce one latest-value view.
- **Join:** a KStream of orders joins the latest customer KTable record. This is a KStream–KTable join; a KStream–KStream join instead correlates records within a time window.
- **Window:** a ten-second tumbling window counts and sums records per customer.
- **State store:** `customer-order-counts` tracks count and total per customer, is queryable through the REST endpoint, and has a Kafka changelog.
- **Streams DLT:** a processor exception is handled by Spring Kafka 4.1's `RecoveringProcessingExceptionHandler`, which forwards the failed raw record to the dedicated streams DLT and allows the topology to continue. This differs from a listener `DefaultErrorHandler`.

## Operations

Inspect topic and group state with Kafka CLI tools in the Kafka container:

```bash
docker exec kafka-lab-broker /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
docker exec kafka-lab-broker /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --all-groups
```

Partition count controls maximum parallelism within a group. More consumers than partitions leaves consumers idle; fewer consumers than partitions assigns multiple partitions to some consumers.
