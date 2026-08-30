# 🌊 Kafka Streams – Stream Service

A comprehensive **Spring Boot + Kafka Streams** module that demonstrates eight core stream-processing patterns.  
Each pattern lives in its own topology class under `src/main/java/.../topology/` and runs as a fully independent processing pipeline inside a single Kafka Streams application.

---

## 📋 Table of Contents

- [Architecture Overview](#architecture-overview)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Getting Started](#getting-started)
- [Stream Patterns](#stream-patterns)
  - [1. Filter Pattern](#1-filter-pattern)
  - [2. Transform Pattern](#2-transform-pattern)
  - [3. Branch Pattern](#3-branch-pattern)
  - [4. Aggregate Pattern](#4-aggregate-pattern)
  - [5. Window Pattern](#5-window-pattern)
  - [6. Join Pattern](#6-join-pattern)
  - [7. KTable Pattern](#7-ktable-pattern)
  - [8. Error Handling Pattern](#8-error-handling-pattern)
  - [9. State Store Service](#9-state-store-service-bonus)
- [Pattern Decision Guide](#-pattern-decision-guide)
- [REST API – Interactive Query](#-rest-api--interactive-query)
- [Configuration](#-configuration)
- [Pattern Comparison Table](#-pattern-comparison-table)

---

## Architecture Overview

```
                         ┌─────────────────────────────────────────────────┐
                         │           Kafka Streams Application              │
                         │                (kafka-lab-streams)               │
                         │                                                  │
  Input Topics ─────────►│  FilterTopology     BranchTopology              │
                         │  TransformTopology  AggregateTopology           │
                         │  WindowTopology     JoinTopology                │
                         │  KTableTopology     ErrorHandlingTopology       │
                         │                     StateStoreService           │
                         │                                                  │
                         │  ┌──────────────┐  ┌──────────────────────────┐│
                         │  │ State Stores │  │  Interactive Query API   ││
                         │  │(RocksDB-backed│  │  GET /api/kafka/streams/ ││
                         │  │  key-value)  │  │  {aggregate,ktable,state}││
                         │  └──────────────┘  └──────────────────────────┘│
                         └─────────────────────────────────────────────────┘
                                            │
                                  Output Topics ──► Downstream Consumers
```

---

## Tech Stack

| Component | Technology |
|---|---|
| Language | Java 21 |
| Framework | Spring Boot 3.x |
| Stream Engine | Apache Kafka Streams |
| Serialization | Jackson JSON (JacksonJsonSerde) |
| State Backend | RocksDB (embedded, managed by Kafka Streams) |
| Error Handler | `RecoveringProcessingExceptionHandler` → DLT |
| Processing Guarantee | `exactly_once_v2` |
| REST API | Spring Web MVC |
| Observability | Spring Boot Actuator |

---

## Project Structure

```
stream-service/
├── pom.xml
└── src/
    └── main/
        ├── java/com/example/kafkalab/streams/
        │   ├── StreamServiceApplication.java
        │   ├── config/
        │   │   └── StreamsConfig.java          ← Global Kafka Streams config & Serde beans
        │   ├── controller/
        │   │   └── StreamsQueryController.java  ← REST endpoints for interactive queries
        │   ├── state/
        │   │   └── StateStoreService.java       ← Persistent state store topology
        │   └── topology/
        │       ├── aggregate/AggregateTopology.java
        │       ├── branch/BranchTopology.java
        │       ├── error/ErrorHandlingTopology.java
        │       ├── filter/FilterTopology.java
        │       ├── join/JoinTopology.java
        │       ├── ktable/KTableTopology.java
        │       ├── transform/TransformTopology.java
        │       └── window/WindowTopology.java
        └── resources/
            └── application.yml
```

---

## Getting Started

### Prerequisites
- Java 21+
- Apache Kafka running on `localhost:9092` (or override via `KAFKA_BOOTSTRAP_SERVERS`)
- Maven 3.9+

### Run the Service

```bash
# From the kafka-patterns root directory
mvn spring-boot:run -pl stream-service

# Or with a custom Kafka broker
KAFKA_BOOTSTRAP_SERVERS=kafka-host:9092 mvn spring-boot:run -pl stream-service
```

The service starts on **port `8083`** with the Kafka Streams app-id `kafka-lab-streams`.

### Health Check

```bash
curl http://localhost:8083/actuator/health
```

---

## Stream Patterns

---

### 1. Filter Pattern

**File:** `src/main/java/com/example/kafkalab/streams/topology/filter/FilterTopology.java`

#### What It Does

Reads a stream of `KafkaDemoEvent` messages and **passes only those whose `amount` exceeds a configured threshold (1,000)** to the output topic. Events below the threshold are silently discarded.

```
streams-filter-input  ──► filter(amount > 1000) ──► streams-filter-output
                                    │
                             (discarded events)
```

#### Key Code Concept

```java
KStream<String, KafkaDemoEvent> filtered = input.filter((key, event) ->
    event.amount().compareTo(THRESHOLD) > 0   // THRESHOLD = 1000
);
filtered.to(KafkaTopics.STREAMS_FILTER_OUTPUT, ...);
```

#### When to Use This Pattern in Real Life

| Scenario | Why Filter? |
|---|---|
| **Fraud detection pre-screening** | Only forward transactions above a suspicious threshold (e.g., > $10,000) to a fraud-analysis service |
| **IoT sensor noise reduction** | Discard sensor readings outside a valid range before sending to a time-series DB |
| **Alert generation** | Only propagate events where `error_level == CRITICAL` to an alerting topic |
| **GDPR / data compliance** | Strip or drop events that match certain customer IDs flagged for deletion |
| **Payment gateway** | Only process payments with a `status == APPROVED` flag and skip others |

> **Rule of thumb:** Use `filter` whenever you need to **reduce the volume** of a stream based on a simple predicate, without needing state or memory from previous events.

---

### 2. Transform Pattern

**File:** `src/main/java/com/example/kafkalab/streams/topology/transform/TransformTopology.java`

#### What It Does

Reads a stream of events and **transforms each record's payload** (converts to uppercase in this demo) while keeping the same key. The transformation is stateless — each event is processed independently.

```
streams-transform-input ──► mapValues(payload → UPPERCASE) ──► streams-transform-output
```

#### Key Code Concept

```java
KStream<String, KafkaDemoEvent> transformed = input.mapValues((key, event) -> {
    String upperPayload = event.payload().toUpperCase();
    return new KafkaDemoEvent(
        event.eventId(), event.correlationId(), event.eventType(),
        event.customerId(), event.orderId(), event.amount(),
        upperPayload, event.timestamp(), event.metadata()
    );
});
```

> **`mapValues` vs `map`**: `mapValues` is preferred when you don't change the key, because Kafka Streams can **skip the repartition step**, making it significantly more efficient.

#### When to Use This Pattern in Real Life

| Scenario | Why Transform? |
|---|---|
| **Data normalization** | Normalize currency codes, phone number formats, or address fields to a standard format |
| **Event enrichment (stateless)** | Add a computed field (e.g., `taxAmount = amount * 0.18`) without needing a lookup table |
| **Schema migration** | Map from a v1 event schema to a v2 schema as part of a rolling upgrade |
| **Masking PII** | Replace SSN/card numbers with masked values before forwarding to analytics topics |
| **Log ingestion pipeline** | Parse raw log strings into structured JSON events in real time |
| **Unit conversion** | Convert Fahrenheit to Celsius for IoT readings on the fly |

> **Rule of thumb:** Use `transform`/`mapValues` when the new event value is derivable **purely from the current event** — no historical state, no lookups.

---

### 3. Branch Pattern

**File:** `src/main/java/com/example/kafkalab/streams/topology/branch/BranchTopology.java`

#### What It Does

Reads a single input stream and **routes each event to one of three output topics** based on its `amount`:

- **Suspicious** → `amount < 0` (negative amounts)
- **High Value** → `amount >= 10,000`
- **Normal** → everything else

```
                         ┌──► streams-branch-suspicious  (amount < 0)
streams-branch-input ────┼──► streams-branch-high-value  (amount >= 10000)
                         └──► streams-branch-normal       (0 <= amount < 10000)
```

#### Key Code Concept

```java
// Each branch is an independent filter on the same input stream
input.filter((key, e) -> e.amount().compareTo(ZERO) < 0)
     .to(KafkaTopics.STREAMS_BRANCH_SUSPICIOUS, ...);

input.filter((key, e) -> e.amount().compareTo(HIGH_VALUE_THRESHOLD) >= 0)
     .to(KafkaTopics.STREAMS_BRANCH_HIGH_VALUE, ...);

input.filter((key, e) -> e.amount().compareTo(ZERO) >= 0
         && e.amount().compareTo(HIGH_VALUE_THRESHOLD) < 0)
     .to(KafkaTopics.STREAMS_BRANCH_NORMAL, ...);
```

> The predicates are designed to be **mutually exclusive** — each event lands in exactly one topic.

#### When to Use This Pattern in Real Life

| Scenario | Why Branch? |
|---|---|
| **Order routing** | Route domestic orders, international orders, and B2B orders to separate fulfilment services |
| **Risk tiering** | Split loan applications into Low/Medium/High risk queues for different underwriting teams |
| **Event type fan-out** | A single CDC (Change Data Capture) stream that needs to fan out to multiple downstream consumers by event type |
| **Log level routing** | Route `ERROR` logs to PagerDuty, `WARN` to Slack, `INFO` to Elasticsearch |
| **Multi-tenant SaaS** | Route events from a shared ingest topic to per-tenant processing topics |
| **A/B testing** | Route a percentage of traffic to a "canary" processing path |

> **Rule of thumb:** Use branching when **one input stream needs to feed multiple independent downstream pipelines** based on content-based routing logic.

---

### 4. Aggregate Pattern

**File:** `src/main/java/com/example/kafkalab/streams/topology/aggregate/AggregateTopology.java`

#### What It Does

Groups events by `customerId` and **continuously accumulates a running total** (`totalAmount`) and a count (`orderCount`) per customer. The result is materialized into a persistent **RocksDB state store** (`customer-aggregates`) and also streamed to an output topic.

```
streams-aggregate-input
    │
    ├── groupBy(customerId)
    ├── aggregate(totalAmount += amount, orderCount++)
    │
    ├─── State Store: "customer-aggregates"  (queryable via REST)
    └─── streams-aggregate-output
```

#### Key Code Concept

```java
KTable<String, AggregationResult> aggregated = input
    .groupBy((key, event) -> event.customerId())
    .aggregate(
        () -> new AggregationResult(null, BigDecimal.ZERO, 0, now(), now()), // initializer
        (customerId, event, agg) -> new AggregationResult(
            customerId,
            agg.totalAmount().add(event.amount()),   // running sum
            agg.orderCount() + 1,                    // running count
            agg.windowStart(), Instant.now()
        ),
        Materialized.as("customer-aggregates")
    );
```

#### When to Use This Pattern in Real Life

| Scenario | Why Aggregate? |
|---|---|
| **Real-time customer spend tracking** | Maintain a live running total of spend per customer for credit limit checks |
| **Leaderboards / scoreboards** | Aggregate game scores per player without batch jobs |
| **Real-time inventory** | Maintain current stock levels by incrementing/decrementing as events arrive |
| **Billing and metering** | Accumulate API call counts or data transfer volumes per tenant in real time |
| **Social media engagement** | Continuously count likes, shares, and comments per post |
| **Microservices CQRS** | Build a read-model (query projection) from a stream of write events |

> **Rule of thumb:** Use `aggregate` when you need to **maintain a continuously updated summary** of data per key, where the summary spans multiple events over time (unlike `window`, there is no time boundary — it is unbounded).

---

### 5. Window Pattern

**File:** `src/main/java/com/example/kafkalab/streams/topology/window/WindowTopology.java`

#### What It Does

Groups events by `customerId`, then applies a **10-second tumbling window** to compute per-window aggregates (count + sum). Each window is independent — results are emitted when a window closes. The windowed results are stored in a `WindowStore` (`window-counts`).

```
Time:  ──[0s──────10s]──[10s──────20s]──[20s──────30s]──►
                │               │               │
          Window 1          Window 2         Window 3
          (per customer)    (per customer)   (per customer)

streams-window-input ──► groupBy(customerId) ──► windowedBy(10s tumbling)
    ──► aggregate(count, sum) ──► streams-window-output
```

#### Key Code Concept

```java
KTable<Windowed<String>, WindowResult> windowed = input
    .groupBy((key, event) -> event.customerId())
    .windowedBy(TimeWindows.ofSizeWithNoGrace(WINDOW_SIZE))  // WINDOW_SIZE = 10 seconds
    .aggregate(
        () -> new WindowResult(null, 0, BigDecimal.ZERO, null, null),
        (customerId, event, agg) -> new WindowResult(
            customerId,
            agg.count() + 1,
            agg.sum().add(event.amount()),
            windowStart, windowEnd
        ),
        Materialized.as("window-counts")
    );
```

#### Types of Windows (Kafka Streams supports)

| Window Type | Description | Use Case |
|---|---|---|
| **Tumbling** (used here) | Fixed-size, non-overlapping windows | Hourly reports, rate limiting per minute |
| **Hopping** | Fixed-size, overlapping windows | Rolling averages |
| **Session** | Activity-based, gap-triggered | User session analysis |
| **Sliding** | Continuous, event-time based | Real-time monitoring dashboards |

#### When to Use This Pattern in Real Life

| Scenario | Why Window? |
|---|---|
| **Rate limiting / throttling** | Count API calls per user per minute; block if count > threshold |
| **Real-time dashboards** | Show orders placed in the last 5 minutes, refreshing continuously |
| **Anomaly detection** | Detect spike in error rate within a 30-second window |
| **SLA monitoring** | Alert if response time average exceeds 500ms in a 1-minute tumbling window |
| **Clickstream analysis** | Count page views per user per session (session windows) |
| **Financial settlement** | Compute daily trading volume (24h tumbling window) |

> **Rule of thumb:** Use `window` when the aggregation has a **time dimension** — the result resets or scopes to a bounded time period. Prefer `aggregate` (unbounded) for lifetime totals.

---

### 6. Join Pattern

**File:** `src/main/java/com/example/kafkalab/streams/topology/join/JoinTopology.java`

#### What It Does

Demonstrates a **KStream-KTable join**: a stream of `Order` events is enriched in real time with `Customer` information (name, tier) from a KTable. The KTable always holds the **latest** customer record per `customerId`, so joins always use the most up-to-date customer data.

```
streams-join-orders   ──► KStream (Order events)
                               │
                               ├── join(customers KTable)
                               │       │
streams-join-customers ──► KTable │    └──► EnrichedOrder (order + customer.name + customer.tier)
  (latest customer per key)        │
                               └──► streams-join-output
```

#### Key Code Concept

```java
// KTable holds latest customer info per customerId
KTable<String, Customer> customers = builder.table(STREAMS_JOIN_CUSTOMERS, ...);

// KStream of incoming orders
KStream<String, Order> orders = builder.stream(STREAMS_JOIN_ORDERS, ...);

// KStream-KTable join — no time window needed; KTable always holds latest value
KStream<String, EnrichedOrder> enrichedOrders = orders.join(customers,
    (order, customer) -> new EnrichedOrder(
        order.orderId(), order.customerId(),
        customer != null ? customer.name() : "Unknown",
        customer != null ? customer.tier() : "Unknown",
        order.amount(), order.product(), order.timestamp()
    ));
```

#### Join Types in Kafka Streams

| Join Type | Left Type | Right Type | Match Behaviour |
|---|---|---|---|
| **KStream-KTable** (this demo) | KStream | KTable | Event-driven; only fires when a stream record arrives |
| **KStream-KStream** | KStream | KStream | Requires a time window; both streams are unbounded |
| **KTable-KTable** | KTable | KTable | Fires on updates to either side; produces a KTable |
| **GlobalKTable** | KStream | GlobalKTable | All partitions available on every node; no co-partitioning needed |

#### When to Use This Pattern in Real Life

| Scenario | Why Join? |
|---|---|
| **Order enrichment** | Enrich order events with real-time customer profile (name, loyalty tier, address) |
| **Payment validation** | Join incoming payment attempts with a KTable of fraud-flagged accounts |
| **Inventory fulfilment** | Join product order events with a product catalogue KTable to resolve product names and prices |
| **Clickstream attribution** | Join click events with a user-session KTable to attribute clicks to marketing campaigns |
| **IoT device tracking** | Join telemetry events with a device-registry KTable to add device metadata |
| **Real-time recommendation** | Join user activity stream with a product recommendation KTable |

> **Rule of thumb:** Use **KStream-KTable join** when you want to enrich a fast-moving event stream with a **slowly-changing reference dataset** (master data). Use **KStream-KStream join** when both sides are event streams that need to be correlated within a time window.

---

### 7. KTable Pattern

**File:** `src/main/java/com/example/kafkalab/streams/topology/ktable/KTableTopology.java`

#### What It Does

Reads a topic as a **KTable** rather than a KStream. A KTable treats each message as an **upsert** — if a new message arrives for the same key, it **replaces** the old value. This means the KTable always holds only the **latest value per key**. The current state is materialized into a `customer-tiers` store and forwarded to an output topic.

```
streams-ktable-input  (key=customerId, value=tier string)
    │
    ├── KTable[upsert semantics: latest-value-per-key]
    │         │
    │   State Store: "customer-tiers"  ← queryable via REST
    │
    └── streams-ktable-output (only emits on change)
```

#### Key Code Concept

```java
// Reading topic as a KTable → each message is an upsert on the key
KTable<String, String> customerTiers = builder.table(
    KafkaTopics.STREAMS_KTABLE_INPUT,
    Consumed.with(Serdes.String(), Serdes.String()),
    Materialized.as("customer-tiers")  // backed by RocksDB
);

// Emit downstream only on value change (changelog semantics)
customerTiers.toStream().to(KafkaTopics.STREAMS_KTABLE_OUTPUT, ...);
```

> **KStream vs KTable** in a nutshell:
> - `KStream` → every record is an independent event. A stream of clicks.
> - `KTable` → each record updates the "current state" for a key. A table of user preferences.

#### When to Use This Pattern in Real Life

| Scenario | Why KTable? |
|---|---|
| **Customer master data** | Maintain the latest profile/tier/address for each customer, updated via CDC |
| **Feature flags / config** | Distribute real-time configuration updates to processing nodes via a KTable |
| **Product catalogue** | Keep the current price and stock status per SKU up-to-date in-stream |
| **User permissions** | Maintain current role/permission set per user, referenced during event processing |
| **DNS / service registry** | A KTable of service endpoints, updated as instances come and go |
| **Deduplicate events** | Use a KTable to track "seen event IDs" and filter out duplicates |

> **Rule of thumb:** Use a KTable when the topic models **state** (latest value matters, history doesn't). Use KStream when the topic models **events** (every record matters, order matters).

---

### 8. Error Handling Pattern

**File:** `src/main/java/com/example/kafkalab/streams/topology/error/ErrorHandlingTopology.java`

#### What It Does

Demonstrates **resilient error handling in Kafka Streams**. Events are processed with `mapValues`; if the payload contains the string `"FAIL"`, a `RuntimeException` is intentionally thrown. Instead of crashing the entire Streams application, the `RecoveringProcessingExceptionHandler` (configured globally in `StreamsConfig`) catches the exception and routes the failed record to a **Dead Letter Topic (DLT)** with enriched headers.

```
streams-error-input
    │
    ├── mapValues(process or throw if payload contains "FAIL")
    │       │
    │   [success] ──► streams-error-output
    │       │
    │   [failure] ──► RecoveringProcessingExceptionHandler
    │                       │
    │               streams-error-dlt  (with DLT headers: exception, original topic, offset)
    └───────────────────────────────────────────────────────────
```

#### Key Code Concept

```java
// In ErrorHandlingTopology.java
KStream<String, KafkaDemoEvent> processed = input.mapValues((key, event) -> {
    if (event.payload() != null && event.payload().contains("FAIL")) {
        throw new RuntimeException("Intentional failure for DLT demo"); // triggers DLT routing
    }
    return new KafkaDemoEvent(..., "PROCESSED: " + event.payload(), ...);
});

// In StreamsConfig.java — global handler for all topologies
props.put(StreamsConfig.PROCESSING_EXCEPTION_HANDLER_CLASS_CONFIG,
    RecoveringProcessingExceptionHandler.class);
props.put("errors.dead.letter.queue.topic.name", KafkaTopics.STREAMS_ERROR_DLT);
```

#### When to Use This Pattern in Real Life

| Scenario | Why Error Handling / DLT? |
|---|---|
| **Deserialization failures** | A malformed JSON arrives — don't crash the app, route to DLT for human inspection |
| **Downstream service timeouts** | An enrichment call to a REST API fails — route the event to DLT for retry later |
| **Schema validation** | An event missing required fields should not block the entire partition |
| **Business rule violations** | An order event with a negative quantity should be flagged, not silently dropped |
| **Replay & debugging** | Failed events in the DLT can be replayed after the bug is fixed |
| **SLA compliance** | Ensure processing failures are auditable and do not cause data loss |

> **Rule of thumb:** Always configure a DLT for production Kafka Streams topologies. Without it, a single poison-pill message can halt an entire partition's processing indefinitely.

---

### 9. State Store Service *(Bonus)*

**File:** `src/main/java/com/example/kafkalab/streams/state/StateStoreService.java`

#### What It Does

Similar to `AggregateTopology`, but explicitly demonstrates **manual state store management**. It registers a named, persistent `KeyValueStore` (`customer-order-counts`) using `Stores.persistentKeyValueStore`, then uses `aggregate` backed by that store to maintain per-customer order counts and totals. The state is queryable via the REST API.

```
streams-state-input
    ├── groupBy(customerId)
    ├── aggregate → Materialized.as("customer-order-counts")  [RocksDB]
    └── streams-state-output
```

#### When to Use This Pattern in Real Life

| Scenario | Why Explicit State Store? |
|---|---|
| **Interactive queries** | Expose state store data directly via a REST API without a separate database |
| **Multi-topology shared state** | Share a named store between different parts of the same topology |
| **Custom changelog retention** | Control compaction and retention settings on the backing changelog topic |
| **Queryable microservice pattern** | Build a service that answers questions like "what is the current balance for account X?" directly from the stream processor |

---

## 🗺️ Pattern Decision Guide

Use this flowchart to choose the right pattern for your use case:

```
Start: What do you need to do with the incoming event stream?
│
├─► Do you need to DISCARD some events?
│        └─► Use FILTER Pattern
│
├─► Do you need to CHANGE the content of each event?
│        └─► Use TRANSFORM Pattern (mapValues / map)
│
├─► Do you need to SPLIT one stream into MULTIPLE streams?
│        └─► Use BRANCH Pattern
│
├─► Do you need to COMBINE events from TWO streams?
│        ├─► One side is a reference table (slowly changing)?
│        │        └─► Use JOIN Pattern (KStream-KTable)
│        └─► Both sides are fast event streams?
│                 └─► Use JOIN Pattern (KStream-KStream with window)
│
├─► Do you need to COMPUTE A SUMMARY across many events?
│        ├─► Summary scoped to a TIME PERIOD (e.g., per hour)?
│        │        └─► Use WINDOW Pattern
│        └─► Summary is UNBOUNDED (lifetime total)?
│                 └─► Use AGGREGATE Pattern
│
├─► Do you need LATEST-VALUE-PER-KEY semantics (like a database table)?
│        └─► Use KTABLE Pattern
│
├─► Do you need to handle PROCESSING FAILURES gracefully?
│        └─► Use ERROR HANDLING Pattern (DLT)
│
└─► Do you need to QUERY CURRENT STATE via REST API?
         └─► Use STATE STORE Pattern (Interactive Query)
```

---

## 🌐 REST API – Interactive Query

The `StreamsQueryController` exposes **interactive queries** against materialized state stores, allowing you to inspect the current state without reading from a topic.

| Endpoint | State Store | Description |
|---|---|---|
| `GET /api/kafka/streams/aggregate/{customerId}` | `customer-aggregates` | Get running total and order count for a customer |
| `GET /api/kafka/streams/ktable/{customerId}` | `customer-tiers` | Get current loyalty tier for a customer |
| `GET /api/kafka/streams/state/{customerId}` | `customer-order-counts` | Get state-store count/total for a customer |
| `GET /api/kafka/streams/state/all` | `customer-order-counts` | Get state for all customers |

### Example Calls

```bash
# Query aggregate state for customer CUST-001
curl http://localhost:8083/api/kafka/streams/aggregate/CUST-001

# Response:
{
  "customerId": "CUST-001",
  "totalAmount": 12500.00,
  "orderCount": 5,
  "windowStart": "2026-08-30T10:00:00Z",
  "windowEnd": "2026-08-30T14:30:00Z"
}

# Query customer tier
curl http://localhost:8083/api/kafka/streams/ktable/CUST-001
# Response: { "customerId": "CUST-001", "tier": "GOLD" }

# Get all state entries
curl http://localhost:8083/api/kafka/streams/state/all
```

---

## ⚙️ Configuration

The service is configured in `application.yml` and `StreamsConfig.java`:

| Property | Value | Description |
|---|---|---|
| `server.port` | `8083` | HTTP port for REST API and Actuator |
| `spring.kafka.bootstrap-servers` | `localhost:9092` | Kafka broker address (override via env: `KAFKA_BOOTSTRAP_SERVERS`) |
| `application-id` | `kafka-lab-streams` | Kafka Streams app ID (used for consumer groups and state store topics) |
| `commit.interval.ms` | `1000` | How often Kafka Streams commits offsets (1 second) |
| `cache.max.bytes.buffering` | `10 MB` | In-memory cache for buffering state before flushing to RocksDB |
| `processing.guarantee` | `exactly_once_v2` | Exactly-once semantics using transactions |
| `auto-offset-reset` | `earliest` | Start consuming from the beginning if no committed offset exists |
| `processing.exception.handler` | `RecoveringProcessingExceptionHandler` | Routes failed records to DLT instead of crashing |
| DLT topic | `streams-error-dlt` | Destination topic for failed records |

---

## 📊 Pattern Comparison Table

| Pattern | Stateful? | Output Cardinality | Time-aware? | Typical Latency | Best For |
|---|---|---|---|---|---|
| **Filter** | No | Same or fewer records | No | Ultra-low | Reducing stream volume |
| **Transform** | No | Same count, different shape | No | Ultra-low | Data normalization/enrichment |
| **Branch** | No | Same records, multiple topics | No | Ultra-low | Content-based routing |
| **Aggregate** | Yes (KV Store) | One record per key (running total) | No (unbounded) | Low | Lifetime summaries |
| **Window** | Yes (Window Store) | One record per key per window | Yes | Low-Medium | Time-scoped summaries |
| **Join** | Yes (KTable side) | One enriched record per input | Depends on type | Low-Medium | Data enrichment |
| **KTable** | Yes (KV Store) | Latest value per key | No | Low | Reference data / master data |
| **Error Handling** | No (handler level) | Same records or DLT | No | Ultra-low | Resilience & auditability |
| **State Store** | Yes (KV Store) | Per-key aggregations | No | Low | Interactive query + aggregation |

---

## 📚 Further Reading

- [Apache Kafka Streams Documentation](https://kafka.apache.org/documentation/streams/)
- [Spring Kafka Streams Reference](https://docs.spring.io/spring-kafka/reference/streams.html)
- [Kafka Streams Interactive Queries](https://kafka.apache.org/documentation/streams/developer-guide/interactive-queries.html)
- [Exactly-Once Semantics in Kafka Streams](https://www.confluent.io/blog/exactly-once-semantics-are-possible-heres-how-apache-kafka-does-it/)
- [Dead Letter Topics with Spring Kafka](https://docs.spring.io/spring-kafka/reference/kafka/annotation-error-handling.html)

---

*This module is part of the `kafka-patterns` multi-module project. Each pattern is self-contained and can be studied independently.*
