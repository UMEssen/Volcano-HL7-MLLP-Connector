# Volcano HL7 MLLP Connector

A high-performance HL7 v2 MLLP listener that converts healthcare messages to JSON and publishes them to Apache Kafka.

## Features

- **MLLP Protocol Support** - Listens for HL7 v2 messages over MLLP (Minimum Lower Layer Protocol)
- **JSON Conversion** - Converts HL7 messages to structured JSON with metadata extraction
- **Kafka Integration** - Publishes messages to Kafka topics based on message structure
- **Reliable Delivery** - Synchronous sends with deterministic ACK/NAK responses
- **Privacy-First** - Metadata-only logging (no PHI in logs)
- **Topic Routing** - Automatic routing based on MSH-9.1 (type) and MSH-9.2 (event) fields

## Quick Start with Docker Compose

```bash
# 1. Build the Docker image
docker build -t volcano-hl7-mllp:latest .

# 2. Start the entire stack (Kafka + Connector + Kafka UI)
docker compose up -d

# 3. View logs
docker compose logs -f volcano-connector

# 4. Stop the stack
docker compose down
```

The connector will be available on port **2575** and Kafka UI on **http://localhost:8080**.

## Manual Build and Run

### Prerequisites

- Java 21+ (CI and the Docker image use 25)
- SBT 1.x
- Kafka cluster (or use docker-compose)

### Build

```bash
# Compile
sbt compile

# Run tests
sbt "Test/runMain volcano.hl7mllp.JsonConversionTest"

# Create JAR
sbt package

# Run locally
sbt run
```

### Docker Build

```bash
# Build image
docker build -t volcano-hl7-mllp:latest .

# Run container
docker run -p 2575:2575 \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
  volcano-hl7-mllp:latest
```

## Configuration

Configure via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `MLLP_PORT` | `2575` | Port to listen on for MLLP connections |
| `MLLP_TLS` | `false` | Enable TLS for MLLP (set to `true` for encrypted) |
| `MLLP_MAX_FRAME_BYTES` | `KAFKA_MAX_REQUEST_SIZE` | Hard cap on one inbound MLLP frame, enforced at the wire before parsing. `0` disables it. |
| `HL7_ENCODING` | `UTF-8` | Character encoding (UTF-8, ISO-8859-1, windows-1252, US-ASCII). MSH-18 is ignored. |
| `HL7_INCLUDE_RAW` | `true` | Include the raw ER7 string in the JSON envelope |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker addresses |
| `KAFKA_TOPIC` | _(unset)_ | **Static topic.** When set, all messages route here (feed-based model). Overrides the derived scheme below. |
| `KAFKA_TOPIC_PREFIX` | `volcano.` | Prefix for derived topic names (used when `KAFKA_TOPIC` is unset) |
| `KAFKA_TOPIC_INFIX` | `hl7.v2.` | Inserted between prefix and `{type}.{event}`; set `""` to suppress. Ignored when `KAFKA_TOPIC` is set. |
| `KAFKA_MAX_REQUEST_SIZE` | `10485760` | Max record size (bytes). Keep ≥ topic `max.message.bytes`; Kafka's 1 MiB default NAKs document HL7. |
| `KAFKA_BUFFER_MEMORY` | `67108864` | Producer buffer (bytes) |
| `KAFKA_COMPRESSION_TYPE` | `lz4` | Producer compression |
| `KAFKA_CLIENT_ID` | `volcano-hl7-mllp` | Kafka client identifier |
| `KAFKA_ACK_TIMEOUT_MS` | `5000` | How long a Kafka outage may block `send()` itself; sets `max.block.ms`. Raised to `KAFKA_DELIVERY_TIMEOUT_MS` for the ACK wait — see [Delivery semantics](#delivery-semantics). |
| `KAFKA_REQUEST_TIMEOUT_MS` | `5000` | `request.timeout.ms` — one broker round trip |
| `KAFKA_DELIVERY_TIMEOUT_MS` | `10000` | `delivery.timeout.ms` — the producer's total retry budget per record. The ACK wait is derived as this plus a 2s margin. **Lower this to make the connector NAK faster.** |
| `METRICS_ENABLED` | `true` | Expose Prometheus `/metrics` + `/healthz` |
| `METRICS_PORT` | `9404` | Metrics/health HTTP port |
| `JAVA_OPTS` | `-Xmx512m -Xms256m` | JVM options |

## Topic Naming & Routing

### Static topic (recommended): `KAFKA_TOPIC=…`

When `KAFKA_TOPIC` is set, every message from the instance is produced to that one topic. Topic identity comes from the **feed/deployment** (which you control), not the HL7 message type (which the upstream may change without notice). The message type and trigger event still travel in the Kafka headers and JSON payload, so consumers dispatch on metadata. This is the model for a one-connector-per-source-feed deployment.

- Example: `KAFKA_TOPIC=volcano.producer.hl7.v2.example.adt` → all ADT events for that feed land in `volcano.producer.hl7.v2.example.adt`.
- Robust to drift: a new/unknown message type just lands in the same topic; nothing fails.

### Derived topic (fallback): MSH-9.1 + MSH-9.2

Used only when `KAFKA_TOPIC` is unset. Topic = `{KAFKA_TOPIC_PREFIX}{KAFKA_TOPIC_INFIX}{type}.{event}`, lowercased.
- Example: `volcano.hl7.v2.adt.a01`
- Both MSH-9.1 and MSH-9.2 are mandatory in every HL7 v2.x version, so this covers all senders.
- Missing fields fall back to `UNKNOWN` (e.g. `volcano.hl7.v2.unknown.a01`).

## Observability

Prometheus metrics + health are served on `METRICS_PORT` (default `9404`):
- `GET /metrics` — Prometheus text exposition
- `GET /healthz` — liveness (process up)

Key metrics: `hl7_messages_received_total{message_type}`, `hl7_messages_processed_total{result}`, `hl7_kafka_produced_total{topic}`, `hl7_kafka_produce_failures_total{reason}`, `hl7_kafka_produce_duration_seconds`, `hl7_messages_in_flight`, `hl7_mllp_frames_rejected_total{reason}`, plus JVM/process metrics. The MLLP TCP probe stays healthy during a Kafka outage by design (so the sender buffers), so **alert on `hl7_kafka_produce_failures_total`** rather than on pod health.

## JSON Output Format

Field values in `segments[].fields[].value` are the field's **ER7 encoding** —
exactly the substring the field occupies in `hl7_raw`. Components are joined
with `^`, subcomponents with `&`, repetitions are already split into separate
entries, and delimiter characters occurring inside data stay escaped (`\F\`,
`\S\`, …). The two delimiter-defining fields, `MSH-1` and `MSH-2` (and their
`FHS`/`BHS` equivalents), are emitted verbatim, since escaping the characters
they declare would be circular.

```json
{
  "schema_version": "1.1",
  "hl7_raw": "MSH|^~\\&|SENDING_APP|...",
  "metadata": {
    "message_type": "ADT",
    "trigger_event": "A01",
    "message_structure": "ADT_A01",
    "message_control_id": "MSG00001",
    "sending_application": "SENDING_APP",
    "sending_facility": "SENDING_FAC",
    "receiving_application": "RECEIVING_APP",
    "receiving_facility": "RECEIVING_FAC",
    "message_datetime": "20251019103000",
    "version": "2.5"
  },
  "segments": [
    {
      "segment_name": "MSH",
      "fields": [
        {"field": 1, "repetition": 0, "value": "|"},
        {"field": 2, "repetition": 0, "value": "^~\\&"},
        {"field": 3, "repetition": 0, "value": "SENDING_APP"},
        {"field": 9, "repetition": 0, "value": "ADT^A01^ADT_A01"}
      ]
    }
  ]
}
```

### Envelope versions

`schema_version` is emitted both in the body and as a Kafka header, so a
consumer can branch without guessing.

| Version | Change |
|---------|--------|
| `1.0` | Initial shape. `segments[].fields[].value` was HAPI's `Type.toString()`, which is a *debug* rendering: every non-primitive field arrived wrapped in its datatype class name — `HD[SENDING_APP]`, `MSG[ADT^A01^ADT_A01]`, `Varies[...]`. |
| `1.1` | `segments[].fields[].value` is the field's ER7 encoding (above). No shape change; the wrappers are gone, and a primitive containing an escaped delimiter now keeps the escape rather than the decoded character. |

The primitive case is the one delta that is not just wrapper removal, and it is
worth being precise about: `AbstractPrimitive.toString()` returns `getValue()`,
the *decoded* value, so a primitive whose wire form was `re\F\f` used to appear
as `re|f` — a bare field separator inside a single-component value, and `a\S\b`
used to appear as `a^b`, which any consumer splitting on `^` reads as two
components. Only primitives behaved that way; `AbstractType.toString()` wraps
everything else and preserves the escapes inside the wrapper, so composites see
wrapper removal and nothing more.

Consumers that strip the `1.0` wrappers must keep doing so for as long as they
may replay archived `1.0` records.

## Delivery semantics

### ACK ordering

The handler is fully synchronous: parse → build envelope → `producer.send(...)`
→ **block until the broker acknowledges** → only then generate the `AA`. Any
failure on that path returns an `AE` instead. A message is therefore never
acknowledged to the sender before it is durably in Kafka (`acks=all`,
`enable.idempotence=true`), and the connector keeps no local buffer — durability
of the retry is deliberately pushed back to the sender, which holds the message
and resends on anything that is not an `AA`.

### The timeout chain

Four values have to nest, and the connector enforces that rather than leaving it
to whoever edits the environment:

```
app ACK wait  >   delivery.timeout.ms  >=  request.timeout.ms  (+ linger.ms, 0 here)
(derived)         KAFKA_DELIVERY_TIMEOUT_MS   KAFKA_REQUEST_TIMEOUT_MS

max.block.ms  =   KAFKA_ACK_TIMEOUT_MS        (how long an outage may block send() itself)
```

* The wait on the send future is **derived**: `max(KAFKA_ACK_TIMEOUT_MS,
  KAFKA_DELIVERY_TIMEOUT_MS + 2000)`. `Future.get(timeout, unit)` does **not**
  cancel the underlying send — the producer keeps retrying for its full delivery
  budget — so an app-level wait shorter than `delivery.timeout.ms` returns an
  `AE` for a record that is still in flight and can still be accepted. The
  sender then resends, and the pipeline gets two copies of a message it was told
  had failed. Deriving the wait removes that window by construction.
* The wait **trails** the delivery budget rather than equalling it. Both clocks
  start almost together (the delivery budget when `send()` appends the record,
  the app wait when `send()` returns), but the producer only expires a batch on
  its next sender-loop pass, so equal deadlines can still fire on the app side
  first. With the margin the app-level wait is a backstop against a stuck
  producer thread and should never be the deadline that actually fires.
* `delivery.timeout.ms < request.timeout.ms` is refused at startup with a
  message naming both variables, rather than degrading silently.
* **To fail faster, lower `KAFKA_DELIVERY_TIMEOUT_MS`**, not
  `KAFKA_ACK_TIMEOUT_MS` — the latter only shrinks the half of the budget the
  sender cannot see.

### The duplicate window that remains

Closing the timeout race does not make delivery exactly-once, and nothing here
can. One window is inherent to acknowledging over a network:

> The broker persists the record and acknowledges it; the `AA` is then lost on
> the way back to the sender — a socket reset, or the connector dying between
> the broker ack and the MLLP layer flushing the acknowledgment. The sender
> never sees an `AA`, holds the message, and resends it.

The resend carries the same MSH-10, so it produces the same Kafka key and lands
on the same partition — but the topics are not compacted, so **nothing at the
Kafka layer removes the duplicate**. Consumers must be idempotent; MSH-10 is the
intended dedup handle (see
[ADR 0001](docs/decisions/0001-kafka-key-msh10.md)).

The practical consequence when debugging a duplicate-count anomaly: an `AE`
proves the connector could not confirm the write, **not** that nothing reached
Kafka.

### Inbound frame-size cap

`MLLP_MAX_FRAME_BYTES` (default: `KAFKA_MAX_REQUEST_SIZE`) caps a single inbound
MLLP frame as it is read off the socket, so an oversized frame costs a bounded
amount of memory and parse time instead of one proportional to whatever the
sender chose to send. Rejections are counted by
`hl7_mllp_frames_rejected_total{reason="oversize"}`.

An oversized frame gets **no `AE`**: HAPI routes a reader failure to connection
teardown without reaching the responder, and there is no parsed message to build
an acknowledgment from anyway. The sender sees the connection drop, which under
its hold-and-retry contract is the same signal as a NAK. Messages that fit the
frame cap but still produce an over-large Kafka record do get a proper `AE`
(`record_too_large`) — so keep the cap at or below `KAFKA_MAX_REQUEST_SIZE` if
you want the largest possible share of size failures to be answered rather than
dropped.

**The operational consequence, and how to see it.** A sender holding a message
that is over the cap will retry it forever and never get an explanatory `AE`, so
the feed appears stalled. Every rejection therefore logs a `WARN` naming
`MLLP_MAX_FRAME_BYTES` (alongside HAPI's own connection-teardown line, which
carries the peer address) and increments
`hl7_mllp_frames_rejected_total{reason="oversize"}`. A non-zero rate on that
counter with no matching produce activity is a stuck oversized sender, not a
network problem.

Set `MLLP_MAX_FRAME_BYTES=0` to restore the previous, uncapped behaviour.

## Design decisions

Architecture decision records live in [`docs/decisions/`](docs/decisions/):

* [0001 — Kafka record key is MSH-10, not a patient identifier](docs/decisions/0001-kafka-key-msh10.md)

## Testing with Sample HL7 Message

Send a test HL7 message using netcat or any MLLP client:

```bash
# Example using echo and nc (Linux/Mac)
printf '\x0bMSH|^~\\&|SENDING|FACILITY|RECEIVING|FACILITY|20251019120000||ADT^A01^ADT_A01|MSG001|P|2.5\rEVN|A01|20251019120000\rPID|1||12345||DOE^JOHN||19800101|M\r\x1c\x0d' | nc localhost 2575
```

Check the Kafka topic:

```bash
# Using kafka-console-consumer
docker exec -it volcano-kafka kafka-console-consumer.sh \
  --bootstrap-server broker:29092 \
  --topic volcano.hl7.v2.adt.a01 \
  --from-beginning
```

Or view in Kafka UI at http://localhost:8080

## Architecture

1. **MLLP Listener** - Receives HL7 v2 messages on configured port
2. **Message Parsing** - HAPI library parses HL7 structure
3. **JSON Conversion** - Custom converter creates structured JSON
4. **Topic Routing** - Routes to topics based on configurable strategy:
   - Legacy: MSH-9.1 (type) + MSH-9.2 (event) for HL7 v2.x compatibility
   - Message Structure: MSH-9.3 (structure) for HL7 v2.5+ precision
5. **Kafka Publishing** - Synchronous send with timeout
6. **ACK Response** - Returns HL7 AA (success) or AE (error) to sender

## Dependencies

- HAPI 2.6.0 (`hapi-base` + `hapi-structures-v25`) - HL7 v2 parsing
- Apache Kafka Clients 4.1.1 - Kafka integration
- Gson 2.13.2 - JSON serialization
- Prometheus simpleclient 0.16.0 - metrics
- Logback 1.5.22 / SLF4J 2.0.17 - Logging

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Support

For issues and questions, see CLAUDE.md for development guidance.
