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
| `KAFKA_ACK_TIMEOUT_MS` | `5000` | Kafka acknowledgment timeout (also bounds `max.block.ms`) |
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

Key metrics: `hl7_messages_received_total{message_type}`, `hl7_messages_processed_total{result}`, `hl7_kafka_produced_total{topic}`, `hl7_kafka_produce_failures_total{reason}`, `hl7_kafka_produce_duration_seconds`, `hl7_messages_in_flight`, plus JVM/process metrics. The MLLP TCP probe stays healthy during a Kafka outage by design (so the sender buffers), so **alert on `hl7_kafka_produce_failures_total`** rather than on pod health.

## JSON Output Format

```json
{
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
        {"field": 3, "repetition": 0, "value": "HD[SENDING_APP]"}
      ]
    }
  ]
}
```

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
