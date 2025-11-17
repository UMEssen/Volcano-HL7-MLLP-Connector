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

- Java 11+
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
| `HL7_ENCODING` | `UTF-8` | Character encoding (UTF-8, ISO-8859-1, windows-1252, US-ASCII) |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker addresses |
| `KAFKA_TOPIC_PREFIX` | `volcano.` | Prefix for generated topic names |
| `KAFKA_TOPIC_NAME` | `legacy` | Topic naming strategy: `legacy` or `message_structure` |
| `KAFKA_CLIENT_ID` | `volcano-hl7-mllp` | Kafka client identifier |
| `KAFKA_ACK_TIMEOUT_MS` | `5000` | Kafka acknowledgment timeout |
| `JAVA_OPTS` | `-Xmx512m -Xms256m` | JVM options |

## Topic Naming & Routing Strategies

The connector supports two routing strategies controlled by the `KAFKA_TOPIC_NAME` environment variable:

### Legacy Mode (default: `KAFKA_TOPIC_NAME=legacy`)

**Best for: HL7 v2.x (all versions including pre-v2.5)**

Uses MSH-9.1 (message type) and MSH-9.2 (trigger event):
- Topic format: `{prefix}hl7.v2.{type}.{event}`
- Example: `volcano.hl7.v2.adt.a01`
- Fallback: Uses "UNKNOWN" for missing fields (e.g., `volcano.hl7.v2.unknown.a01`)
- Compatible with older HL7 messages that may not populate MSH-9.3

### Message Structure Mode (`KAFKA_TOPIC_NAME=message_structure`)

**Best for: HL7 v2.5 and later**

Uses MSH-9.3 (message structure):
- Topic format: `{prefix}hl7.v2.{structure}`
- Example: `volcano.hl7.v2.adt_a01`
- Fallback: Uses "UNKNOWN" if MSH-9.3 is missing (e.g., `volcano.hl7.v2.unknown`)
- More precise routing as MSH-9.3 explicitly defines the message structure
- Note: MSH-9.3 is required in HL7 v2.5+ but optional in earlier versions

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

- HAPI HL7 v2.3 - HL7 message parsing
- Apache Kafka Clients 3.7.0 - Kafka integration
- Gson 2.11.0 - JSON serialization
- Logback 1.5.7 - Logging

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Support

For issues and questions, see CLAUDE.md for development guidance.
