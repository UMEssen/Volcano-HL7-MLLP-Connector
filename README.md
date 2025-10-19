# Volcano HL7 MLLP Connector

A high-performance HL7 v2 MLLP listener that converts healthcare messages to JSON and publishes them to Apache Kafka.

## Features

- **MLLP Protocol Support** - Listens for HL7 v2 messages over MLLP (Minimum Lower Layer Protocol)
- **JSON Conversion** - Converts HL7 messages to structured JSON with metadata extraction
- **Kafka Integration** - Publishes messages to Kafka topics based on message structure
- **Reliable Delivery** - Synchronous sends with deterministic ACK/NAK responses
- **Privacy-First** - Metadata-only logging (no PHI in logs)
- **Topic Routing** - Automatic routing based on MSH-9.3 message structure field

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
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker addresses |
| `KAFKA_TOPIC_PREFIX` | `volcano.` | Prefix for generated topic names |
| `FANOUT_TYPE_EVENT` | `false` | Also publish to `{prefix}hl7.v2.{type}.{event}` topics |
| `KAFKA_CLIENT_ID` | `volcano-hl7-mllp` | Kafka client identifier |
| `KAFKA_ACK_TIMEOUT_MS` | `5000` | Kafka acknowledgment timeout |
| `JAVA_OPTS` | `-Xmx512m -Xms256m` | JVM options |

## Topic Naming

Messages are routed to Kafka topics based on the HL7 message structure (MSH-9.3 field):

- Primary topic: `{prefix}hl7.v2.{structure}` (e.g., `volcano.hl7.v2.adt_a01`)
- Optional fanout: `{prefix}hl7.v2.{type}.{event}` (e.g., `volcano.hl7.v2.adt.a01`) when `FANOUT_TYPE_EVENT=true`

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
  --topic volcano.hl7.v2.adt_a01 \
  --from-beginning
```

Or view in Kafka UI at http://localhost:8080

## Architecture

1. **MLLP Listener** - Receives HL7 v2 messages on configured port
2. **Message Parsing** - HAPI library parses HL7 structure
3. **JSON Conversion** - Custom converter creates structured JSON
4. **Topic Routing** - Routes to topics based on message structure (MSH-9.3)
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
