# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Volcano connector that listens for HL7 v2 messages over MLLP (Minimum Lower Layer Protocol), parses them to JSON using HAPI, and publishes to Kafka topics based on message structure/type. This is a healthcare data integration component written in Scala 3.

**Key Technologies:**
- Scala 3.3.3
- HAPI (HL7 v2 parser library)
- Apache Kafka clients
- SBT build system
- Logback for logging

## Build and Development Commands

### Building the Project
```bash
sbt compile          # Compile the project
sbt run             # Run the connector
sbt clean           # Clean build artifacts
sbt package         # Create JAR package
```

### Docker Commands
```bash
# Build Docker image
docker build -t volcano-hl7-mllp:latest .

# Run with docker-compose (includes Kafka + Kafka UI)
docker-compose up -d

# View logs
docker-compose logs -f volcano-connector

# Stop all services
docker-compose down
```

### Running the Connector
The connector is configured via environment variables defined in `.env` (for local dev) or docker-compose.yml (for containers):
```bash
sbt run              # Local development
# or
docker-compose up    # Containerized with Kafka
```

Key environment variables (see `.env.example` or `docker-compose.yml`):
- `MLLP_PORT` - Port to listen on (default: 2575)
- `MLLP_TLS` - Enable TLS (default: false)
- `KAFKA_BOOTSTRAP_SERVERS` - Kafka broker addresses
- `KAFKA_TOPIC_PREFIX` - Prefix for generated topics (default: "volcano.")
- `FANOUT_TYPE_EVENT` - Also publish to type.event topics (default: false)
- `KAFKA_SASL_ENABLED` - Enable SASL authentication (default: false)
- `KAFKA_SASL_MECHANISM` - SASL mechanism (default: SCRAM-SHA-512)
- `KAFKA_SASL_USERNAME` - SASL username (required if SASL enabled)
- `KAFKA_SASL_PASSWORD` - SASL password (required if SASL enabled)

## Architecture

### Single-File Application
The entire connector logic is in `src/main/scala/volcano/hl7mllp/Main.scala` (~148 lines).

### Core Flow
1. **MLLP Listener** - HAPI HL7Service listens on configured port for incoming HL7 v2 messages
2. **Message Parsing** - Uses HAPI Terser to extract MSH segment fields (message structure, type, event)
3. **Topic Routing** - Routes to Kafka topics based on MSH-9.3 (message structure) like `adt_a01`, `oru_r01`
   - Primary topic: `{prefix}hl7.v2.{structure}` (e.g., `volcano.hl7.v2.adt_a01`)
   - Optional fanout: `{prefix}hl7.v2.{type}.{event}` if `FANOUT_TYPE_EVENT=true`
4. **JSON Conversion** - Custom converter creates structured JSON with:
   - `hl7_raw`: Original ER7 pipe-delimited format
   - `metadata`: Extracted key fields (message type, control ID, sending/receiving systems, etc.)
   - `segments`: Array of all segments with their fields
5. **Kafka Publishing** - Synchronous send with deterministic ACK/NAK response to sender
6. **ACK Response** - Sends HL7 ACK (AA) on success or AE (application error) on failure

### Message Key Strategy
Uses MSH-10 (message control ID) as Kafka partition key for ordering. Falls back to `{structure}-{timestamp}` if missing.

### Reliability Design
- **Kafka Producer Config**: `acks=all`, idempotence enabled, max retries, 5 in-flight requests
- **Synchronous Send**: Waits for Kafka confirmation before ACKing to sender (max 5s timeout)
- **Graceful Shutdown**: Flushes and closes producer, stops MLLP server cleanly

### Privacy & Compliance
- **Metadata-Only Logging**: Never logs HL7 message payloads (PHI protection)
- Logging configuration in `src/main/resources/logback.xml` enforces this
- Only logs structure type, message control ID, and Kafka metadata (topic, partition, offset)

### Security & Authentication

**SASL SCRAM-SHA-512 Authentication:**
The connector supports SASL authentication for secured Kafka clusters using SCRAM-SHA-512 mechanism.

Configuration in Main.scala:76-87:
- When `KAFKA_SASL_ENABLED=true`, the producer configures SASL authentication
- Uses `SASL_PLAINTEXT` security protocol (SASL over plaintext connection)
- Supports SCRAM-SHA-512 mechanism (configurable via `KAFKA_SASL_MECHANISM`)
- Credentials provided via `KAFKA_SASL_USERNAME` and `KAFKA_SASL_PASSWORD` environment variables
- JAAS configuration is generated programmatically using `ScramLoginModule`
- Logs warning if SASL is enabled but credentials are missing

**For production use with TLS:**
To use SASL with encrypted connections, modify the security protocol from `SASL_PLAINTEXT` to `SASL_SSL` and configure SSL properties as needed.

## Code Patterns

### JSON Conversion Strategy
Since HAPI 2.3 doesn't include a native JSON parser, this project implements a custom converter that:
- Preserves the original ER7 format in `hl7_raw` field
- Extracts metadata into a structured object for easy querying
- Converts all segments and fields into JSON arrays
- Uses Gson for JSON serialization with pretty printing

The `messageToJson` function in Main.scala:215 walks the HAPI message structure and builds a comprehensive JSON representation suitable for downstream processing.

### Terser for Field Extraction
HAPI's `Terser` provides XPath-like access to HL7 fields:
```scala
terser.get("/MSH-9-3")  // Message structure (ADT_A01, ORU_R01, etc.)
terser.get("/MSH-9-1")  // Message type (ADT, ORU, ORM, etc.)
terser.get("/MSH-9-2")  // Trigger event (A01, R01, etc.)
terser.get("/MSH-10")   // Message control ID
```

### Topic Naming Convention
- Message structure (MSH-9.3) is the primary routing key
- Topics are lowercase with underscores replacing special chars
- Format: `{prefix}hl7.v2.{structure}` where structure like ADT_A01 becomes `adt_a01`

### Error Handling
- Kafka timeouts (>5s) return HL7 AE acknowledgment with "Kafka timeout" error text
- Other Kafka failures return AE with exception message (truncated to 200 chars, escaped)
- HL7 parsing errors would prevent message processing (handled by HAPI framework)

## Dependencies (build.sbt)

- `ca.uhn.hapi:hapi-base:2.3` - Core HL7 v2 parsing
- `ca.uhn.hapi:hapi-structures-v25:2.3` - HL7 v2.5 message structures
- `org.apache.kafka:kafka-clients:3.7.0` - Kafka producer
- `org.slf4j:slf4j-api:2.0.13` + `ch.qos.logback:logback-classic:1.5.7` - Logging
- `com.google.gson:gson:2.11.0` - JSON serialization support

## Testing Considerations

A simple test exists in `src/test/scala/volcano/hl7mllp/JsonConversionTest.scala` to verify JSON output.

Run the test:
```bash
sbt "Test/runMain volcano.hl7mllp.JsonConversionTest"
```

When adding more tests:
- Mock HL7Service and KafkaProducer for unit testing
- Test topic routing logic with various MSH-9 field combinations
- Test ACK/NAK generation for success/failure scenarios
- Test JSON conversion with different HL7 message types
- Integration tests would require test Kafka cluster and HL7 test messages
- Consider property-based testing for HL7 field sanitization logic

## HL7 v2 Context

HL7 v2 is a healthcare messaging standard for exchanging clinical/administrative data. Key concepts:
- **MLLP**: Framing protocol (wraps messages with `0x0B` start, `0x1C0x0D` end bytes)
- **MSH segment**: Message header containing routing metadata
- **Message structure**: Defines which segments appear (e.g., ADT_A01 has MSH, EVN, PID, PV1...)
- **ACK messages**: Application acknowledgments sent back to sender (AA=success, AE=error)
