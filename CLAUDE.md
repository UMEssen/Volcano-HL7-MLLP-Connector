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
- `HL7_ENCODING` - Character encoding for HL7 messages (default: UTF-8). Supports UTF-8, ISO-8859-1, windows-1252, US-ASCII
- `KAFKA_BOOTSTRAP_SERVERS` - Kafka broker addresses
- `KAFKA_TOPIC_PREFIX` - Prefix for generated topics (default: "volcano.")
- `KAFKA_TOPIC_INFIX` - Inserted between prefix and {type}.{event} or {structure} (default: "hl7.v2."). Set to "" when the prefix already encodes the protocol/version segments.
- `KAFKA_TOPIC_NAME` - Topic naming strategy: "legacy" or "message_structure" (default: "legacy")
- `KAFKA_SASL_ENABLED` - Enable SASL authentication (default: false)
- `KAFKA_SASL_MECHANISM` - SASL mechanism (default: SCRAM-SHA-512)
- `KAFKA_SASL_USERNAME` - SASL username (required if SASL enabled)
- `KAFKA_SASL_PASSWORD` - SASL password (required if SASL enabled)
- `KAFKA_SSL_ENABLED` - Enable SSL/TLS encryption (default: false)
- `KAFKA_SSL_TRUSTSTORE_LOCATION` - Path to truststore file (.pem, .jks, .p12)
- `KAFKA_SSL_TRUSTSTORE_TYPE` - Truststore format: PEM, JKS, or PKCS12 (default: PEM)

## Architecture

### Single-File Application
The entire connector logic is in `src/main/scala/volcano/hl7mllp/Main.scala` (~148 lines).

### Core Flow
1. **MLLP Listener** - HAPI HL7Service listens on configured port for incoming HL7 v2 messages
2. **Message Parsing** - Uses HAPI Terser to extract MSH segment fields (message type, trigger event, or structure)
3. **Topic Routing** - Routes to Kafka topics based on configurable routing strategy (see Routing Strategies below)
4. **JSON Conversion** - Custom converter creates structured JSON with:
   - `hl7_raw`: Original ER7 pipe-delimited format
   - `metadata`: Extracted key fields (message type, control ID, sending/receiving systems, etc.)
   - `segments`: Array of all segments with their fields
5. **Kafka Publishing** - Synchronous send with deterministic ACK/NAK response to sender
6. **ACK Response** - Sends HL7 ACK (AA) on success or AE (application error) on failure

### Routing Strategies

The connector supports two routing strategies controlled by the `KAFKA_TOPIC_NAME` environment variable:

**1. Legacy Mode (default: `KAFKA_TOPIC_NAME=legacy`)**
- **HL7 Version Support**: All HL7 v2.x versions (including pre-v2.5)
- **Fields Used**: MSH-9.1 (message type) and MSH-9.2 (trigger event)
- **Topic Format**: `{prefix}{infix}{type}.{event}`
- **Example**: `volcano.hl7.v2.adt.a01` (default infix `hl7.v2.`)
- **Fallback**: Uses "UNKNOWN" for missing fields (e.g., `volcano.hl7.v2.unknown.a01`)
- **Use Case**: Environments with older HL7 v2 messages that may not populate MSH-9.3
- **Message Key Fallback**: `{type}.{event}-{timestamp}` (e.g., `ADT.A01-1234567890`)

**2. Message Structure Mode (`KAFKA_TOPIC_NAME=message_structure`)**
- **HL7 Version Support**: HL7 v2.5 and later (MSH-9.3 is required in v2.5+)
- **Fields Used**: MSH-9.3 (message structure)
- **Topic Format**: `{prefix}{infix}{structure}`
- **Example**: `volcano.hl7.v2.adt_a01` (default infix `hl7.v2.`)
- **Fallback**: Uses "UNKNOWN" if MSH-9.3 is missing (e.g., `volcano.hl7.v2.unknown`)
- **Use Case**: Modern HL7 v2.5+ environments where MSH-9.3 is reliably populated
- **Message Key Fallback**: `{structure}-{timestamp}` (e.g., `ADT_A01-1234567890`)
- **Advantage**: More precise routing as structure field explicitly defines the message structure

### Configurable `hl7.v2.` Infix

Both strategies insert `KAFKA_TOPIC_INFIX` (default `hl7.v2.`) between the prefix and the message-derived suffix. Set it to empty when you'd rather encode the protocol/version segments inside the prefix itself:

```
KAFKA_TOPIC_PREFIX=volcano.producer.hl7.v2.cgm.medico.
KAFKA_TOPIC_INFIX=
KAFKA_TOPIC_NAME=legacy
# → volcano.producer.hl7.v2.cgm.medico.adt.a01
```

### Message Key Strategy
Uses MSH-10 (message control ID) as Kafka partition key for ordering. Fallback depends on routing strategy:
- **Legacy mode**: `{type}.{event}-{timestamp}` (e.g., `ADT.A01-1234567890`)
- **Message structure mode**: `{structure}-{timestamp}` (e.g., `ADT_A01-1234567890`)

### Reliability Design
- **Kafka Producer Config**: `acks=all`, idempotence enabled, max retries, 5 in-flight requests
- **Synchronous Send**: Waits for Kafka confirmation before ACKing to sender (max 5s timeout)
- **Graceful Shutdown**: Flushes and closes producer, stops MLLP server cleanly

### Character Encoding
Configuration in Main.scala:224-226:
- **Configurable Encoding**: Set via `HL7_ENCODING` environment variable (default: UTF-8)
- **MLLP Layer Configuration**: Uses HAPI's `MinLowerLayerProtocol.setCharset()` method
- **Supported Encodings**: UTF-8, ISO-8859-1, windows-1252, US-ASCII
- **UTF-8 Recommended**: Handles international characters (ä, ö, ü, ß, é, etc.) correctly
- **Common Issue**: If you see characters like "M�rkische" instead of "Märkische", the encoding is likely misconfigured
- **Default HAPI Behavior**: Without explicit configuration, HAPI uses US-ASCII which doesn't handle non-English characters
- **How It Works**: The charset is applied to the MLLP layer before messages are received and parsed

### Privacy & Compliance
- **Metadata-Only Logging**: Never logs HL7 message payloads (PHI protection)
- Logging configuration in `src/main/resources/logback.xml` enforces this
- Only logs structure type, message control ID, and Kafka metadata (topic, partition, offset)

### Security & Authentication

The connector supports multiple security configurations for Kafka connections:

**Security Protocol Selection:**
The connector automatically selects the appropriate security protocol based on configuration:
- `PLAINTEXT` - No security (default when both SASL and SSL are disabled)
- `SSL` - TLS encryption only (when `KAFKA_SSL_ENABLED=true` and `KAFKA_SASL_ENABLED=false`)
- `SASL_PLAINTEXT` - SASL authentication without TLS (when `KAFKA_SASL_ENABLED=true` and `KAFKA_SSL_ENABLED=false`)
- `SASL_SSL` - Both SASL authentication and TLS encryption (when both `KAFKA_SASL_ENABLED=true` and `KAFKA_SSL_ENABLED=true`)

**SASL Authentication (SCRAM-SHA-512):**
Configuration in Main.scala:92-101:
- Enable with `KAFKA_SASL_ENABLED=true`
- Supports SCRAM-SHA-512 mechanism (configurable via `KAFKA_SASL_MECHANISM`)
- Credentials provided via `KAFKA_SASL_USERNAME` and `KAFKA_SASL_PASSWORD` environment variables
- JAAS configuration is generated programmatically using `ScramLoginModule`
- Logs warning if SASL is enabled but credentials are missing

**SSL/TLS Configuration:**
Configuration in Main.scala:103-111:
- Enable with `KAFKA_SSL_ENABLED=true`
- Supports multiple truststore formats via `KAFKA_SSL_TRUSTSTORE_TYPE`:
  - **PEM** (default) - Plain-text certificate files, no password required
  - **JKS** - Java KeyStore format (may require password)
  - **PKCS12** - PKCS#12 format (may require password)
- Certificate/truststore location specified via `KAFKA_SSL_TRUSTSTORE_LOCATION`
- Certificate files should be mounted into the container at the configured path (default: `/app/certs/ca-cert.pem`)
- Logs warning if SSL is enabled but truststore location is not specified
- **Note**: PEM files are plain-text and don't use passwords. Only JKS/PKCS12 formats may require passwords (which would need to be added as a separate configuration if needed)

**Docker Certificate Mounting:**
To use a CA certificate with Docker:
1. Place your certificate file (e.g., `ca-cert.pem`) in a local directory (e.g., `./certs/`)
2. Uncomment the volumes section in `docker-compose.yml`:
   ```yaml
   volumes:
     - ./certs/ca-cert.pem:/app/certs/ca-cert.pem:ro
   ```
3. Set environment variables:
   - `KAFKA_SSL_ENABLED=true`
   - `KAFKA_SSL_TRUSTSTORE_LOCATION=/app/certs/ca-cert.pem`
   - `KAFKA_SSL_TRUSTSTORE_TYPE=PEM` (or JKS/PKCS12 if using those formats)

**Production Recommendation:**
For production deployments, use `SASL_SSL` (both SASL and SSL enabled) to ensure both authentication and encryption.

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
Topic naming depends on the configured `KAFKA_TOPIC_NAME` strategy:

**Legacy Mode (default):**
- Message type (MSH-9.1) and trigger event (MSH-9.2) combine to form the routing key
- Topics are lowercase with underscores replacing special chars within fields
- Format: `{prefix}hl7.v2.{type}.{event}` where type ADT and event A01 becomes `volcano.hl7.v2.adt.a01`
- If either field is missing, "UNKNOWN" is used (e.g., `volcano.hl7.v2.unknown.a01` or `volcano.hl7.v2.adt.unknown`)

**Message Structure Mode:**
- Message structure (MSH-9.3) forms the routing key
- Topics are lowercase with underscores replacing special chars
- Format: `{prefix}hl7.v2.{structure}` where structure ADT_A01 becomes `volcano.hl7.v2.adt_a01`
- If MSH-9.3 is missing, "UNKNOWN" is used (e.g., `volcano.hl7.v2.unknown`)

### Error Handling
- Kafka timeouts (>5s) return HL7 AE acknowledgment with "Kafka timeout" error text
- Other Kafka failures return AE with exception message (truncated to 200 chars, escaped)
- HL7 parsing errors would prevent message processing (handled by HAPI framework)

## Dependencies (build.sbt)

- `ca.uhn.hapi:hapi-base:2.6.0` - Core HL7 v2 parsing (latest stable)
- `ca.uhn.hapi:hapi-structures-v25:2.6.0` - HL7 v2.5 message structures
- `org.apache.kafka:kafka-clients:4.1.1` - Kafka producer (latest)
- `org.slf4j:slf4j-api:2.0.17` + `ch.qos.logback:logback-classic:1.5.21` - Logging (latest stable)
- `com.google.code.gson:gson:2.13.2` - JSON serialization support (latest)

### Assembly Merge Strategy
The fat JAR build uses sbt-assembly with a custom merge strategy (build.sbt:18-27):
- **META-INF/services/**: Concatenated (preserves SLF4J service provider files for Logback discovery)
- **Signature files** (*.SF, *.DSA, *.RSA): Discarded (removes JAR signatures that cause conflicts)
- **MANIFEST.MF**: Discarded (prevents manifest conflicts)
- **module-info.class**: Discarded (removes Java 9+ module descriptors)
- **Other META-INF files**: First occurrence kept

This strategy ensures SLF4J 2.x can discover the Logback implementation via ServiceLoader while avoiding common fat JAR conflicts.

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
