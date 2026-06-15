# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Volcano connector that listens for HL7 v2 messages over MLLP (Minimum Lower Layer Protocol), parses them to JSON using HAPI, and publishes to Kafka topics based on message structure/type. This is a healthcare data integration component written in Scala 3.

**Key Technologies:**
- Scala 3.3.7
- HAPI (HL7 v2 parser library)
- Apache Kafka clients
- Prometheus simpleclient (metrics)
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
- `HL7_ENCODING` - Character encoding for HL7 messages (default: UTF-8). Supports UTF-8, ISO-8859-1, windows-1252, US-ASCII. **MSH-18 (declared charset) is ignored** — match this to what the sender emits.
- `HL7_INCLUDE_RAW` - Include the raw ER7 string in the JSON envelope (default: true)
- `KAFKA_BOOTSTRAP_SERVERS` - Kafka broker addresses
- `KAFKA_TOPIC` - **Static topic** (default: unset). When set, every message routes here and MSH-9 is used only for key/headers/metadata. Preferred for feed-based deployments. When unset, the derived `{prefix}{infix}{type}.{event}` scheme below applies.
- `KAFKA_TOPIC_PREFIX` - Prefix for derived topics (default: "volcano.")
- `KAFKA_TOPIC_INFIX` - Inserted between prefix and {type}.{event} (default: "hl7.v2."). Set to "" when the prefix already encodes the protocol/version segments. Ignored when `KAFKA_TOPIC` is set.
- `KAFKA_MAX_REQUEST_SIZE` - Client-side max record size in bytes (default: 10485760 / 10 MiB). Must be ≥ the topic's `max.message.bytes`; the Kafka default of 1 MiB silently NAKs document-bearing HL7.
- `KAFKA_BUFFER_MEMORY` - Producer buffer in bytes (default: 67108864 / 64 MiB)
- `KAFKA_COMPRESSION_TYPE` - Producer compression (default: lz4)
- `METRICS_ENABLED` - Expose Prometheus metrics + health (default: true)
- `METRICS_PORT` - Port for `/metrics` and `/healthz` (default: 9404)
- `KAFKA_SASL_ENABLED` - Enable SASL authentication (default: false)
- `KAFKA_SASL_MECHANISM` - SASL mechanism (default: SCRAM-SHA-512)
- `KAFKA_SASL_USERNAME` - SASL username (required if SASL enabled)
- `KAFKA_SASL_PASSWORD` - SASL password (required if SASL enabled)
- `KAFKA_SSL_ENABLED` - Enable SSL/TLS encryption (default: false)
- `KAFKA_SSL_TRUSTSTORE_LOCATION` - Path to truststore file (.pem, .jks, .p12)
- `KAFKA_SSL_TRUSTSTORE_TYPE` - Truststore format: PEM, JKS, or PKCS12 (default: PEM)

## Architecture

### Module Layout
The connector is split into focused single-responsibility files under `src/main/scala/volcano/hl7mllp/`:
- `Main.scala` — wiring: HAPI context, MLLP server, the `ReceivingApplication` handler, shutdown hook.
- `Config.scala` — environment-variable configuration (`Config.load()`).
- `KafkaProducerFactory.scala` — builds the producer (reliability, sizing, SASL/SSL).
- `HL7MessageProcessor.scala` — topic resolution (static vs derived), partition key, Kafka headers.
- `HL7ToJsonConverter.scala` — HAPI message → JSON envelope (compact, `schema_version`).
- `HL7AckGenerator.scala` — AA/AE acknowledgments.
- `Metrics.scala` — Prometheus registry + embedded `/metrics` and `/healthz` HTTP server.

### Core Flow
1. **MLLP Listener** - HAPI HL7Service listens on configured port for incoming HL7 v2 messages
2. **Message Parsing** - Uses HAPI Terser to extract MSH segment fields (message type, trigger event, or structure)
3. **Topic Routing** - Static (`KAFKA_TOPIC`) or derived from MSH-9 (see Topic Naming below)
4. **JSON Conversion** - Custom converter creates structured JSON with:
   - `hl7_raw`: Original ER7 pipe-delimited format
   - `metadata`: Extracted key fields (message type, control ID, sending/receiving systems, etc.)
   - `segments`: Array of all segments with their fields
5. **Kafka Publishing** - Synchronous send with deterministic ACK/NAK response to sender
6. **ACK Response** - Sends HL7 ACK (AA) on success or AE (application error) on failure

### Topic Naming

Two modes, resolved in `HL7MessageProcessor.topicName`:

**1. Static (preferred for feed-based deployments).** Set `KAFKA_TOPIC` and every message from this instance is produced to that exact topic. MSH-9 never touches routing — so even a message too malformed to parse MSH-9 still lands in the right topic. This is the model for a feed-based deployment: one connector instance per upstream feed, topic identity derived from the *feed* (which the operator controls), not the message type (which the upstream can change without notice). The message type/event still travel in the Kafka headers and JSON payload.

**2. Derived (fallback when `KAFKA_TOPIC` is unset).** `{prefix}{infix}{type}.{event}` where:
- `{type}` = MSH-9.1 (message type, e.g. ADT, ORU, ORM)
- `{event}` = MSH-9.2 (trigger event, e.g. A01, R01, O01)
- `{prefix}` = `KAFKA_TOPIC_PREFIX` (default `volcano.`)
- `{infix}` = `KAFKA_TOPIC_INFIX` (default `hl7.v2.`; set to empty to suppress)

**Why MSH-9.1/9.2 only and not MSH-9.3?** Both fields are *mandatory in every HL7 v2.x version* (including pre-2.5), so a single code path covers all senders. MSH-9.3 (message structure) is only required from v2.5 onward and would force a sender-version branch for marginal benefit.

**Examples:**

| Mode | Config | Resulting topic for ADT^A01 |
|------|--------|------------------------------|
| Static | `KAFKA_TOPIC=volcano.producer.hl7.v2.example.adt` | `volcano.producer.hl7.v2.example.adt` |
| Derived | `KAFKA_TOPIC_PREFIX=volcano.`, `KAFKA_TOPIC_INFIX=hl7.v2.` | `volcano.hl7.v2.adt.a01` |

**Derived-mode fallback for missing MSH-9 fields:** `UNKNOWN` (e.g. `prefix.infix.unknown.a01`).

### Kafka Headers
Every record carries headers so a downstream router can dispatch/dedupe without deserializing the body (`HL7MessageProcessor.headers`): `hl7.message_type`, `hl7.trigger_event`, `hl7.message_structure`, `hl7.message_control_id`, `hl7.version`, `hl7.sending_application`, `content_type=application/json`, `schema_version`.

### Message Key Strategy
Uses MSH-10 (message control ID) as the Kafka partition key. **Deliberately not the patient ID** — the key is stored in cleartext in the partition log and printed by Kafka tooling, so a raw patient identifier there would be a PHI leak (and would surface in this connector's own log lines). MSH-10 is unique-per-message and non-PHI, and doubles as the consumer **dedup key** for the at-least-once resend an ack-timeout can cause. Fallback when MSH-10 is empty: `{type}.{event}-{timestamp}`.

### Reliability Design
- **Kafka Producer Config**: `acks=all`, idempotence enabled, max retries, 5 in-flight requests
- **Message sizing**: `max.request.size`/`buffer.memory` raised above the 1 MiB client default (document feeds), `compression.type=lz4`, `max.block.ms` bounded to the ack timeout so a Kafka outage fails fast instead of stalling the serial MLLP thread
- **Synchronous Send**: Waits for Kafka confirmation before ACKing to sender (timeout = `KAFKA_ACK_TIMEOUT_MS`); on any failure returns AE so the upstream sender buffers and retries — durability is intentionally pushed back to the sender
- **Graceful Shutdown**: Flushes and closes producer, stops MLLP server + metrics server cleanly

### Observability (Metrics)
`Metrics.scala` runs a Prometheus exporter on `METRICS_PORT` (default 9404): `/metrics` (text format) and `/healthz`. App metrics: `hl7_messages_received_total{message_type}`, `hl7_messages_processed_total{result=ack|nak}`, `hl7_kafka_produced_total{topic}`, `hl7_kafka_produce_failures_total{reason=timeout|record_too_large|hl7_parse|other}`, `hl7_kafka_produce_duration_seconds` (histogram), `hl7_messages_in_flight`, plus JVM/process metrics. **Why metrics, not probes:** the k8s liveness/readiness probes are TCP-on-MLLP and stay green during a Kafka outage (intentional — keep accepting so the sender buffers). Pipeline health is only visible via the produce-failure metric, so alert on that.

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
Single naming scheme — see the "Topic Naming" section above for the full reference.
- Message type (MSH-9.1) and trigger event (MSH-9.2) combine to form the routing key.
- Topic format: `{KAFKA_TOPIC_PREFIX}{KAFKA_TOPIC_INFIX}{type}.{event}` (lowercased, special chars `^/<space>` replaced with `_`).
- Default: `volcano.hl7.v2.adt.a01`.
- Missing fields → `UNKNOWN` (e.g. `volcano.hl7.v2.unknown.a01`).

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

- **`RoutingSuite.scala`** — MUnit unit tests for topic resolution (static/derived), partition key + fallback, Kafka headers, and the JSON envelope. Run with `sbt test`.
- **`JsonConversionTest.scala`** — a `runMain` smoke test that parses a sample message and prints/asserts the JSON envelope. Run with `sbt "Test/runMain volcano.hl7mllp.JsonConversionTest"`.

CI (`.github/workflows/ci.yml`) runs both, plus: hadolint, a CycloneDX **SBOM** (Syft) of the image, **Trivy** scan with SARIF upload to the Security tab, sbt **dependency-graph submission** (feeds Dependabot/GitHub Advisory alerts), and **dependency-review** on PRs. Third-party actions are pinned to commit SHAs (Dependabot bumps them).

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
