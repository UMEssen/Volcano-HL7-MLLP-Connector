# Multi-stage build for Volcano HL7 MLLP Connector

# Stage 1: Build the application using pre-built JAR
FROM eclipse-temurin:25-jdk-noble AS builder

# Pipefail so the `curl … | apt-key add` chain below fails loud on a curl error.
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

# Install SBT
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl gnupg && \
    echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | tee /etc/apt/sources.list.d/sbt.list && \
    echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | tee /etc/apt/sources.list.d/sbt_old.list && \
    curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | apt-key add - && \
    apt-get update && \
    apt-get install -y --no-install-recommends sbt && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Copy build files
COPY build.sbt .
COPY project ./project

# Download dependencies (cached layer)
RUN sbt update

# Copy source code
COPY src ./src

# Build fat JAR with dependencies using sbt-assembly
RUN sbt assembly

# Stage 2: Runtime image
FROM eclipse-temurin:25-jre-noble

# Patch OS packages for known-fixable CVEs (e.g. openssl/libssl) that the base
# image layer hasn't picked up yet — the Trivy gate in CI enforces this.
# hadolint DL3005 (avoid apt-get upgrade) is intentionally ignored: for a
# regularly-rebuilt image, patching the base beats shipping a stale layer.
# hadolint ignore=DL3005
RUN apt-get update && \
    apt-get upgrade -y && \
    rm -rf /var/lib/apt/lists/*

# Create non-root user and directories
RUN groupadd -r volcano && useradd -r -g volcano volcano && \
    mkdir -p /app/certs

WORKDIR /app

# Copy the fat JAR with all dependencies
COPY --from=builder /build/target/scala-*/*-assembly-*.jar /app/volcano-connector.jar

# Environment variables with defaults
ENV MLLP_PORT=2575 \
    MLLP_TLS=false \
    HL7_ENCODING=UTF-8 \
    HL7_INCLUDE_RAW=true \
    KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
    KAFKA_TOPIC="" \
    KAFKA_TOPIC_PREFIX=volcano. \
    KAFKA_TOPIC_INFIX=hl7.v2. \
    KAFKA_CLIENT_ID=volcano-hl7-mllp \
    KAFKA_ACK_TIMEOUT_MS=5000 \
    KAFKA_MAX_REQUEST_SIZE=10485760 \
    KAFKA_BUFFER_MEMORY=67108864 \
    KAFKA_COMPRESSION_TYPE=lz4 \
    KAFKA_SASL_ENABLED=false \
    KAFKA_SASL_MECHANISM=SCRAM-SHA-512 \
    KAFKA_SSL_ENABLED=false \
    KAFKA_SSL_TRUSTSTORE_LOCATION=/app/certs/ca-cert.pem \
    KAFKA_SSL_TRUSTSTORE_TYPE=PEM \
    METRICS_ENABLED=true \
    METRICS_PORT=9404 \
    JAVA_OPTS="-Xmx512m -Xms256m"

# Expose MLLP port + Prometheus metrics port
EXPOSE 2575 9404

# Switch to non-root user
RUN chown -R volcano:volcano /app && \
    chmod 755 /app/certs
USER volcano

# Health check (check if port is listening)
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD timeout 1 bash -c 'cat < /dev/null > /dev/tcp/localhost/${MLLP_PORT}' || exit 1

# Run the application
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar /app/volcano-connector.jar"]
