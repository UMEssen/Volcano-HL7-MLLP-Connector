# Multi-stage build for Volcano HL7 MLLP Connector

# Stage 1: Build the application using pre-built JAR
FROM eclipse-temurin:21-jdk-jammy AS builder

# Install SBT
RUN apt-get update && \
    apt-get install -y curl gnupg && \
    echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | tee /etc/apt/sources.list.d/sbt.list && \
    echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | tee /etc/apt/sources.list.d/sbt_old.list && \
    curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | apt-key add && \
    apt-get update && \
    apt-get install -y sbt && \
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
FROM eclipse-temurin:21-jre-jammy

# Create non-root user
RUN groupadd -r volcano && useradd -r -g volcano volcano

WORKDIR /app

# Copy the fat JAR with all dependencies
COPY --from=builder /build/target/scala-3.3.3/*-assembly-*.jar /app/volcano-connector.jar

# Environment variables with defaults
ENV MLLP_PORT=2575 \
    MLLP_TLS=false \
    KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
    KAFKA_TOPIC_PREFIX=volcano. \
    FANOUT_TYPE_EVENT=false \
    KAFKA_CLIENT_ID=volcano-hl7-mllp \
    KAFKA_ACK_TIMEOUT_MS=5000 \
    JAVA_OPTS="-Xmx512m -Xms256m"

# Expose MLLP port
EXPOSE 2575

# Switch to non-root user
RUN chown -R volcano:volcano /app
USER volcano

# Health check (check if port is listening)
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD timeout 1 bash -c 'cat < /dev/null > /dev/tcp/localhost/${MLLP_PORT}' || exit 1

# Run the application
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar /app/volcano-connector.jar"]
