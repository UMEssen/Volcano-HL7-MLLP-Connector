package volcano.hl7mllp

final case class Config(
  port: Int,
  useTls: Boolean,
  // Hard cap on one inbound MLLP frame, enforced at the wire before HAPI
  // buffers or parses it. <= 0 disables the cap (pre-cap behaviour: a frame of
  // any size is received and parsed in full before the Kafka-side size check
  // can reject it). Default: KAFKA_MAX_REQUEST_SIZE, since anything larger is
  // guaranteed to fail at the Kafka stage anyway.
  mllpMaxFrameBytes: Int = Config.DefaultMaxRequestSize,
  kafkaBootstrap: String,
  // Static topic: when set (KAFKA_TOPIC), every message from this instance is
  // produced to exactly this topic and MSH-9 is used only for key/headers/
  // metadata/logging. This is the model for a feed-based deployment: topic
  // identity comes from the feed (which the operator controls), not from the
  // message type (which the upstream can change without notice).
  topicStatic: Option[String],
  // Derive-from-MSH-9 mode (used only when topicStatic is empty):
  // {prefix}{infix}{type}.{event}.
  topicPrefix: String,
  topicInfix: String,
  kafkaClientId: String,
  // How long a Kafka outage may block the (serial) MLLP thread in send()
  // itself — this bounds max.block.ms. It is NOT the whole ACK budget; see
  // kafkaAckWaitMs.
  kafkaAcksTimeoutMs: Int,
  // Producer-internal timeouts. request.timeout.ms bounds one broker round
  // trip; delivery.timeout.ms is the producer's total retry budget for a
  // record. Kafka requires delivery.timeout.ms >= request.timeout.ms +
  // linger.ms (linger is left at the client default of 0).
  kafkaRequestTimeoutMs: Int = Config.DefaultRequestTimeoutMs,
  kafkaDeliveryTimeoutMs: Int = Config.DefaultDeliveryTimeoutMs,
  kafkaMaxRequestSize: Int,
  kafkaBufferMemory: Long,
  kafkaCompressionType: String,
  kafkaSaslEnabled: Boolean,
  kafkaSaslMechanism: String,
  kafkaSaslUsername: Option[String],
  kafkaSaslPassword: Option[String],
  kafkaSslEnabled: Boolean,
  kafkaSslTruststoreLocation: Option[String],
  kafkaSslTruststoreType: String,
  hl7Encoding: String,
  includeRaw: Boolean,
  metricsEnabled: Boolean,
  metricsPort: Int
):

  /**
   * How long the handler blocks on the send future before it gives up and
   * NAKs.
   *
   * It must be at least delivery.timeout.ms. `Future.get(timeout, unit)` does
   * not cancel the underlying send — the producer keeps retrying internally
   * for its full delivery budget — so a shorter app-level wait returns an AE
   * to the sender while the original record is still in flight and can still
   * be accepted. The sender then resends, and the pipeline gets two copies of
   * a message it was told had failed. Deriving the wait from the delivery
   * budget makes that window impossible by construction rather than by
   * matching two numbers by hand.
   *
   * To fail faster, lower KAFKA_DELIVERY_TIMEOUT_MS; that shrinks the whole
   * chain instead of only the half the sender can see.
   */
  def kafkaAckWaitMs: Int = math.max(kafkaAcksTimeoutMs, kafkaDeliveryTimeoutMs)

  /** True when kafkaAckWaitMs had to be raised above the configured ack timeout. */
  def ackWaitWasRaised: Boolean = kafkaAckWaitMs > kafkaAcksTimeoutMs

object Config:
  val DefaultRequestTimeoutMs: Int  = 5000
  val DefaultDeliveryTimeoutMs: Int = 10000
  val DefaultMaxRequestSize: Int    = 10485760 // 10 MiB

  private def env(name: String, default: => String): String =
    sys.env.getOrElse(name, default)

  def load(): Config =
    // Kafka-side record cap first: the MLLP frame cap defaults to it.
    val maxRequestSize = env("KAFKA_MAX_REQUEST_SIZE", DefaultMaxRequestSize.toString).toInt
    val ackTimeoutMs   = env("KAFKA_ACK_TIMEOUT_MS", "5000").toInt
    val cfg = Config(
      port               = env("MLLP_PORT", "2575").toInt,
      useTls             = env("MLLP_TLS", "false").toBoolean, // plain by default
      mllpMaxFrameBytes  = env("MLLP_MAX_FRAME_BYTES", maxRequestSize.toString).toInt,
      kafkaBootstrap     = env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
      // Empty / unset => derive-from-MSH-9 mode below.
      topicStatic        = sys.env.get("KAFKA_TOPIC").map(_.trim).filter(_.nonEmpty),
      topicPrefix        = sanitizePrefix(env("KAFKA_TOPIC_PREFIX", "volcano.")),
      // Inserted between prefix and {type}.{event}. Default preserves the
      // historical schema "volcano.hl7.v2.adt.a01"; set empty to bake the
      // protocol/version segments into the prefix instead. Ignored entirely
      // when KAFKA_TOPIC (topicStatic) is set.
      topicInfix         = env("KAFKA_TOPIC_INFIX", "hl7.v2."),
      kafkaClientId      = env("KAFKA_CLIENT_ID", "volcano-hl7-mllp"),
      kafkaAcksTimeoutMs = ackTimeoutMs,
      kafkaRequestTimeoutMs = env("KAFKA_REQUEST_TIMEOUT_MS", DefaultRequestTimeoutMs.toString).toInt,
      // Historically this was max(KAFKA_ACK_TIMEOUT_MS, 10000) and not
      // configurable on its own; that default is preserved exactly, and it is
      // now the knob to turn when the whole ACK budget should shrink or grow.
      kafkaDeliveryTimeoutMs = env(
        "KAFKA_DELIVERY_TIMEOUT_MS",
        math.max(ackTimeoutMs, DefaultDeliveryTimeoutMs).toString
      ).toInt,
      // Client-side cap on a single record. Default Kafka is 1 MiB, which
      // silently rejects document-bearing HL7 (MDM, ORU with embedded
      // PDFs/images) before it ever reaches the broker. Raise to match the
      // topic's max.message.bytes. The JSON envelope inflates the raw HL7
      // ~2-3x, so size headroom matters.
      kafkaMaxRequestSize  = maxRequestSize,
      kafkaBufferMemory    = env("KAFKA_BUFFER_MEMORY", "67108864").toLong,    // 64 MiB
      kafkaCompressionType = env("KAFKA_COMPRESSION_TYPE", "lz4"),
      kafkaSaslEnabled   = env("KAFKA_SASL_ENABLED", "false").toBoolean,
      kafkaSaslMechanism = env("KAFKA_SASL_MECHANISM", "SCRAM-SHA-512"),
      kafkaSaslUsername  = sys.env.get("KAFKA_SASL_USERNAME"),
      kafkaSaslPassword  = sys.env.get("KAFKA_SASL_PASSWORD"),
      kafkaSslEnabled    = env("KAFKA_SSL_ENABLED", "false").toBoolean,
      kafkaSslTruststoreLocation = sys.env.get("KAFKA_SSL_TRUSTSTORE_LOCATION"),
      kafkaSslTruststoreType = env("KAFKA_SSL_TRUSTSTORE_TYPE", "PEM"),
      hl7Encoding        = env("HL7_ENCODING", "UTF-8"),
      // Set HL7_INCLUDE_RAW=false to drop the (large) hl7_raw ER7 string from
      // the envelope when downstream consumers only need the parsed view.
      includeRaw         = env("HL7_INCLUDE_RAW", "true").toBoolean,
      metricsEnabled     = env("METRICS_ENABLED", "true").toBoolean,
      metricsPort        = env("METRICS_PORT", "9404").toInt
    )
    validateTimeouts(cfg) match
      case Right(valid) => valid
      case Left(reason) => throw new IllegalArgumentException(reason)

  /**
   * Check the producer timeout chain. Returns the reason rather than throwing
   * so it is testable without the environment; `load` turns a Left into a
   * startup failure, which is the intended behaviour — a misconfigured timeout
   * chain silently degrades ACK correctness rather than failing visibly.
   */
  def validateTimeouts(cfg: Config): Either[String, Config] =
    if cfg.kafkaAcksTimeoutMs <= 0 then
      Left(s"KAFKA_ACK_TIMEOUT_MS must be > 0 (got ${cfg.kafkaAcksTimeoutMs})")
    else if cfg.kafkaRequestTimeoutMs <= 0 then
      Left(s"KAFKA_REQUEST_TIMEOUT_MS must be > 0 (got ${cfg.kafkaRequestTimeoutMs})")
    else if cfg.kafkaDeliveryTimeoutMs < cfg.kafkaRequestTimeoutMs then
      Left(
        s"KAFKA_DELIVERY_TIMEOUT_MS (${cfg.kafkaDeliveryTimeoutMs}) must be >= " +
          s"KAFKA_REQUEST_TIMEOUT_MS (${cfg.kafkaRequestTimeoutMs}): Kafka requires " +
          "delivery.timeout.ms >= request.timeout.ms + linger.ms, and a delivery " +
          "budget below one request round trip cannot retry at all"
      )
    else Right(cfg)

  private def sanitizePrefix(p: String): String =
    val px = if p.endsWith(".") || p.endsWith("_") || p.endsWith("-") then p else p + "."
    px.replace('/', '.').replace("..", ".").toLowerCase
