package volcano.hl7mllp

final case class Config(
  port: Int,
  useTls: Boolean,
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
  kafkaAcksTimeoutMs: Int,
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
)

object Config:
  private def env(name: String, default: => String): String =
    sys.env.getOrElse(name, default)

  def load(): Config =
    Config(
      port               = env("MLLP_PORT", "2575").toInt,
      useTls             = env("MLLP_TLS", "false").toBoolean, // plain by default
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
      kafkaAcksTimeoutMs = env("KAFKA_ACK_TIMEOUT_MS", "5000").toInt,
      // Client-side cap on a single record. Default Kafka is 1 MiB, which
      // silently rejects document-bearing HL7 (MDM, ORU with embedded
      // PDFs/images) before it ever reaches the broker. Raise to match the
      // topic's max.message.bytes. The JSON envelope inflates the raw HL7
      // ~2-3x, so size headroom matters.
      kafkaMaxRequestSize  = env("KAFKA_MAX_REQUEST_SIZE", "10485760").toInt,  // 10 MiB
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

  private def sanitizePrefix(p: String): String =
    val px = if p.endsWith(".") || p.endsWith("_") || p.endsWith("-") then p else p + "."
    px.replace('/', '.').replace("..", ".").toLowerCase
