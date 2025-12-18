package volcano.hl7mllp

final case class Config(
  port: Int,
  useTls: Boolean,
  kafkaBootstrap: String,
  topicPrefix: String,
  kafkaTopicName: String,
  kafkaClientId: String,
  kafkaAcksTimeoutMs: Int,
  kafkaSaslEnabled: Boolean,
  kafkaSaslMechanism: String,
  kafkaSaslUsername: Option[String],
  kafkaSaslPassword: Option[String],
  kafkaSslEnabled: Boolean,
  kafkaSslTruststoreLocation: Option[String],
  kafkaSslTruststoreType: String,
  hl7Encoding: String
)

object Config:
  private def env(name: String, default: => String): String =
    sys.env.getOrElse(name, default)

  def load(): Config =
    Config(
      port               = env("MLLP_PORT", "2575").toInt,
      useTls             = env("MLLP_TLS", "false").toBoolean, // plain by default
      kafkaBootstrap     = env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
      topicPrefix        = sanitizePrefix(env("KAFKA_TOPIC_PREFIX", "volcano.")),
      kafkaTopicName     = env("KAFKA_TOPIC_NAME", "legacy").toLowerCase,
      kafkaClientId      = env("KAFKA_CLIENT_ID", "volcano-hl7-mllp"),
      kafkaAcksTimeoutMs = env("KAFKA_ACK_TIMEOUT_MS", "5000").toInt,
      kafkaSaslEnabled   = env("KAFKA_SASL_ENABLED", "false").toBoolean,
      kafkaSaslMechanism = env("KAFKA_SASL_MECHANISM", "SCRAM-SHA-512"),
      kafkaSaslUsername  = sys.env.get("KAFKA_SASL_USERNAME"),
      kafkaSaslPassword  = sys.env.get("KAFKA_SASL_PASSWORD"),
      kafkaSslEnabled    = env("KAFKA_SSL_ENABLED", "false").toBoolean,
      kafkaSslTruststoreLocation = sys.env.get("KAFKA_SSL_TRUSTSTORE_LOCATION"),
      kafkaSslTruststoreType = env("KAFKA_SSL_TRUSTSTORE_TYPE", "PEM"),
      hl7Encoding        = env("HL7_ENCODING", "UTF-8")
    )

  private def sanitizePrefix(p: String): String =
    val px = if p.endsWith(".") || p.endsWith("_") || p.endsWith("-") then p else p + "."
    px.replace('/', '.').replace("..", ".").toLowerCase
