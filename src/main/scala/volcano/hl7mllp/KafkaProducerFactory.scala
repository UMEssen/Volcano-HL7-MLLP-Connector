package volcano.hl7mllp

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory

import java.util.Properties

object KafkaProducerFactory:

  private val log = LoggerFactory.getLogger(getClass)

  def create(cfg: Config): KafkaProducer[String, String] =
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cfg.kafkaBootstrap)
    props.put(ProducerConfig.CLIENT_ID_CONFIG, cfg.kafkaClientId)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

    // Reliability settings
    props.put(ProducerConfig.ACKS_CONFIG, "all")
    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    props.put(ProducerConfig.RETRIES_CONFIG, Integer.valueOf(Integer.MAX_VALUE))
    props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5")
    props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, Integer.valueOf(5000))
    props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, Integer.valueOf(Math.max(cfg.kafkaAcksTimeoutMs, 10000)))

    configureSecurityProtocol(props, cfg)
    configureSasl(props, cfg)
    configureSsl(props, cfg)

    new KafkaProducer[String, String](props)

  private def configureSecurityProtocol(props: Properties, cfg: Config): Unit =
    val securityProtocol = (cfg.kafkaSslEnabled, cfg.kafkaSaslEnabled) match
      case (true, true)   => "SASL_SSL"       // Both TLS and SASL
      case (true, false)  => "SSL"            // TLS only
      case (false, true)  => "SASL_PLAINTEXT" // SASL without TLS
      case (false, false) => "PLAINTEXT"      // No security (default)

    props.put("security.protocol", securityProtocol)
    log.info(s"Kafka security protocol: $securityProtocol")

  private def configureSasl(props: Properties, cfg: Config): Unit =
    if cfg.kafkaSaslEnabled then
      (cfg.kafkaSaslUsername, cfg.kafkaSaslPassword) match
        case (Some(username), Some(password)) =>
          props.put("sasl.mechanism", cfg.kafkaSaslMechanism)
          val jaasConfig = s"""org.apache.kafka.common.security.scram.ScramLoginModule required username="$username" password="$password";"""
          props.put("sasl.jaas.config", jaasConfig)
          log.info(s"SASL authentication enabled with mechanism ${cfg.kafkaSaslMechanism}")
        case _ =>
          log.warn("SASL enabled but username or password missing - connecting without authentication")

  private def configureSsl(props: Properties, cfg: Config): Unit =
    if cfg.kafkaSslEnabled then
      cfg.kafkaSslTruststoreLocation match
        case Some(truststorePath) =>
          props.put("ssl.truststore.location", truststorePath)
          props.put("ssl.truststore.type", cfg.kafkaSslTruststoreType)
          log.info(s"SSL enabled with truststore: $truststorePath (type: ${cfg.kafkaSslTruststoreType})")
        case None =>
          log.warn("SSL enabled but truststore location not specified - using default JVM truststore")
