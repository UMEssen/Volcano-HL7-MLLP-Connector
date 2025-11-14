// Purpose: Volcano connector that listens on MLLP, parses HL7 v2 with HAPI to JSON, and writes to Kafka by message structure/type.

package volcano.hl7mllp

import ca.uhn.hl7v2.{DefaultHapiContext}
import ca.uhn.hl7v2.app.HL7Service
import ca.uhn.hl7v2.protocol.ReceivingApplication
import ca.uhn.hl7v2.model.{Message, Segment, Type, Primitive}
import ca.uhn.hl7v2.parser.{Parser, PipeParser}
import ca.uhn.hl7v2.util.Terser
import ca.uhn.hl7v2.HL7Exception
import ca.uhn.hl7v2.llp.LLPException
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.{StringSerializer}
import org.slf4j.LoggerFactory
import com.google.gson.{Gson, GsonBuilder, JsonObject, JsonArray}

import java.nio.charset.StandardCharsets
import java.util.Properties
import java.util.concurrent.TimeUnit
import scala.util.{Try, Using}
import scala.jdk.CollectionConverters._

object Main:

  private val log = LoggerFactory.getLogger(getClass)

  final case class Config(
    port: Int,
    useTls: Boolean,
    kafkaBootstrap: String,
    topicPrefix: String,
    fanOutTypeEvent: Boolean,
    kafkaClientId: String,
    kafkaAcksTimeoutMs: Int,
    kafkaSaslEnabled: Boolean,
    kafkaSaslMechanism: String,
    kafkaSaslUsername: Option[String],
    kafkaSaslPassword: Option[String]
  )
  object Config:
    def env(name: String, default: => String): String =
      sys.env.getOrElse(name, default)
    def load(): Config =
      Config(
        port               = env("MLLP_PORT", "2575").toInt,
        useTls             = env("MLLP_TLS", "false").toBoolean, // plain by default
        kafkaBootstrap     = env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        topicPrefix        = sanitizePrefix(env("KAFKA_TOPIC_PREFIX", "volcano.")),
        fanOutTypeEvent    = env("FANOUT_TYPE_EVENT", "false").toBoolean, // also publish hl7.v2.<type>.<event>
        kafkaClientId      = env("KAFKA_CLIENT_ID", "volcano-hl7-mllp"),
        kafkaAcksTimeoutMs = env("KAFKA_ACK_TIMEOUT_MS", "5000").toInt,
        kafkaSaslEnabled   = env("KAFKA_SASL_ENABLED", "false").toBoolean,
        kafkaSaslMechanism = env("KAFKA_SASL_MECHANISM", "SCRAM-SHA-512"),
        kafkaSaslUsername  = sys.env.get("KAFKA_SASL_USERNAME"),
        kafkaSaslPassword  = sys.env.get("KAFKA_SASL_PASSWORD")
      )
    private def sanitizePrefix(p: String) =
      val px = if p.endsWith(".") || p.endsWith("_") || p.endsWith("-") then p else p + "."
      px.replace('/', '.').replace("..", ".").toLowerCase

  private def kafkaProducer(cfg: Config): KafkaProducer[String,String] =
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cfg.kafkaBootstrap)
    props.put(ProducerConfig.CLIENT_ID_CONFIG, cfg.kafkaClientId)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    // reliability
    props.put(ProducerConfig.ACKS_CONFIG, "all")
    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    props.put(ProducerConfig.RETRIES_CONFIG, Integer.valueOf(Integer.MAX_VALUE))
    props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5")
    props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, Integer.valueOf(5000))
    props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, Integer.valueOf(Math.max(cfg.kafkaAcksTimeoutMs, 10000)))

    // SASL authentication
    if cfg.kafkaSaslEnabled then
      (cfg.kafkaSaslUsername, cfg.kafkaSaslPassword) match
        case (Some(username), Some(password)) =>
          props.put("security.protocol", "SASL_PLAINTEXT")
          props.put("sasl.mechanism", cfg.kafkaSaslMechanism)
          val jaasConfig = s"""org.apache.kafka.common.security.scram.ScramLoginModule required username="$username" password="$password";"""
          props.put("sasl.jaas.config", jaasConfig)
          log.info(s"SASL authentication enabled with mechanism ${cfg.kafkaSaslMechanism}")
        case _ =>
          log.warn("SASL enabled but username or password missing - connecting without authentication")

    new KafkaProducer[String,String](props)

  private def topicNames(prefix: String, terser: Terser, fanOut: Boolean): Seq[String] =
    // MSH-9.3 is HL7 v2 "message structure" (e.g., ADT_A01, ORU_R01) – most stable for routing
    val structure = Option(terser.get("/MSH-9-3")).map(_.trim).filter(_.nonEmpty)
    val typ       = Option(terser.get("/MSH-9-1")).map(_.trim).filter(_.nonEmpty) // ADT/ORU/ORM...
    val event     = Option(terser.get("/MSH-9-2")).map(_.trim).filter(_.nonEmpty) // A01/R01...
    val base = structure.orElse(typ.map(t => s"${t}_UNKNOWN")).getOrElse("UNKNOWN")
    val primary = s"${prefix}hl7.v2.${base.replace('^','_').replace('/','_').replace(' ','_')}".toLowerCase
    val extra =
      if fanOut && typ.nonEmpty && event.nonEmpty then
        Seq(s"${prefix}hl7.v2.${typ.get}.${event.get}".toLowerCase)
      else Seq.empty
    primary +: extra

  private def messageKey(terser: Terser): String =
    // Prefer MSH-10 message control ID; fallback to structure+timestamp
    val mcid = Option(terser.get("/MSH-10")).filter(s => s != null && s.nonEmpty)
    mcid.getOrElse {
      val s = Option(terser.get("/MSH-9-3")).getOrElse("UNKNOWN")
      s"${s}-${System.currentTimeMillis()}"
    }

  private def ackOk(msg: Message): Message =
    Try(msg.generateACK()).getOrElse(msg.getParser.parse("MSH|^~\\&|||||||ACK^A01|1|P|2.5\rMSA|AA|1\r"))

  private def ackErr(msg: Message, err: String): Message =
    val a = Try(msg.generateACK("AE", new HL7Exception(err))).getOrElse(
      msg.getParser.parse(s"MSH|^~\\&|||||||ACK^A01|1|P|2.5\rMSA|AE|1|${escapeHl7(err)}\r")
    )
    a
  private def escapeHl7(s:String) = s.replace("|","\\F\\").replace("\r"," ").take(200)

  // Convert HL7 Message to JSON
  // Package-private for testing
  private[hl7mllp] def messageToJson(msg: Message, pipeParser: PipeParser): String =
    val gson = new GsonBuilder().setPrettyPrinting().create()
    val json = new JsonObject()

    // Add the raw HL7 message as ER7 (pipe-delimited) format
    json.addProperty("hl7_raw", pipeParser.encode(msg))

    // Add message metadata
    val terser = new Terser(msg)
    val metadata = new JsonObject()
    metadata.addProperty("message_type", Option(terser.get("/MSH-9-1")).getOrElse(""))
    metadata.addProperty("trigger_event", Option(terser.get("/MSH-9-2")).getOrElse(""))
    metadata.addProperty("message_structure", Option(terser.get("/MSH-9-3")).getOrElse(""))
    metadata.addProperty("message_control_id", Option(terser.get("/MSH-10")).getOrElse(""))
    metadata.addProperty("sending_application", Option(terser.get("/MSH-3")).getOrElse(""))
    metadata.addProperty("sending_facility", Option(terser.get("/MSH-4")).getOrElse(""))
    metadata.addProperty("receiving_application", Option(terser.get("/MSH-5")).getOrElse(""))
    metadata.addProperty("receiving_facility", Option(terser.get("/MSH-6")).getOrElse(""))
    metadata.addProperty("message_datetime", Option(terser.get("/MSH-7")).getOrElse(""))
    metadata.addProperty("version", Option(terser.get("/MSH-12")).getOrElse(""))
    json.add("metadata", metadata)

    // Add all segments as structured data
    val segments = new JsonArray()
    val names = msg.getNames()
    for (i <- 0 until names.length) {
      val name = names(i)
      val structures = msg.getAll(name)
      for (structure <- structures) {
        structure match {
          case seg: Segment =>
            val segmentObj = new JsonObject()
            segmentObj.addProperty("segment_name", name)

            val fields = new JsonArray()
            for (fieldNum <- 1 to seg.numFields()) {
              try {
                val field = seg.getField(fieldNum)
                if (field != null && field.length > 0) {
                  for (rep <- 0 until field.length) {
                    val fieldValue = field(rep)
                    if (fieldValue != null) {
                      val fieldStr = fieldValue.toString
                      if (fieldStr != null && fieldStr.nonEmpty) {
                        val fieldObj = new JsonObject()
                        fieldObj.addProperty("field", fieldNum)
                        fieldObj.addProperty("repetition", rep)
                        fieldObj.addProperty("value", fieldStr)
                        fields.add(fieldObj)
                      }
                    }
                  }
                }
              } catch {
                case _: Exception => // Skip fields that cause errors
              }
            }
            segmentObj.add("fields", fields)
            segments.add(segmentObj)
          case _ => // Skip non-segment structures
        }
      }
    }
    json.add("segments", segments)

    gson.toJson(json)

  def main(args: Array[String]): Unit =
    val cfg = Config.load()
    log.info(s"Starting Volcano HL7 MLLP connector on port ${cfg.port}, TLS=${cfg.useTls}, Kafka=${cfg.kafkaBootstrap}, prefix='${cfg.topicPrefix}', fanOut=${cfg.fanOutTypeEvent}")

    val hapiCtx   = new DefaultHapiContext()
    val server: HL7Service = hapiCtx.newServer(cfg.port, cfg.useTls)
    val pipeParser = hapiCtx.getPipeParser()
    val producer = kafkaProducer(cfg)

    // Application handler
    val app = new ReceivingApplication[Message]:
      override def canProcess(in: Message): Boolean = true
      override def processMessage(in: Message, meta: java.util.Map[String,Object]): Message =
        // Parse quickly and avoid logging PHI
        val terser = new Terser(in)
        val key    = messageKey(terser)
        val topics = topicNames(cfg.topicPrefix, terser, cfg.fanOutTypeEvent)
        val info   =
          val t  = Option(terser.get("/MSH-9-1")).getOrElse("?")
          val ev = Option(terser.get("/MSH-9-2")).getOrElse("?")
          val st = Option(terser.get("/MSH-9-3")).getOrElse("?")
          s"$st ($t^$ev)"
        try
          val json = messageToJson(in, pipeParser) // HAPI HL7v2 → JSON (string)
          topics.foreach { topic =>
            val rec = new ProducerRecord[String,String](topic, key, json)
            // sync send so we can decide ACK vs AE deterministically
            val md: RecordMetadata = producer.send(rec).get(5, java.util.concurrent.TimeUnit.SECONDS)
            // Only log metadata, never payload
            log.debug(s"Produced to ${md.topic}@${md.partition} offset=${md.offset} key=$key struct=$info")
          }
          ackOk(in)
        catch
          case e: java.util.concurrent.TimeoutException =>
            log.warn(s"Kafka timeout; key=$key struct=$info: ${e.getMessage}")
            ackErr(in, s"Kafka timeout")
          case e: Exception =>
            log.error(s"Kafka failure; key=$key struct=$info", e)
            ackErr(in, s"Kafka error: ${Option(e.getMessage).getOrElse(e.getClass.getSimpleName)}")

    // register wildcard (any MSH-9)
    server.registerApplication("*", "*", app)
    Runtime.getRuntime.addShutdownHook(new Thread(() =>
      log.info("Shutting down...")
      Try(server.stopAndWait())
      Try(producer.flush())
      Try(producer.close(java.time.Duration.ofSeconds(5)))
      Try(hapiCtx.close())
      log.info("Stopped.")
    ))

    server.startAndWait()
    log.info(s"MLLP listener up on :${cfg.port}")