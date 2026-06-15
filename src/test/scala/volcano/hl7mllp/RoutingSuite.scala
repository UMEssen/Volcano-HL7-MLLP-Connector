package volcano.hl7mllp

import ca.uhn.hl7v2.DefaultHapiContext
import ca.uhn.hl7v2.util.Terser

import scala.jdk.CollectionConverters.*

// Unit tests for routing, partition key, headers, and the JSON envelope.
// Run: sbt test
class RoutingSuite extends munit.FunSuite:

  private val ctx    = new DefaultHapiContext()
  private val parser = ctx.getPipeParser()

  private val sample =
    "MSH|^~\\&|SENDING_APP|SENDING_FAC|RECEIVING_APP|RECEIVING_FAC|20251019103000||ADT^A01^ADT_A01|MSG00001|P|2.5\r" +
      "EVN|A01|20251019103000\rPID|1||12345^^^MRN||DOE^JOHN^A||19800101|M"

  // MSH with no message control ID (MSH-10 empty) to exercise the key fallback.
  private val noControlId =
    "MSH|^~\\&|S|F|R|F|20251019103000||ORU^R01^ORU_R01||P|2.5\rPID|1||9||X^Y"

  private def cfg(topicStatic: Option[String]): Config = Config(
    port = 2575, useTls = false, kafkaBootstrap = "localhost:9092",
    topicStatic = topicStatic, topicPrefix = "volcano.", topicInfix = "hl7.v2.",
    kafkaClientId = "test", kafkaAcksTimeoutMs = 5000,
    kafkaMaxRequestSize = 10485760, kafkaBufferMemory = 67108864L, kafkaCompressionType = "lz4",
    kafkaSaslEnabled = false, kafkaSaslMechanism = "SCRAM-SHA-512",
    kafkaSaslUsername = None, kafkaSaslPassword = None,
    kafkaSslEnabled = false, kafkaSslTruststoreLocation = None, kafkaSslTruststoreType = "PEM",
    hl7Encoding = "UTF-8", includeRaw = true, metricsEnabled = false, metricsPort = 9404
  )

  private def terserOf(msg: String): Terser = new Terser(parser.parse(msg))

  test("static topic routing ignores MSH-9") {
    assertEquals(
      HL7MessageProcessor.topicName(cfg(Some("volcano.producer.hl7.v2.example.adt")), terserOf(sample)),
      "volcano.producer.hl7.v2.example.adt"
    )
  }

  test("derived topic from MSH-9.1/9.2") {
    assertEquals(HL7MessageProcessor.topicName(cfg(None), terserOf(sample)), "volcano.hl7.v2.adt.a01")
  }

  test("message key is MSH-10") {
    assertEquals(HL7MessageProcessor.messageKey(terserOf(sample)), "MSG00001")
  }

  test("message key falls back to type.event-timestamp when MSH-10 empty") {
    val key = HL7MessageProcessor.messageKey(terserOf(noControlId))
    assert(key.startsWith("ORU.R01-"), s"unexpected fallback key: $key")
  }

  test("message type is MSH-9.1") {
    assertEquals(HL7MessageProcessor.messageType(terserOf(sample)), "ADT")
  }

  test("headers carry routing metadata") {
    val h = HL7MessageProcessor.headers(terserOf(sample)).asScala.map(x => x.key -> new String(x.value)).toMap
    assertEquals(h.get("hl7.message_type"), Some("ADT"))
    assertEquals(h.get("hl7.trigger_event"), Some("A01"))
    assertEquals(h.get("hl7.message_control_id"), Some("MSG00001"))
    assertEquals(h.get("content_type"), Some("application/json"))
    assertEquals(h.get("schema_version"), Some(HL7ToJsonConverter.SchemaVersion))
  }

  test("envelope is compact and versioned") {
    val json = HL7ToJsonConverter.convert(parser.parse(sample), parser)
    assert(json.contains("\"schema_version\":\"1.0\""))
    assert(json.contains("\"hl7_raw\""))
    assert(!json.contains("\n  "), "envelope must be compact (no pretty-printing)")
  }

  test("includeRaw=false drops hl7_raw") {
    val json = HL7ToJsonConverter.convert(parser.parse(sample), parser, includeRaw = false)
    assert(!json.contains("\"hl7_raw\""))
  }
