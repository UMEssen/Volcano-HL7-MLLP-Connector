package volcano.hl7mllp

import ca.uhn.hl7v2.DefaultHapiContext
import ca.uhn.hl7v2.util.Terser

import scala.jdk.CollectionConverters.*

// Smoke test (no assertion framework — fails loud via require()). Run in CI:
//   sbt "Test/runMain volcano.hl7mllp.JsonConversionTest"
object JsonConversionTest:

  // A throwaway Config for routing/header checks. Static topic mode mirrors the
  // feed-based deployment.
  private def testConfig(topicStatic: Option[String]): Config = Config(
    port = 2575, useTls = false, kafkaBootstrap = "localhost:9092",
    topicStatic = topicStatic, topicPrefix = "volcano.", topicInfix = "hl7.v2.",
    kafkaClientId = "test", kafkaAcksTimeoutMs = 5000,
    kafkaMaxRequestSize = 10485760, kafkaBufferMemory = 67108864L, kafkaCompressionType = "lz4",
    kafkaSaslEnabled = false, kafkaSaslMechanism = "SCRAM-SHA-512",
    kafkaSaslUsername = None, kafkaSaslPassword = None,
    kafkaSslEnabled = false, kafkaSslTruststoreLocation = None, kafkaSslTruststoreType = "PEM",
    hl7Encoding = "UTF-8", includeRaw = true, metricsEnabled = false, metricsPort = 9404
  )

  def main(args: Array[String]): Unit =
    // Sample HL7 ADT message (with \r separator as required by HL7)
    val sampleHL7 = "MSH|^~\\&|SENDING_APP|SENDING_FAC|RECEIVING_APP|RECEIVING_FAC|20251019103000||ADT^A01^ADT_A01|MSG00001|P|2.5\rEVN|A01|20251019103000\rPID|1||12345^^^MRN||DOE^JOHN^A||19800101|M"

    val hapiCtx = new DefaultHapiContext()
    val pipeParser = hapiCtx.getPipeParser()
    val msg = pipeParser.parse(sampleHL7)

    // --- Envelope ---
    val json = HL7ToJsonConverter.convert(msg, pipeParser)
    println("=" * 80)
    println("JSON OUTPUT:")
    println("=" * 80)
    println(json)
    println("=" * 80)
    require(json.contains("\"schema_version\":\"1.0\""), "envelope must carry schema_version")
    require(json.contains("\"hl7_raw\""), "default convert should include hl7_raw")
    require(!json.contains("\n  "), "envelope must be compact (no pretty-printing)")

    val noRaw = HL7ToJsonConverter.convert(msg, pipeParser, includeRaw = false)
    require(!noRaw.contains("\"hl7_raw\""), "includeRaw=false must drop hl7_raw")

    // --- Routing ---
    val terser = new Terser(msg)
    val staticTopic = HL7MessageProcessor.topicName(testConfig(Some("volcano.producer.hl7.v2.example.adt")), terser)
    require(staticTopic == "volcano.producer.hl7.v2.example.adt", s"static topic wrong: $staticTopic")
    val derivedTopic = HL7MessageProcessor.topicName(testConfig(None), terser)
    require(derivedTopic == "volcano.hl7.v2.adt.a01", s"derived topic wrong: $derivedTopic")

    // --- Key / type / headers ---
    require(HL7MessageProcessor.messageKey(terser) == "MSG00001", "key should be MSH-10")
    require(HL7MessageProcessor.messageType(terser) == "ADT", "message type should be MSH-9.1")
    val headers = HL7MessageProcessor.headers(terser).asScala.map(h => h.key -> new String(h.value)).toMap
    require(headers.get("hl7.message_type").contains("ADT"), "header hl7.message_type")
    require(headers.get("hl7.trigger_event").contains("A01"), "header hl7.trigger_event")
    require(headers.get("hl7.message_control_id").contains("MSG00001"), "header hl7.message_control_id")
    require(headers.get("content_type").contains("application/json"), "header content_type")

    println("All smoke-test assertions passed.")
    hapiCtx.close()
