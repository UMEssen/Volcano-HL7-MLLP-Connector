package volcano.hl7mllp

import ca.uhn.hl7v2.util.Terser
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.internals.RecordHeader

import java.nio.charset.StandardCharsets.UTF_8

object HL7MessageProcessor:

  private def sanitize(s: String): String =
    s.replace('^', '_').replace('/', '_').replace(' ', '_')

  private def field(terser: Terser, path: String): String =
    Option(terser.get(path)).map(_.trim).filter(_.nonEmpty).getOrElse("UNKNOWN")

  // Resolve the destination topic.
  //   - Static mode (KAFKA_TOPIC set): the feed owns the topic; MSH-9 never
  //     touches routing, so even a message too malformed to read MSH-9 still
  //     lands in the right topic.
  //   - Derive mode: {prefix}{infix}{type}.{event} from MSH-9.1 / MSH-9.2.
  def topicName(cfg: Config, terser: Terser): String =
    cfg.topicStatic match
      case Some(topic) => topic
      case None =>
        val typ   = sanitize(field(terser, "/MSH-9-1"))
        val event = sanitize(field(terser, "/MSH-9-2"))
        s"${cfg.topicPrefix}${cfg.topicInfix}${typ}.${event}".toLowerCase

  // Message type (MSH-9.1) for the low-cardinality metric label. Trigger event
  // is intentionally NOT a label — the upstream can introduce new events
  // freely, and unbounded label values are a metrics-cardinality footgun. The
  // event still travels in the headers and payload.
  def messageType(terser: Terser): String =
    field(terser, "/MSH-9-1")

  // Kafka record headers so a downstream router can dispatch / dedupe without
  // deserializing the JSON body. Only non-empty fields are added.
  def headers(terser: Terser): java.util.List[Header] =
    val hs = new java.util.ArrayList[Header]()
    def add(key: String, path: String): Unit =
      Option(terser.get(path)).map(_.trim).filter(_.nonEmpty).foreach { v =>
        hs.add(new RecordHeader(key, v.getBytes(UTF_8)))
      }
    add("hl7.message_type", "/MSH-9-1")
    add("hl7.trigger_event", "/MSH-9-2")
    add("hl7.message_structure", "/MSH-9-3")
    add("hl7.message_control_id", "/MSH-10")
    add("hl7.version", "/MSH-12")
    add("hl7.sending_application", "/MSH-3")
    hs.add(new RecordHeader("content_type", "application/json".getBytes(UTF_8)))
    hs.add(new RecordHeader("schema_version", HL7ToJsonConverter.SchemaVersion.getBytes(UTF_8)))
    hs

  // Kafka partition key = MSH-10 (message control ID). Deliberately NOT the
  // patient identifier: the key is stored in cleartext in the partition log
  // and printed by Kafka tooling, so a raw patient ID there would be a PHI
  // leak (and would also show up in this connector's own log lines). MSH-10 is
  // unique-per-message and non-PHI, so it doubles as the consumer dedup key
  // for the at-least-once resend that an ack-timeout can cause. Fallback keeps
  // same-kind messages grouped on retries.
  def messageKey(terser: Terser): String =
    val mcid = Option(terser.get("/MSH-10")).filter(s => s != null && s.nonEmpty)
    mcid.getOrElse(s"${field(terser, "/MSH-9-1")}.${field(terser, "/MSH-9-2")}-${System.currentTimeMillis()}")

  def messageInfo(terser: Terser): String =
    val t  = Option(terser.get("/MSH-9-1")).getOrElse("?")
    val ev = Option(terser.get("/MSH-9-2")).getOrElse("?")
    val st = Option(terser.get("/MSH-9-3")).getOrElse("?")
    s"$st ($t^$ev)"
