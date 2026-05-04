package volcano.hl7mllp

import ca.uhn.hl7v2.util.Terser

object HL7MessageProcessor:

  private def sanitize(s: String): String =
    s.replace('^', '_').replace('/', '_').replace(' ', '_')

  private def field(terser: Terser, path: String): String =
    Option(terser.get(path)).map(_.trim).filter(_.nonEmpty).getOrElse("UNKNOWN")

  def topicNames(prefix: String, terser: Terser, topicNameStrategy: String): Seq[String] =
    val topic = topicNameStrategy match
      case "structure" =>
        // MSH-9.3 only — no `hl7.v2.` infix. Use this when the prefix already
        // encodes the protocol/version segments (e.g.
        // KAFKA_TOPIC_PREFIX=volcano.producer.hl7.<sender>.).
        // Topic format: {prefix}{structure}  →  volcano.producer.hl7.cgm.medico.adt_a01
        s"${prefix}${sanitize(field(terser, "/MSH-9-3"))}".toLowerCase

      case "message_structure" =>
        // MSH-9.3 is message structure (ADT_A01, ORU_R01, etc.) - required in HL7 v2.5+
        // Topic format: {prefix}hl7.v2.{structure}  →  volcano.hl7.v2.adt_a01
        s"${prefix}hl7.v2.${sanitize(field(terser, "/MSH-9-3"))}".toLowerCase

      case _ => // "legacy" or default
        // MSH-9.1 is message type (ADT/ORU/ORM...), MSH-9.2 is trigger event (A01/R01...)
        // Using type.event format for backwards compatibility with HL7 v2 versions prior to v2.5
        // Topic format: {prefix}hl7.v2.{type}.{event}  →  volcano.hl7.v2.adt.a01
        val typ   = sanitize(field(terser, "/MSH-9-1"))
        val event = sanitize(field(terser, "/MSH-9-2"))
        s"${prefix}hl7.v2.${typ}.${event}".toLowerCase

    Seq(topic)

  def messageKey(terser: Terser, topicNameStrategy: String): String =
    // Prefer MSH-10 message control ID; fallback depends on topic naming strategy
    val mcid = Option(terser.get("/MSH-10")).filter(s => s != null && s.nonEmpty)
    mcid.getOrElse {
      topicNameStrategy match
        case "structure" | "message_structure" =>
          s"${field(terser, "/MSH-9-3")}-${System.currentTimeMillis()}"
        case _ => // "legacy" or default
          s"${field(terser, "/MSH-9-1")}.${field(terser, "/MSH-9-2")}-${System.currentTimeMillis()}"
    }

  def messageInfo(terser: Terser): String =
    val t  = Option(terser.get("/MSH-9-1")).getOrElse("?")
    val ev = Option(terser.get("/MSH-9-2")).getOrElse("?")
    val st = Option(terser.get("/MSH-9-3")).getOrElse("?")
    s"$st ($t^$ev)"
