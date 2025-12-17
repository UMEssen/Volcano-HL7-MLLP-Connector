package volcano.hl7mllp

import ca.uhn.hl7v2.util.Terser

object HL7MessageProcessor:

  def topicNames(prefix: String, terser: Terser, topicNameStrategy: String): Seq[String] =
    val topic = topicNameStrategy match
      case "message_structure" =>
        // MSH-9.3 is message structure (ADT_A01, ORU_R01, etc.) - required in HL7 v2.5+
        val structure = Option(terser.get("/MSH-9-3")).map(_.trim).filter(_.nonEmpty).getOrElse("UNKNOWN")
        val sanitized = structure.replace('^', '_').replace('/', '_').replace(' ', '_')
        s"${prefix}hl7.v2.${sanitized}".toLowerCase

      case _ => // "legacy" or default
        // MSH-9.1 is message type (ADT/ORU/ORM...), MSH-9.2 is trigger event (A01/R01...)
        // Using type.event format for backwards compatibility with HL7 v2 versions prior to v2.5
        val typ   = Option(terser.get("/MSH-9-1")).map(_.trim).filter(_.nonEmpty).getOrElse("UNKNOWN")
        val event = Option(terser.get("/MSH-9-2")).map(_.trim).filter(_.nonEmpty).getOrElse("UNKNOWN")
        val sanitizedTyp   = typ.replace('^', '_').replace('/', '_').replace(' ', '_')
        val sanitizedEvent = event.replace('^', '_').replace('/', '_').replace(' ', '_')
        s"${prefix}hl7.v2.${sanitizedTyp}.${sanitizedEvent}".toLowerCase

    Seq(topic)

  def messageKey(terser: Terser, topicNameStrategy: String): String =
    // Prefer MSH-10 message control ID; fallback depends on topic naming strategy
    val mcid = Option(terser.get("/MSH-10")).filter(s => s != null && s.nonEmpty)
    mcid.getOrElse {
      topicNameStrategy match
        case "message_structure" =>
          val structure = Option(terser.get("/MSH-9-3")).map(_.trim).filter(_.nonEmpty).getOrElse("UNKNOWN")
          s"${structure}-${System.currentTimeMillis()}"
        case _ => // "legacy" or default
          val typ = Option(terser.get("/MSH-9-1")).map(_.trim).filter(_.nonEmpty).getOrElse("UNKNOWN")
          val event = Option(terser.get("/MSH-9-2")).map(_.trim).filter(_.nonEmpty).getOrElse("UNKNOWN")
          s"${typ}.${event}-${System.currentTimeMillis()}"
    }

  def messageInfo(terser: Terser): String =
    val t  = Option(terser.get("/MSH-9-1")).getOrElse("?")
    val ev = Option(terser.get("/MSH-9-2")).getOrElse("?")
    val st = Option(terser.get("/MSH-9-3")).getOrElse("?")
    s"$st ($t^$ev)"
