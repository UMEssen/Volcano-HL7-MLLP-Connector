package volcano.hl7mllp

import ca.uhn.hl7v2.util.Terser

object HL7MessageProcessor:

  private def sanitize(s: String): String =
    s.replace('^', '_').replace('/', '_').replace(' ', '_')

  private def field(terser: Terser, path: String): String =
    Option(terser.get(path)).map(_.trim).filter(_.nonEmpty).getOrElse("UNKNOWN")

  // Topic name = {prefix}{infix}{type}.{event}. Uses MSH-9.1 (message type)
  // and MSH-9.2 (trigger event) — both mandatory in every HL7 v2.x version
  // (pre-2.5 and 2.5+), so this single path covers all senders.
  def topicNames(prefix: String, infix: String, terser: Terser): Seq[String] =
    val typ   = sanitize(field(terser, "/MSH-9-1"))
    val event = sanitize(field(terser, "/MSH-9-2"))
    Seq(s"${prefix}${infix}${typ}.${event}".toLowerCase)

  // Kafka partition key = MSH-10 (message control ID); fallback to a
  // synthetic key that includes the message type/event so partitioning
  // still groups same-kind messages on retries.
  def messageKey(terser: Terser): String =
    val mcid = Option(terser.get("/MSH-10")).filter(s => s != null && s.nonEmpty)
    mcid.getOrElse(s"${field(terser, "/MSH-9-1")}.${field(terser, "/MSH-9-2")}-${System.currentTimeMillis()}")

  def messageInfo(terser: Terser): String =
    val t  = Option(terser.get("/MSH-9-1")).getOrElse("?")
    val ev = Option(terser.get("/MSH-9-2")).getOrElse("?")
    val st = Option(terser.get("/MSH-9-3")).getOrElse("?")
    s"$st ($t^$ev)"
