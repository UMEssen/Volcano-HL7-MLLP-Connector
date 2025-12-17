package volcano.hl7mllp

import ca.uhn.hl7v2.model.Message
import ca.uhn.hl7v2.HL7Exception

import scala.util.Try

object HL7AckGenerator:

  def success(msg: Message): Message =
    Try(msg.generateACK()).getOrElse(
      msg.getParser.parse("MSH|^~\\&|||||||ACK^A01|1|P|2.5\rMSA|AA|1\r")
    )

  def error(msg: Message, errorMessage: String): Message =
    Try(msg.generateACK("AE", new HL7Exception(errorMessage))).getOrElse(
      msg.getParser.parse(s"MSH|^~\\&|||||||ACK^A01|1|P|2.5\rMSA|AE|1|${escapeHl7(errorMessage)}\r")
    )

  private def escapeHl7(s: String): String =
    s.replace("|", "\\F\\").replace("\r", " ").take(200)
