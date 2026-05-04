package volcano.hl7mllp

import ca.uhn.hl7v2.DefaultHapiContext

object JsonConversionTest:
  def main(args: Array[String]): Unit =
    // Sample HL7 ADT message (with \r separator as required by HL7)
    val sampleHL7 = "MSH|^~\\&|SENDING_APP|SENDING_FAC|RECEIVING_APP|RECEIVING_FAC|20251019103000||ADT^A01^ADT_A01|MSG00001|P|2.5\rEVN|A01|20251019103000\rPID|1||12345^^^MRN||DOE^JOHN^A||19800101|M"

    val hapiCtx = new DefaultHapiContext()
    val pipeParser = hapiCtx.getPipeParser()

    // Parse the HL7 message
    val msg = pipeParser.parse(sampleHL7)

    // Convert to JSON using the dedicated converter
    val json = HL7ToJsonConverter.convert(msg, pipeParser)

    println("=" * 80)
    println("JSON OUTPUT:")
    println("=" * 80)
    println(json)
    println("=" * 80)

    hapiCtx.close()
