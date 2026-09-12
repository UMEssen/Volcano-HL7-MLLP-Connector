package volcano.hl7mllp

import ca.uhn.hl7v2.DefaultHapiContext
import com.google.gson.JsonParser

import scala.jdk.CollectionConverters.*

// Pins the envelope's field-value contract (issue #13): every
// segments[].fields[].value is the field's ER7 encoding — never HAPI's debug
// rendering, which wrapped non-primitive fields in their datatype class name
// ("HD[SENDING_APP]", "MSG[ADT^A01^ADT_A01]", "Varies[...]").
//
// All fixtures are synthetic placeholder data.
class EnvelopeEncodingSuite extends munit.FunSuite:

  private val ctx    = new DefaultHapiContext()
  private val parser = ctx.getPipeParser()

  // Exercises every shape that used to leak a wrapper: composites (MSH-3 HD,
  // MSH-9 MSG, PID-3 CX, PID-5 XPN), a repeating field, a Varies-backed field
  // (OBX-5) and a Z-segment HAPI has no structure for at all.
  private val sample =
    "MSH|^~\\&|SENDING_APP|SENDING_FAC|RECEIVING_APP|RECEIVING_FAC|20251019103000||ADT^A01^ADT_A01|MSG00001|P|2.5\r" +
      "EVN|A01|20251019103000\r" +
      "PID|1||12345^^^MRN~67890^^^MRN2||DOE^JOHN^A||19800101|M\r" +
      "OBX|1|ST|CODE^Label^L||free text value|unit|ref|N|||F\r" +
      "ZZZ|custom^segment|second"

  // A pipe inside data arrives escaped on the wire as \F\.
  private val escapedSample =
    "MSH|^~\\&|SENDING_APP|SENDING_FAC|RECEIVING_APP|RECEIVING_FAC|20251019103000||ORU^R01^ORU_R01|MSG00002|P|2.5\r" +
      "OBX|1|ST|CODE^La\\S\\bel^L||value|unit|re\\F\\f|N|||F"

  private def envelope(er7: String): Map[(String, Int, Int), String] =
    val json = JsonParser.parseString(HL7ToJsonConverter.convert(parser.parse(er7), parser)).getAsJsonObject
    (for
      segment <- json.getAsJsonArray("segments").asScala.map(_.getAsJsonObject)
      field   <- segment.getAsJsonArray("fields").asScala.map(_.getAsJsonObject)
    yield (
      segment.get("segment_name").getAsString,
      field.get("field").getAsInt,
      field.get("repetition").getAsInt
    ) -> field.get("value").getAsString).toMap

  private def rawOf(er7: String): String =
    JsonParser.parseString(HL7ToJsonConverter.convert(parser.parse(er7), parser)).getAsJsonObject
      .get("hl7_raw").getAsString

  test("composite fields are pipe-encoded, not wrapped in their datatype name") {
    val f = envelope(sample)
    assertEquals(f(("MSH", 3, 0)), "SENDING_APP")
    assertEquals(f(("MSH", 4, 0)), "SENDING_FAC")
    assertEquals(f(("MSH", 5, 0)), "RECEIVING_APP")
    assertEquals(f(("MSH", 9, 0)), "ADT^A01^ADT_A01")
    assertEquals(f(("MSH", 12, 0)), "2.5")
    assertEquals(f(("PID", 5, 0)), "DOE^JOHN^A")
  }

  test("repeating composite field keeps one entry per repetition, each pipe-encoded") {
    val f = envelope(sample)
    assertEquals(f(("PID", 3, 0)), "12345^^^MRN")
    assertEquals(f(("PID", 3, 1)), "67890^^^MRN2")
  }

  test("Varies-backed and Z-segment fields are unwrapped to their value") {
    val f = envelope(sample)
    assertEquals(f(("OBX", 5, 0)), "free text value")
    assertEquals(f(("ZZZ", 1, 0)), "custom^segment")
    assertEquals(f(("ZZZ", 2, 0)), "second")
  }

  test("no field value carries a DatatypeName[...] wrapper") {
    val wrapper = """^[A-Za-z][A-Za-z0-9_]*\[.*\]$""".r
    val leaked  = envelope(sample).collect { case (k, v) if wrapper.matches(v) => s"$k=$v" }
    assert(leaked.isEmpty, s"wrapped values in envelope: ${leaked.mkString(", ")}")
  }

  test("MSH-1/MSH-2 carry the delimiters verbatim, not escaped into their own data") {
    val f = envelope(sample)
    assertEquals(f(("MSH", 1, 0)), "|")
    assertEquals(f(("MSH", 2, 0)), "^~\\&")
  }

  test("delimiters inside data stay escaped, exactly as in hl7_raw") {
    val f = envelope(escapedSample)
    // Primitive field: HAPI unescapes into the model, the envelope re-encodes.
    assertEquals(f(("OBX", 7, 0)), "re\\F\\f")
    // Composite: the escape sequence is carried through untouched.
    assertEquals(f(("OBX", 3, 0)), "CODE^La\\S\\bel^L")
  }

  test("every field value appears verbatim in hl7_raw") {
    val raw = rawOf(sample)
    envelope(sample).foreach { case ((seg, num, rep), value) =>
      assert(raw.contains(value), s"$seg-$num rep=$rep value [$value] is not a substring of hl7_raw")
    }
  }

  test("schema_version advertises the field-value contract") {
    assertEquals(HL7ToJsonConverter.SchemaVersion, "1.1")
  }
