package volcano.hl7mllp

import ca.uhn.hl7v2.model.{Message, Primitive, Segment, Type, Varies}
import ca.uhn.hl7v2.parser.{EncodingCharacters, PipeParser}
import ca.uhn.hl7v2.util.Terser
import com.google.gson.{Gson, JsonObject, JsonArray}

import scala.util.Try

object HL7ToJsonConverter:

  // Bump when the envelope shape or the meaning of its values changes. Emitted
  // both in the JSON body and as the `schema_version` Kafka header so consumers
  // can branch without guessing.
  //   1.0 — initial shape.
  //   1.1 — segments[].fields[].value is the field's ER7 encoding. Before 1.1
  //         it was Type.toString(), which is HAPI's debug rendering and wrapped
  //         every non-primitive field in its datatype class name
  //         ("HD[SENDING_APP]", "MSG[ADT^A01]", "Varies[...]").
  val SchemaVersion: String = "1.1"

  // Compact (not pretty-printed) and a single shared instance: this is a
  // machine-to-machine pipeline, so the whitespace was pure payload bloat —
  // it inflated every record and pushed document messages past size limits.
  // Gson is thread-safe for reuse.
  private val gson = new Gson()

  // MSH-1/MSH-2 (and their batch-header equivalents) *define* the delimiters,
  // so they hold raw delimiter characters rather than data. Re-encoding them
  // would escape the very characters they declare ("|" -> "\F\",
  // "^~\&" -> "\S\\R\\E\\T\"), which is both wrong and unparseable. They are
  // emitted verbatim, exactly as they appear in the ER7.
  private val DelimiterSegments = Set("MSH", "FHS", "BHS")

  private def isDelimiterField(segmentName: String, fieldNum: Int): Boolean =
    (fieldNum == 1 || fieldNum == 2) && DelimiterSegments.contains(segmentName)

  def convert(msg: Message, pipeParser: PipeParser, includeRaw: Boolean = true): String =
    val json = new JsonObject()

    json.addProperty("schema_version", SchemaVersion)

    // The raw ER7 (pipe-delimited) form. Optional: it roughly doubles the
    // envelope, so consumers that only need the parsed view can drop it.
    if includeRaw then
      json.addProperty("hl7_raw", pipeParser.encode(msg))

    json.add("metadata", extractMetadata(msg))
    json.add("segments", extractSegments(msg, encodingCharsOf(msg)))

    gson.toJson(json)

  // The message's own delimiters (MSH-2), so a sender using non-standard
  // encoding characters round-trips. Falls back to the HL7 defaults if MSH-2
  // is unreadable — which is what HAPI's own debug rendering always used.
  private def encodingCharsOf(msg: Message): EncodingCharacters =
    Try(EncodingCharacters.getInstance(msg)).getOrElse(EncodingCharacters.defaultInstance())

  private def extractMetadata(msg: Message): JsonObject =
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
    metadata

  private def extractSegments(msg: Message, encodingChars: EncodingCharacters): JsonArray =
    val segments = new JsonArray()
    val names = msg.getNames()

    for (i <- 0 until names.length) {
      val name = names(i)
      val structures = msg.getAll(name)
      for (structure <- structures) {
        structure match {
          case seg: Segment =>
            segments.add(extractSegment(name, seg, encodingChars))
          case _ => // Skip non-segment structures
        }
      }
    }
    segments

  private def extractSegment(name: String, seg: Segment, encodingChars: EncodingCharacters): JsonObject =
    val segmentObj = new JsonObject()
    segmentObj.addProperty("segment_name", name)
    segmentObj.add("fields", extractFields(name, seg, encodingChars))
    segmentObj

  private def extractFields(segmentName: String, seg: Segment, encodingChars: EncodingCharacters): JsonArray =
    val fields = new JsonArray()

    for (fieldNum <- 1 to seg.numFields()) {
      try {
        val field = seg.getField(fieldNum)
        if (field != null && field.length > 0) {
          for (rep <- 0 until field.length) {
            val fieldValue = field(rep)
            if (fieldValue != null) {
              val fieldStr = encodeField(segmentName, fieldNum, fieldValue, encodingChars)
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
    fields

  // Render one field repetition as its ER7 encoding — the exact substring the
  // field occupies in `hl7_raw`: components joined with "^", subcomponents with
  // "&", delimiters inside data escaped ("\F\", "\S\", ...).
  //
  // NOT Type.toString(): HAPI documents that as a debug rendering and it
  // returns "<DatatypeClassName>[<encoded value>]" for every non-primitive
  // (AbstractType.toString, hapi-base 2.6.0), which is where the "HD[...]",
  // "MSG[...]" and "Varies[...]" wrappers in the envelope came from.
  private def encodeField(
      segmentName: String,
      fieldNum: Int,
      fieldValue: Type,
      encodingChars: EncodingCharacters
  ): String =
    fieldValue match
      // Delimiter definitions are stored as the literal delimiter characters
      // and must not be escaped back into the data they declare.
      case p: Primitive if isDelimiterField(segmentName, fieldNum) =>
        Option(p.getValue).getOrElse("")
      case _ =>
        // Fields HAPI cannot type statically (OBX-5, Z-segments) arrive as a
        // Varies wrapping the concrete Type. PipeParser.encode unwraps Varies
        // itself; doing it here too is defence in depth, and keeps the
        // behaviour explicit rather than relying on a library internal.
        val resolved = fieldValue match
          case v: Varies if v.getData != null => v.getData
          case other                          => other
        PipeParser.encode(resolved, encodingChars)
