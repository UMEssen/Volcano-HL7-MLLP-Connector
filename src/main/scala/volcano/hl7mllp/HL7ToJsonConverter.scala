package volcano.hl7mllp

import ca.uhn.hl7v2.model.{Message, Segment}
import ca.uhn.hl7v2.parser.PipeParser
import ca.uhn.hl7v2.util.Terser
import com.google.gson.{Gson, JsonObject, JsonArray}

object HL7ToJsonConverter:

  // Bump when the envelope shape changes. Emitted both in the JSON body and as
  // the `schema_version` Kafka header so consumers can branch without guessing.
  val SchemaVersion: String = "1.0"

  // Compact (not pretty-printed) and a single shared instance: this is a
  // machine-to-machine pipeline, so the whitespace was pure payload bloat —
  // it inflated every record and pushed document messages past size limits.
  // Gson is thread-safe for reuse.
  private val gson = new Gson()

  def convert(msg: Message, pipeParser: PipeParser, includeRaw: Boolean = true): String =
    val json = new JsonObject()

    json.addProperty("schema_version", SchemaVersion)

    // The raw ER7 (pipe-delimited) form. Optional: it roughly doubles the
    // envelope, so consumers that only need the parsed view can drop it.
    if includeRaw then
      json.addProperty("hl7_raw", pipeParser.encode(msg))

    json.add("metadata", extractMetadata(msg))
    json.add("segments", extractSegments(msg))

    gson.toJson(json)

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

  private def extractSegments(msg: Message): JsonArray =
    val segments = new JsonArray()
    val names = msg.getNames()

    for (i <- 0 until names.length) {
      val name = names(i)
      val structures = msg.getAll(name)
      for (structure <- structures) {
        structure match {
          case seg: Segment =>
            segments.add(extractSegment(name, seg))
          case _ => // Skip non-segment structures
        }
      }
    }
    segments

  private def extractSegment(name: String, seg: Segment): JsonObject =
    val segmentObj = new JsonObject()
    segmentObj.addProperty("segment_name", name)
    segmentObj.add("fields", extractFields(seg))
    segmentObj

  private def extractFields(seg: Segment): JsonArray =
    val fields = new JsonArray()

    for (fieldNum <- 1 to seg.numFields()) {
      try {
        val field = seg.getField(fieldNum)
        if (field != null && field.length > 0) {
          for (rep <- 0 until field.length) {
            val fieldValue = field(rep)
            if (fieldValue != null) {
              val fieldStr = fieldValue.toString
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
