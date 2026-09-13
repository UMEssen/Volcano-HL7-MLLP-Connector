package volcano.hl7mllp

import ca.uhn.hl7v2.DefaultHapiContext
import ca.uhn.hl7v2.model.{Group, Message, Segment, Structure}
import ca.uhn.hl7v2.parser.{GenericModelClassFactory, PipeParser}
import com.google.gson.JsonParser

import scala.collection.mutable
import scala.jdk.CollectionConverters.*

// The envelope's `segments[]` must be a flat list in document order regardless
// of which shape HAPI gave the message — see HL7ToJsonConverter.extractSegments.
//
// HAPI picks the shape, not the sender:
//   Path A, structure parse  — the default PipeParser resolves a message to a
//     generated structure class when a structures library for MSH-12 is on the
//     classpath and the type/trigger resolves (here: hapi-structures-v25, so
//     MSH-12 = 2.5 with a known type). Segments then sit inside the groups the
//     standard defines, several levels deep.
//   Path B, generic parse    — anything else yields GenericMessage, which is
//     flat. Forced here with GenericModelClassFactory so the same fixture text
//     can be put through both paths and the results compared directly.
class SegmentWalkSuite extends munit.FunSuite:

  // --- fixtures: synthetic placeholders only, no real message content -------

  private val OruWithGroups =
    "MSH|^~\\&|APP_A|FAC_A|APP_B|FAC_B|20250101000000||ORU^R01^ORU_R01|MSGID0001|P|2.5\r" +
      "PID|1||PATID001^^^AUTH||SAMPLE^TEST^A||19700101|F\r" +
      "PV1|1|I|WARD^ROOM^BED\r" +
      "OBR|1||FILLER001|PANEL^Panel name^L\r" +
      "OBX|1|NM|CODE1^Analyte one^L||1.0|unit|||||F\r" +
      "OBX|2|NM|CODE2^Analyte two^L||2.0|unit|||||F\r"

  // The same segments with an NTE between the two OBX, so the second OBX is a
  // repeat that is NOT contiguous with the first — the case where the two
  // parse paths name a segment differently (see the naming test below).
  private val OruNonContiguousRepeat =
    "MSH|^~\\&|APP_A|FAC_A|APP_B|FAC_B|20250101000000||ORU^R01^ORU_R01|MSGID0002|P|2.5\r" +
      "PID|1||PATID001^^^AUTH||SAMPLE^TEST^A||19700101|F\r" +
      "OBR|1||FILLER001|PANEL^Panel name^L\r" +
      "OBX|1|NM|CODE1^Analyte one^L||1.0|unit|||||F\r" +
      "NTE|1||Comment one\r" +
      "OBX|2|NM|CODE2^Analyte two^L||2.0|unit|||||F\r"

  // PR1 lives in the PROCEDURE group and IN1 in the INSURANCE group of ADT_A01;
  // everything else in this message is a top-level child.
  private val AdtWithNestedGroups =
    "MSH|^~\\&|APP_A|FAC_A|APP_B|FAC_B|20250101000000||ADT^A01^ADT_A01|MSGID0003|P|2.5\r" +
      "EVN|A01|20250101000000\r" +
      "PID|1||PATID001^^^AUTH||SAMPLE^TEST^A||19700101|F\r" +
      "PV1|1|I|WARD^ROOM^BED\r" +
      "PR1|1||PROC1^Procedure^L|20250101\r" +
      "IN1|1|PLAN1^Plan^L|INS1\r"

  private val OmlOrderGroup =
    "MSH|^~\\&|APP_A|FAC_A|APP_B|FAC_B|20250101000000||OML^O21^OML_O21|MSGID0004|P|2.5\r" +
      "PID|1||PATID001^^^AUTH||SAMPLE^TEST^A||19700101|F\r" +
      "ORC|NW|PLACER001|FILLER001\r" +
      "OBR|1|PLACER001|FILLER001|PANEL^Panel name^L\r"

  private val OmgOrderGroupWithZSegment =
    "MSH|^~\\&|APP_A|FAC_A|APP_B|FAC_B|20250101000000||OMG^O19^OMG_O19|MSGID0005|P|2.5\r" +
      "PID|1||PATID001^^^AUTH||SAMPLE^TEST^A||19700101|F\r" +
      "ORC|NW|PLACER001|FILLER001\r" +
      "OBR|1|PLACER001|FILLER001|PANEL^Panel name^L\r" +
      "ZZZ|1|local\r"

  private val AllFixtures = Map(
    "ORU_R01 with OBR/OBX groups" -> OruWithGroups,
    "ORU_R01 with a non-contiguous OBX repeat" -> OruNonContiguousRepeat,
    "ADT_A01 with PROCEDURE and INSURANCE groups" -> AdtWithNestedGroups,
    "OML_O21 order group" -> OmlOrderGroup,
    "OMG_O19 order group with a Z-segment" -> OmgOrderGroupWithZSegment
  )

  // --- harness --------------------------------------------------------------

  private val hapiCtx = new DefaultHapiContext()
  // Path A: the parser the connector actually runs (Main uses the context's).
  private val structureParser: PipeParser = hapiCtx.getPipeParser
  // Path B: GenericModelClassFactory never resolves a structure class, so every
  // message comes back as GenericMessage — the flat shape production sees today.
  private val genericParser: PipeParser = new PipeParser(new GenericModelClassFactory())

  override def afterAll(): Unit = hapiCtx.close()

  private def envelope(parser: PipeParser, er7: String): String =
    HL7ToJsonConverter.convert(parser.parse(er7), parser)

  /** (segment_name, ["<field>.<repetition>=<value>", ...]) for every segment. */
  private def segmentsOf(json: String): Seq[(String, Seq[String])] =
    JsonParser
      .parseString(json)
      .getAsJsonObject
      .getAsJsonArray("segments")
      .asScala
      .toSeq
      .map { entry =>
        val obj = entry.getAsJsonObject
        val fields = obj.getAsJsonArray("fields").asScala.toSeq.map { f =>
          val fo = f.getAsJsonObject
          s"${fo.get("field").getAsInt}.${fo.get("repetition").getAsInt}=${fo.get("value").getAsString}"
        }
        (obj.get("segment_name").getAsString, fields)
      }

  private def namesOf(json: String): Seq[String] = segmentsOf(json).map(_._1)

  /** MSH-1/MSH-2 hold the delimiters themselves and are the one field-level
    * difference between the two parse paths; see the dedicated test below. */
  private def withoutDelimiterFields(seg: (String, Seq[String])): (String, Seq[String]) =
    if seg._1.startsWith("MSH") then (seg._1, seg._2.filterNot(f => f.startsWith("1.") || f.startsWith("2.")))
    else seg

  private def rawSegmentCount(er7: String): Int = er7.split('\r').count(_.nonEmpty)

  /** The pre-fix extraction: immediate children only, nested groups discarded.
    * Kept here as the reference for "the flat path is unchanged". */
  private def oneLevelSegmentNames(msg: Message): Seq[String] =
    val out = mutable.ArrayBuffer[String]()
    for name <- msg.getNames; structure <- msg.getAll(name) do
      structure match
        case _: Segment => out += name
        case _          => ()
    out.toSeq

  /** Names of the groups enclosing `structure`, outermost first. */
  private def groupPath(structure: Structure): Seq[String] =
    val out = mutable.ArrayBuffer[String]()
    var parent = structure.getParent
    while parent != null && !parent.isInstanceOf[Message] do
      out.prepend(parent.getName)
      parent = parent.getParent
    out.toSeq

  private def findSegment(group: Group, target: String): Option[Segment] =
    group.getNames.iterator
      .flatMap(name => group.getAll(name).iterator)
      .flatMap {
        case seg: Segment if seg.getName == target => Iterator.single(seg)
        case nested: Group                         => findSegment(nested, target).iterator
        case _                                     => Iterator.empty
      }
      .nextOption()

  // --- the defect -----------------------------------------------------------

  test("a structure-parsed ORU_R01 keeps every segment, in document order") {
    val msg = structureParser.parse(OruWithGroups)
    // Guard the premise: if this fixture ever stops resolving to a structure
    // class the test would pass vacuously on the flat path.
    assertEquals(msg.getClass.getName, "ca.uhn.hl7v2.model.v25.message.ORU_R01")

    val names = namesOf(HL7ToJsonConverter.convert(msg, structureParser))
    assertEquals(names, Seq("MSH", "PID", "PV1", "OBR", "OBX", "OBX"))
  }

  test("segments three groups below the message still reach the envelope") {
    val msg = structureParser.parse(OruWithGroups)
    val pv1 = findSegment(msg, "PV1").getOrElse(fail("PV1 missing from the parsed model"))
    // Measured from the model rather than asserted from the standard: PV1 is
    // nested PATIENT_RESULT > PATIENT > VISIT, i.e. three levels of groups.
    assertEquals(groupPath(pv1), Seq("PATIENT_RESULT", "PATIENT", "VISIT"))

    val segments = segmentsOf(HL7ToJsonConverter.convert(msg, structureParser)).toMap
    assertEquals(segments.get("PV1"), Some(Seq("1.0=1", "2.0=I", "3.0=WARD^ROOM^BED")))
  }

  test("nested groups in an ADT_A01 (PROCEDURE, INSURANCE) reach the envelope") {
    val msg = structureParser.parse(AdtWithNestedGroups)
    assertEquals(msg.getClass.getName, "ca.uhn.hl7v2.model.v25.message.ADT_A01")
    assertEquals(groupPath(findSegment(msg, "PR1").get), Seq("PROCEDURE"))
    assertEquals(groupPath(findSegment(msg, "IN1").get), Seq("INSURANCE"))

    val names = namesOf(HL7ToJsonConverter.convert(msg, structureParser))
    assertEquals(names, Seq("MSH", "EVN", "PID", "PV1", "PR1", "IN1"))
  }

  test("an order group (OML_O21, OMG_O19 + Z-segment) reaches the envelope") {
    val oml = structureParser.parse(OmlOrderGroup)
    assertEquals(oml.getClass.getName, "ca.uhn.hl7v2.model.v25.message.OML_O21")
    assertEquals(namesOf(HL7ToJsonConverter.convert(oml, structureParser)), Seq("MSH", "PID", "ORC", "OBR"))

    val omg = structureParser.parse(OmgOrderGroupWithZSegment)
    assertEquals(omg.getClass.getName, "ca.uhn.hl7v2.model.v25.message.OMG_O19")
    assertEquals(namesOf(HL7ToJsonConverter.convert(omg, structureParser)), Seq("MSH", "PID", "ORC", "OBR", "ZZZ"))
  }

  // --- the two paths agree ---------------------------------------------------

  test("both parse paths yield the same segments for the same message") {
    for (label, er7) <- AllFixtures do
      val structured = segmentsOf(envelope(structureParser, er7)).map(withoutDelimiterFields)
      val generic = segmentsOf(envelope(genericParser, er7)).map(withoutDelimiterFields)
      // Names are compared with the repeat suffix stripped: the flat path
      // appends an index to a non-contiguous repeat ("OBX2"), the structure
      // path does not, and that difference is pinned separately below.
      def base(n: String) = n.reverse.dropWhile(_.isDigit).reverse
      assertEquals(structured.map(s => base(s._1)), generic.map(s => base(s._1)), label)
      assertEquals(structured.map(_._2), generic.map(_._2), label)
  }

  test("envelope segment count equals the hl7_raw segment count on both parse paths") {
    for (label, er7) <- AllFixtures do
      val expected = rawSegmentCount(er7)
      assertEquals(namesOf(envelope(structureParser, er7)).size, expected, s"$label (structure parse)")
      assertEquals(namesOf(envelope(genericParser, er7)).size, expected, s"$label (generic parse)")
  }

  test("no phantom segment for a group the message did not carry") {
    // ORU_R01 declares SFT, DSC, TIMING_QTY, SPECIMEN and more that this
    // fixture does not use; an empty optional structure must contribute
    // nothing rather than a nameless or field-less entry.
    val segments = segmentsOf(envelope(structureParser, OruWithGroups))
    assert(segments.forall(_._2.nonEmpty), s"field-less segment in $segments")
    val groupNames = Set("PATIENT_RESULT", "PATIENT", "VISIT", "ORDER_OBSERVATION", "OBSERVATION")
    assert(segments.forall(s => !groupNames.contains(s._1)), s"group emitted as a segment in $segments")
  }

  // --- the flat path is untouched --------------------------------------------

  test("the generic parse path visits exactly what the one-level loop visited") {
    for (label, er7) <- AllFixtures do
      val msg = genericParser.parse(er7)
      // A GenericMessage has no group children at all, so the recursion never
      // fires and the walk degenerates to the pre-fix loop.
      assert(
        msg.getNames.forall(n => !msg.isGroup(n)),
        s"$label: generic message unexpectedly has a group child"
      )
      assertEquals(namesOf(HL7ToJsonConverter.convert(msg, genericParser)), oneLevelSegmentNames(msg), label)
  }

  test("the generic parse path's envelope content is unchanged") {
    val segments = segmentsOf(envelope(genericParser, OruNonContiguousRepeat))
    assertEquals(
      segments,
      Seq(
        "MSH" -> Seq(
          "1.0=\\F\\",
          "2.0=\\S\\\\R\\\\E\\\\T\\",
          "3.0=APP_A",
          "4.0=FAC_A",
          "5.0=APP_B",
          "6.0=FAC_B",
          "7.0=20250101000000",
          "9.0=ORU^R01^ORU_R01",
          "10.0=MSGID0002",
          "11.0=P",
          "12.0=2.5"
        ),
        "PID" -> Seq("1.0=1", "3.0=PATID001^^^AUTH", "5.0=SAMPLE^TEST^A", "7.0=19700101", "8.0=F"),
        "OBR" -> Seq("1.0=1", "3.0=FILLER001", "4.0=PANEL^Panel name^L"),
        "OBX" -> Seq("1.0=1", "2.0=NM", "3.0=CODE1^Analyte one^L", "5.0=1.0", "6.0=unit", "11.0=F"),
        "NTE" -> Seq("1.0=1", "3.0=Comment one"),
        "OBX2" -> Seq("1.0=2", "2.0=NM", "3.0=CODE2^Analyte two^L", "5.0=2.0", "6.0=unit", "11.0=F")
      )
    )
  }

  // --- the two differences between the paths, pinned -------------------------

  test("a non-contiguous repeat is named with a suffix only on the flat path") {
    // HAPI's own naming, not this converter's: in a GenericMessage a repeat
    // that is not adjacent to its first occurrence gets an index appended,
    // while inside a structure class each repeat is a fresh group instance and
    // keeps the plain name. `segment_name` reports HAPI's name verbatim on both
    // paths, so a consumer that strips a trailing index keeps working.
    assertEquals(
      namesOf(envelope(genericParser, OruNonContiguousRepeat)),
      Seq("MSH", "PID", "OBR", "OBX", "NTE", "OBX2")
    )
    assertEquals(
      namesOf(envelope(structureParser, OruNonContiguousRepeat)),
      Seq("MSH", "PID", "OBR", "OBX", "NTE", "OBX")
    )
  }

  test("MSH-1/MSH-2 are verbatim only on the structure path (pre-existing)") {
    // Not touched by the segment walk, and pinned so it cannot drift silently:
    // HL7ToJsonConverter emits MSH-1/MSH-2 verbatim only for a Primitive, and a
    // GenericMessage types every field as Varies, so on the flat path the two
    // delimiter-defining fields arrive escaped into the characters they declare.
    val structured = segmentsOf(envelope(structureParser, OruWithGroups)).toMap
    assertEquals(structured("MSH").take(2), Seq("1.0=|", "2.0=^~\\&"))

    val generic = segmentsOf(envelope(genericParser, OruWithGroups)).toMap
    assertEquals(generic("MSH").take(2), Seq("1.0=\\F\\", "2.0=\\S\\\\R\\\\E\\\\T\\"))
  }
