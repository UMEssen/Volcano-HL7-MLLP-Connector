package volcano.hl7mllp

import ca.uhn.hl7v2.llp.MinLowerLayerProtocol

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.atomic.AtomicInteger

// The inbound frame-size cap (issue #15): an oversized MLLP frame must be
// refused at the wire, before HAPI buffers and parses it.
//
// The fixtures are synthetic: a placeholder MSH plus an OBX padded with filler.
class MllpFrameLimitSuite extends munit.FunSuite:

  private val StartByte = 0x0b.toByte
  private val EndByte1  = 0x1c.toByte
  private val EndByte2  = 0x0d.toByte

  private def message(padding: Int): String =
    "MSH|^~\\&|SENDING_APP|SENDING_FAC|RECEIVING_APP|RECEIVING_FAC|20251019103000||ADT^A01^ADT_A01|MSG00001|P|2.5\r" +
      "OBX|1|ST|CODE^Label^L||" + ("A" * padding) + "|||||F"

  /** Wrap messages in MLLP frames: <SB> payload <EB><CR>, back to back. */
  private def framed(messages: String*): Array[Byte] =
    messages.foldLeft(Array.empty[Byte]) { (acc, m) =>
      acc ++ Array(StartByte) ++ m.getBytes(UTF_8) ++ Array(EndByte1, EndByte2)
    }

  private def readAll(bytes: Array[Byte], cap: Int, rejects: AtomicInteger, count: Int): List[String] =
    val llp = BoundedMinLowerLayerProtocol(cap, () => rejects.incrementAndGet())
    llp.setCharset(UTF_8)
    val reader = llp.getReader(new ByteArrayInputStream(bytes))
    List.fill(count)(reader.getMessage())

  test("a frame within the cap is delivered unchanged and does not trip the counter") {
    val rejects = new AtomicInteger(0)
    val m       = message(100)
    assertEquals(readAll(framed(m), cap = 64 * 1024, rejects, count = 1), List(m))
    assertEquals(rejects.get(), 0)
  }

  test("an oversized frame is refused before parsing, and counted") {
    val rejects = new AtomicInteger(0)
    val cap     = 4096
    val bytes   = framed(message(32 * 1024))
    val thrown = intercept[MllpFrameTooLargeException] {
      readAll(bytes, cap, rejects, count = 1)
    }
    assertEquals(thrown.maxFrameBytes, cap)
    assertEquals(rejects.get(), 1)
  }

  test("the byte budget is per frame, not per connection") {
    // Three frames, each comfortably under the cap, together far over it: a
    // counter that did not reset on the start byte would reject the third.
    val rejects = new AtomicInteger(0)
    val cap     = 4096
    val ms      = List(message(1000), message(1000), message(1000))
    assertEquals(readAll(framed(ms*), cap, rejects, count = 3), ms)
    assertEquals(rejects.get(), 0)
  }

  test("the cap bounds what is buffered, not just what is parsed") {
    // The decoder accumulates the payload as it reads; the stream must fail
    // within a bounded margin of the cap rather than after the whole frame.
    val cap     = 4096
    val payload = framed(message(8 * 1024 * 1024))
    val counted = new AtomicInteger(0)
    val stream  = new FrameLimitingInputStream(new ByteArrayInputStream(payload), cap, () => ())
    intercept[MllpFrameTooLargeException] {
      while stream.read() >= 0 do counted.incrementAndGet()
    }
    // One start byte plus at most `cap` payload bytes reach the consumer.
    assert(counted.get() <= cap + 1, s"consumed ${counted.get()} bytes for a cap of $cap")
  }

  test("an unbounded protocol is still available and reads the same frame") {
    val m      = message(100)
    val llp    = new MinLowerLayerProtocol()
    llp.setCharset(UTF_8)
    val reader = llp.getReader(new ByteArrayInputStream(framed(m)))
    assertEquals(reader.getMessage(), m)
  }
