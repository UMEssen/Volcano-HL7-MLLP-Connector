// Purpose: bound the size of an inbound MLLP frame before HAPI buffers and parses it.

package volcano.hl7mllp

import ca.uhn.hl7v2.llp.{HL7Reader, LLPException, MinLowerLayerProtocol}

import java.io.{FilterInputStream, IOException, InputStream}

// MLLP framing (HL7 Implementation Guide, Appendix C): <SB> payload <EB><CR>.
private[hl7mllp] object MllpFraming:
  val StartByte: Int = 0x0b
  val EndByte1: Int  = 0x1c
  val EndByte2: Int  = 0x0d

/** An inbound MLLP frame exceeded the configured cap and was refused mid-read. */
final class MllpFrameTooLargeException(val maxFrameBytes: Int)
    extends IOException(
      s"Inbound MLLP frame exceeds the configured cap of $maxFrameBytes bytes " +
        "(MLLP_MAX_FRAME_BYTES); refused before parsing"
    )

/**
 * Counts the payload bytes of each MLLP frame as they come off the socket and
 * fails the read once one frame exceeds `maxFrameBytes`.
 *
 * Why here and not after parsing: the only size limit the connector had was
 * Kafka-side (`max.request.size`), which is checked after HAPI has already
 * received the whole frame and built the full object model from it. A single
 * oversized frame therefore cost memory and parse time proportional to its
 * size before anything could reject it. This caps that cost at the wire.
 *
 * Only bytes *inside* a frame are counted, and the counter resets on every
 * start byte, so a long-lived connection carrying many frames is bounded per
 * frame rather than in total. Bytes arriving outside a frame are not counted:
 * HAPI's decoder discards them without buffering (`MllpDecoderState.START`,
 * hapi-base 2.6.0), so they are not a memory risk.
 *
 * Accuracy: HAPI wraps this stream in a `BufferedInputStream`
 * (`Hl7DecoderReader.setInputStream`), which reads ahead a block at a time, so
 * the counter can run up to one buffer (8 KiB by default) ahead of what the
 * decoder has actually consumed. At megabyte-scale caps that is irrelevant,
 * and it only ever errs towards rejecting slightly early.
 *
 * Not thread-safe, and does not need to be: HAPI creates one reader — and
 * therefore one instance of this stream — per accepted connection, read only
 * by that connection's receiver thread.
 */
final class FrameLimitingInputStream(
    in: InputStream,
    maxFrameBytes: Int,
    onReject: () => Unit
) extends FilterInputStream(in):

  private var inFrame: Boolean = false
  private var frameBytes: Long = 0L

  override def read(): Int =
    val b = in.read()
    if b >= 0 then account(b)
    b

  override def read(buf: Array[Byte], off: Int, len: Int): Int =
    val n = in.read(buf, off, len)
    if n > 0 then
      var i = off
      val end = off + n
      while i < end do
        account(buf(i) & 0xff)
        i += 1
    n

  // Read-and-discard rather than delegating, so skipped bytes are still counted.
  override def skip(n: Long): Long =
    if n <= 0L then 0L
    else
      val buf = new Array[Byte](math.min(n, 8192L).toInt)
      val read = this.read(buf, 0, buf.length)
      if read < 0 then 0L else read.toLong

  // mark/reset would re-deliver bytes and double-count them. Refusing it keeps
  // the count honest; BufferedInputStream does not need it.
  override def markSupported(): Boolean = false

  private def account(b: Int): Unit =
    if b == MllpFraming.StartByte then
      inFrame = true
      frameBytes = 0L
    else if b == MllpFraming.EndByte1 then inFrame = false
    else if inFrame then
      frameBytes += 1L
      if frameBytes > maxFrameBytes then
        // Reset before throwing so a caller that keeps reading (it should not;
        // HAPI closes the connection) does not trip on the same bytes forever.
        inFrame = false
        frameBytes = 0L
        onReject()
        throw new MllpFrameTooLargeException(maxFrameBytes)

/**
 * `MinLowerLayerProtocol` with an inbound frame-size cap.
 *
 * There is no AE for an oversized frame, deliberately and unavoidably: HAPI
 * routes a reader failure to `Receiver.handle`, which closes the connection
 * without reaching the responder (hapi-base 2.6.0) — and there is no parsed
 * message to build an acknowledgment from in the first place. The sender sees
 * the connection drop, which under the hold-and-retry contract it already
 * implements is the same signal as a NAK: it keeps the message and retries.
 * Messages that fit the frame cap but still produce an over-large Kafka record
 * do get a proper AE, via `RecordTooLargeException`.
 */
final class BoundedMinLowerLayerProtocol(maxFrameBytes: Int, onReject: () => Unit)
    extends MinLowerLayerProtocol:

  @throws[LLPException]
  override def getReader(in: InputStream): HL7Reader =
    super.getReader(new FrameLimitingInputStream(in, maxFrameBytes, onReject))
