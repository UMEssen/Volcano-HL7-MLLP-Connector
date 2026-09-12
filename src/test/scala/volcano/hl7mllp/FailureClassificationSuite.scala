package volcano.hl7mllp

import ca.uhn.hl7v2.HL7Exception
import org.apache.kafka.common.errors.{RecordTooLargeException, TimeoutException as KafkaTimeoutException}

import java.util.concurrent.{ExecutionException, TimeoutException}

// Guards the mapping from a thrown failure to the metric label operators alert
// on. Two unrelated classes are called TimeoutException on this path and both
// have to land on "timeout": java.util.concurrent's (Future.get) and
// org.apache.kafka.common.errors' (max.block.ms synchronously from send(),
// delivery.timeout.ms wrapped in an ExecutionException).
class FailureClassificationSuite extends munit.FunSuite:

  test("Future.get's own deadline is a timeout") {
    assertEquals(Main.classify(new TimeoutException("app-level wait expired")), "timeout")
  }

  test("a max.block.ms timeout thrown synchronously by send() is a timeout") {
    assertEquals(Main.classify(new KafkaTimeoutException("Topic metadata not available")), "timeout")
  }

  test("a delivery.timeout.ms expiry wrapped in an ExecutionException is a timeout") {
    val cause = new KafkaTimeoutException("Expiring 1 record(s): 10000 ms has passed")
    assertEquals(Main.classify(new ExecutionException(cause)), "timeout")
  }

  test("an over-large record keeps its own reason") {
    assertEquals(Main.classify(new ExecutionException(new RecordTooLargeException("too big"))), "record_too_large")
  }

  test("an HL7 parse failure keeps its own reason") {
    assertEquals(Main.classify(new HL7Exception("bad MSH")), "hl7_parse")
  }

  test("anything else falls back to other") {
    assertEquals(Main.classify(new IllegalStateException("boom")), "other")
  }

  test("every reason the metric pre-warms is reachable") {
    val reasons = Set(
      Main.classify(new TimeoutException("x")),
      Main.classify(new ExecutionException(new RecordTooLargeException("x"))),
      Main.classify(new HL7Exception("x")),
      Main.classify(new IllegalStateException("x"))
    )
    assertEquals(reasons, Set("timeout", "record_too_large", "hl7_parse", "other"))
  }
