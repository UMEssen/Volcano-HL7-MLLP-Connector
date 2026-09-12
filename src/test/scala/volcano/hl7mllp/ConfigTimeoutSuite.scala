package volcano.hl7mllp

// The producer timeout chain (issue #14): the handler's wait on the send
// future must never be shorter than the producer's own delivery budget, or an
// AE is returned for a record the producer is still retrying and may still
// land.
class ConfigTimeoutSuite extends munit.FunSuite:

  private def cfg(ackMs: Int, requestMs: Int, deliveryMs: Int): Config = Config(
    port = 2575, useTls = false, kafkaBootstrap = "localhost:9092",
    topicStatic = None, topicPrefix = "volcano.", topicInfix = "hl7.v2.",
    kafkaClientId = "test",
    kafkaAcksTimeoutMs = ackMs, kafkaRequestTimeoutMs = requestMs, kafkaDeliveryTimeoutMs = deliveryMs,
    kafkaMaxRequestSize = 10485760, kafkaBufferMemory = 67108864L, kafkaCompressionType = "lz4",
    kafkaSaslEnabled = false, kafkaSaslMechanism = "SCRAM-SHA-512",
    kafkaSaslUsername = None, kafkaSaslPassword = None,
    kafkaSslEnabled = false, kafkaSslTruststoreLocation = None, kafkaSslTruststoreType = "PEM",
    hl7Encoding = "UTF-8", includeRaw = true, metricsEnabled = false, metricsPort = 9404
  )

  test("the shipped defaults satisfy ack wait >= delivery >= request") {
    val c = cfg(5000, Config.DefaultRequestTimeoutMs, Config.DefaultDeliveryTimeoutMs)
    assert(c.kafkaAckWaitMs >= c.kafkaDeliveryTimeoutMs)
    assert(c.kafkaDeliveryTimeoutMs >= c.kafkaRequestTimeoutMs)
  }

  test("an ack timeout below the delivery budget is raised to it") {
    val c = cfg(5000, 5000, 10000)
    assertEquals(c.kafkaAckWaitMs, 10000)
    assert(c.ackWaitWasRaised)
  }

  test("an ack timeout above the delivery budget is left alone") {
    val c = cfg(30000, 5000, 10000)
    assertEquals(c.kafkaAckWaitMs, 30000)
    assert(!c.ackWaitWasRaised)
  }

  test("a delivery budget shorter than one request round trip is rejected") {
    val reason = Config.validateTimeouts(cfg(5000, 5000, 3000))
    assert(reason.isLeft, "delivery < request must not start up")
    assert(clue(reason.left.getOrElse("")).contains("KAFKA_DELIVERY_TIMEOUT_MS"))
  }

  test("non-positive timeouts are rejected") {
    assert(Config.validateTimeouts(cfg(0, 5000, 10000)).isLeft)
    assert(Config.validateTimeouts(cfg(5000, 0, 10000)).isLeft)
  }

  test("a valid chain is returned unchanged") {
    val c = cfg(5000, 5000, 10000)
    assertEquals(Config.validateTimeouts(c), Right(c))
  }
