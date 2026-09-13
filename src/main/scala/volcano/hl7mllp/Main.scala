// Purpose: Volcano connector that listens on MLLP, parses HL7 v2 with HAPI to JSON, and writes to Kafka by feed (static topic) or message type.

package volcano.hl7mllp

import ca.uhn.hl7v2.DefaultHapiContext
import ca.uhn.hl7v2.app.HL7Service
import ca.uhn.hl7v2.protocol.ReceivingApplication
import ca.uhn.hl7v2.model.Message
import ca.uhn.hl7v2.llp.MinLowerLayerProtocol
import ca.uhn.hl7v2.util.Terser
import ca.uhn.hl7v2.HL7Exception
import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}
import org.apache.kafka.common.errors.{RecordTooLargeException, TimeoutException as KafkaTimeoutException}
import org.slf4j.LoggerFactory

import java.util.concurrent.{ExecutionException, TimeoutException, TimeUnit}
import scala.util.Try

object Main:

  private val log = LoggerFactory.getLogger(getClass)

  // Map a thrown failure to a stable, low-cardinality metric reason + log tag.
  //
  // There are TWO unrelated TimeoutException classes on this path and both mean
  // "timeout" to an operator: java.util.concurrent's, thrown by Future.get when
  // the app-level wait expires, and org.apache.kafka.common.errors', thrown
  // synchronously by send() when max.block.ms expires and wrapped in an
  // ExecutionException when delivery.timeout.ms expires (it extends
  // RetriableException, not the java.util.concurrent one). Matching only the
  // first leaves hl7_kafka_produce_failures_total{reason="timeout"} at zero
  // through a Kafka outage — the exact series the README tells operators to
  // alert on.
  private[hl7mllp] def classify(e: Throwable): String = e match
    case _: TimeoutException          => "timeout"
    case _: KafkaTimeoutException     => "timeout"
    case _: RecordTooLargeException   => "record_too_large"
    case _: HL7Exception              => "hl7_parse"
    case ee: ExecutionException if ee.getCause != null => classify(ee.getCause)
    case _                            => "other"

  def main(args: Array[String]): Unit =
    try
      val cfg = Config.load()

      val routing = cfg.topicStatic match
        case Some(t) => s"static topic='$t'"
        case None    => s"derived prefix='${cfg.topicPrefix}', infix='${cfg.topicInfix}'"
      log.info(s"Starting Volcano HL7 MLLP connector on port ${cfg.port}, TLS=${cfg.useTls}, Kafka=${cfg.kafkaBootstrap}, routing: $routing")
      log.info(s"HL7 MLLP Charset configured: ${cfg.hl7Encoding}, includeRaw=${cfg.includeRaw}")
      log.info(
        s"Kafka timeout chain: request.timeout.ms=${cfg.kafkaRequestTimeoutMs}, " +
          s"delivery.timeout.ms=${cfg.kafkaDeliveryTimeoutMs}, max.block.ms=${cfg.kafkaAcksTimeoutMs}, " +
          s"app ACK wait=${cfg.kafkaAckWaitMs}ms"
      )
      log.info(
        "The ACK wait is derived to trail delivery.timeout.ms, so the producer always gives up " +
          "first and an AE is never returned for a record that can still land. Lower " +
          "KAFKA_DELIVERY_TIMEOUT_MS to fail faster."
      )

      // /healthz reads MLLP-listener liveness through this ref: false until the
      // HL7Service is created and started, true while it runs, false once it
      // stops — so the probe reflects the actual listener, not just the JVM.
      val serverRef = new java.util.concurrent.atomic.AtomicReference[HL7Service]()
      val metricsServer =
        if cfg.metricsEnabled then
          Some(Metrics.start(cfg.metricsPort, () => Option(serverRef.get()).exists(_.isRunning())))
        else { log.info("Metrics disabled (METRICS_ENABLED=false)"); None }

      // Configure HAPI context with proper character encoding, and cap the
      // inbound frame size at the wire. Without the cap the only size limit is
      // Kafka's max.request.size, which is checked long after HAPI has
      // received and fully parsed the message.
      val hapiCtx = new DefaultHapiContext()
      val llp: MinLowerLayerProtocol =
        if cfg.mllpMaxFrameBytes > 0 then
          log.info(s"Inbound MLLP frame cap: ${cfg.mllpMaxFrameBytes} bytes (MLLP_MAX_FRAME_BYTES)")
          if cfg.mllpMaxFrameBytes > cfg.kafkaMaxRequestSize then
            log.warn(
              s"MLLP_MAX_FRAME_BYTES (${cfg.mllpMaxFrameBytes}) exceeds KAFKA_MAX_REQUEST_SIZE " +
                s"(${cfg.kafkaMaxRequestSize}); frames between the two are accepted here and then " +
                "rejected by Kafka with an AE"
            )
          BoundedMinLowerLayerProtocol(
            cfg.mllpMaxFrameBytes,
            () =>
              Metrics.framesRejected.labels("oversize").inc()
              // HAPI logs the connection teardown itself (Receiver.handle, at
              // WARN, with the peer address), but not the knob. Say which
              // setting refused it, so "this feed stopped ingesting" does not
              // have to be diagnosed from a counter.
              log.warn(
                s"Refused an inbound MLLP frame larger than ${cfg.mllpMaxFrameBytes} bytes " +
                  "(MLLP_MAX_FRAME_BYTES); the connection is dropped without an AE and the sender " +
                  "will retry the same frame, so this repeats until the sender or the cap changes"
              )
          )
        else
          log.warn(
            "Inbound MLLP frame cap disabled (MLLP_MAX_FRAME_BYTES <= 0): a frame of any size is " +
              "received and parsed in full before any size check can reject it"
          )
          new MinLowerLayerProtocol()
      llp.setCharset(cfg.hl7Encoding)
      hapiCtx.setLowerLayerProtocol(llp)
      log.info(s"MinLowerLayerProtocol charset explicitly set to: ${cfg.hl7Encoding}")

      val pipeParser = hapiCtx.getPipeParser()

      log.info("Initializing Kafka producer...")
      val producer = try {
        KafkaProducerFactory.create(cfg)
      } catch {
        case e: Exception =>
          log.error("Failed to initialize Kafka producer. Check Kafka connection and SSL/SASL settings.", e)
          throw e
      }
      log.info("Kafka producer initialized successfully")

      log.info(s"Creating MLLP server on port ${cfg.port}...")
      val server: HL7Service = hapiCtx.newServer(cfg.port, cfg.useTls)
      serverRef.set(server) // expose to /healthz; isRunning() flips true at server.start()

      // Application handler
      val app = new ReceivingApplication[Message]:
        override def canProcess(in: Message): Boolean = true
        override def processMessage(in: Message, meta: java.util.Map[String, Object]): Message =
          Metrics.inFlight.inc()
          try
            // Parse quickly and avoid logging PHI
            val terser = new Terser(in)
            val key    = HL7MessageProcessor.messageKey(terser)
            val topic  = HL7MessageProcessor.topicName(cfg, terser)
            val mtype  = HL7MessageProcessor.messageType(terser)
            val info   = HL7MessageProcessor.messageInfo(terser)
            Metrics.received.labels(mtype).inc()

            log.info(s"Received HL7 message: key=$key struct=$info topic=$topic")

            val json    = HL7ToJsonConverter.convert(in, pipeParser, cfg.includeRaw)
            val headers = HL7MessageProcessor.headers(terser)
            // partition=null → let the partitioner hash the key (MSH-10).
            val rec     = new ProducerRecord[String, String](topic, (null: java.lang.Integer), key, json, headers)

            // sync send so we can decide ACK vs AE deterministically
            log.info(s"Sending to Kafka topic=$topic key=$key")
            val timer = Metrics.produceDuration.startTimer()
            // Wait at least the producer's own delivery budget: giving up
            // earlier does not cancel the send, it only makes the AE a lie.
            val md: RecordMetadata = producer.send(rec).get(cfg.kafkaAckWaitMs, TimeUnit.MILLISECONDS)
            timer.observeDuration()
            Metrics.produced.labels(md.topic).inc()
            // Only log metadata, never payload
            log.info(s"Produced to ${md.topic}@${md.partition} offset=${md.offset} key=$key struct=$info")

            log.info(s"Successfully processed message key=$key, sending ACK")
            Metrics.processed.labels("ack").inc()
            HL7AckGenerator.success(in)
          catch
            case e: Exception =>
              val reason = classify(e)
              Metrics.produceFailures.labels(reason).inc()
              Metrics.processed.labels("nak").inc()
              val errorMsg = reason match
                case "timeout"          =>
                  s"Kafka timeout (delivery budget ${cfg.kafkaDeliveryTimeoutMs}ms, ACK wait ${cfg.kafkaAckWaitMs}ms)"
                case "record_too_large" => "Message exceeds Kafka max.request.size — raise KAFKA_MAX_REQUEST_SIZE and the topic's max.message.bytes"
                case "hl7_parse"        => s"HL7 parsing error: ${Option(e.getMessage).getOrElse("Invalid message format")}"
                case _                  => s"Processing error: ${Option(e.getMessage).getOrElse(e.getClass.getSimpleName)}"
              log.error(errorMsg, e)
              HL7AckGenerator.error(in, errorMsg)
          finally
            Metrics.inFlight.dec()

      // register wildcard (any MSH-9)
      server.registerApplication("*", "*", app)
      log.info("Registered HL7 message handler for all message types (*/*)")

      Runtime.getRuntime.addShutdownHook(new Thread(() =>
        log.info("Shutting down...")
        Try(server.stopAndWait())
        Try(producer.flush())
        Try(producer.close(java.time.Duration.ofSeconds(5)))
        Try(hapiCtx.close())
        metricsServer.foreach(s => Try(s.stop(1)))
        log.info("Stopped.")
      ))

      log.info(s"Starting MLLP listener on port ${cfg.port}...")
      server.start()
      log.info(s"✓ MLLP listener is ready and accepting connections on port ${cfg.port}")

      // Keep main thread alive
      try
        Thread.currentThread().join()
      catch
        case _: InterruptedException =>
          log.info("Main thread interrupted, shutting down...")
    catch
      case e: Exception =>
        log.error("Fatal error during startup", e)
        System.exit(1)
