// Purpose: Volcano connector that listens on MLLP, parses HL7 v2 with HAPI to JSON, and writes to Kafka by message structure/type.

package volcano.hl7mllp

import ca.uhn.hl7v2.DefaultHapiContext
import ca.uhn.hl7v2.app.HL7Service
import ca.uhn.hl7v2.protocol.ReceivingApplication
import ca.uhn.hl7v2.model.Message
import ca.uhn.hl7v2.llp.MinLowerLayerProtocol
import ca.uhn.hl7v2.util.Terser
import ca.uhn.hl7v2.HL7Exception
import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}
import org.slf4j.LoggerFactory

import scala.util.Try

object Main:

  private val log = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit =
    try
      val cfg = Config.load()

      log.info(s"Starting Volcano HL7 MLLP connector on port ${cfg.port}, TLS=${cfg.useTls}, Kafka=${cfg.kafkaBootstrap}, prefix='${cfg.topicPrefix}', infix='${cfg.topicInfix}'")
      log.info(s"HL7 MLLP Charset configured: ${cfg.hl7Encoding}")
      log.info(s"Kafka topic naming strategy: ${cfg.kafkaTopicName} (${if cfg.kafkaTopicName == "message_structure" then "HL7 v2.5+" else "HL7 v2.x legacy"})")

      // Configure HAPI context with proper character encoding
      val hapiCtx = new DefaultHapiContext()
      val llp = new MinLowerLayerProtocol()
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

      // Application handler
      val app = new ReceivingApplication[Message]:
        override def canProcess(in: Message): Boolean = true
        override def processMessage(in: Message, meta: java.util.Map[String, Object]): Message =
          try
            // Parse quickly and avoid logging PHI
            val terser = new Terser(in)
            val key    = HL7MessageProcessor.messageKey(terser, cfg.kafkaTopicName)
            val topics = HL7MessageProcessor.topicNames(cfg.topicPrefix, cfg.topicInfix, terser, cfg.kafkaTopicName)
            val info   = HL7MessageProcessor.messageInfo(terser)

            log.info(s"Received HL7 message: key=$key struct=$info")

            val json = HL7ToJsonConverter.convert(in, pipeParser)
            topics.foreach { topic =>
              val rec = new ProducerRecord[String, String](topic, key, json)
              // sync send so we can decide ACK vs AE deterministically
              log.info(s"Sending to Kafka topic=$topic key=$key")
              val md: RecordMetadata = producer.send(rec).get(cfg.kafkaAcksTimeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS)
              // Only log metadata, never payload
              log.info(s"Produced to ${md.topic}@${md.partition} offset=${md.offset} key=$key struct=$info")
            }
            log.info(s"Successfully processed message key=$key, sending ACK")
            HL7AckGenerator.success(in)
          catch
            case e: java.util.concurrent.TimeoutException =>
              val errorMsg = s"Kafka timeout (>${cfg.kafkaAcksTimeoutMs}ms)"
              log.error(errorMsg, e)
              HL7AckGenerator.error(in, errorMsg)
            case e: HL7Exception =>
              val errorMsg = s"HL7 parsing error: ${Option(e.getMessage).getOrElse("Invalid message format")}"
              log.error(errorMsg, e)
              HL7AckGenerator.error(in, errorMsg)
            case e: Exception =>
              val errorMsg = s"Processing error: ${Option(e.getMessage).getOrElse(e.getClass.getSimpleName)}"
              log.error(errorMsg, e)
              HL7AckGenerator.error(in, errorMsg)

      // register wildcard (any MSH-9)
      server.registerApplication("*", "*", app)
      log.info("Registered HL7 message handler for all message types (*/*)")

      Runtime.getRuntime.addShutdownHook(new Thread(() =>
        log.info("Shutting down...")
        Try(server.stopAndWait())
        Try(producer.flush())
        Try(producer.close(java.time.Duration.ofSeconds(5)))
        Try(hapiCtx.close())
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
