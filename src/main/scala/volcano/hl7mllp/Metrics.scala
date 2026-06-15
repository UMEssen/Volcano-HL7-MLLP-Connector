package volcano.hl7mllp

import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import io.prometheus.client.{Counter, Gauge, Histogram, CollectorRegistry}
import io.prometheus.client.hotspot.DefaultExports
import io.prometheus.client.exporter.common.TextFormat
import org.slf4j.LoggerFactory

import java.io.StringWriter
import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets.UTF_8

// Prometheus metrics + a tiny HTTP exporter.
//
// Why this matters here: the k8s liveness/readiness probes are TCP-on-the-MLLP
// port, which stays green even while every message is being NAK'd because
// Kafka is unreachable (intentional — we want to keep accepting so the sender
// buffers and retries). That means pod health does NOT reflect pipeline
// health. These metrics are how you actually see it: alert on the produce
// failure rate, watch the produce latency, count ACK vs NAK.
object Metrics:

  private val log = LoggerFactory.getLogger(getClass)
  private val registry = CollectorRegistry.defaultRegistry

  val received: Counter = Counter.build()
    .name("hl7_messages_received_total")
    .help("HL7 messages received over MLLP, labelled by MSH-9.1 message type.")
    .labelNames("message_type")
    .register()

  val processed: Counter = Counter.build()
    .name("hl7_messages_processed_total")
    .help("HL7 messages processed, labelled by ACK result (ack|nak).")
    .labelNames("result")
    .register()

  val produced: Counter = Counter.build()
    .name("hl7_kafka_produced_total")
    .help("Records successfully produced to Kafka, labelled by topic.")
    .labelNames("topic")
    .register()

  val produceFailures: Counter = Counter.build()
    .name("hl7_kafka_produce_failures_total")
    .help("Produce failures by reason (timeout|record_too_large|hl7_parse|other).")
    .labelNames("reason")
    .register()

  val produceDuration: Histogram = Histogram.build()
    .name("hl7_kafka_produce_duration_seconds")
    .help("Time from send() to broker ack for successful produces.")
    .buckets(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30)
    .register()

  val inFlight: Gauge = Gauge.build()
    .name("hl7_messages_in_flight")
    .help("HL7 messages currently being processed.")
    .register()

  // Pre-create the common label values so the series exist (at 0) before the
  // first event — otherwise rate() alerts can't fire on a metric that has
  // never been observed.
  def warmup(): Unit =
    processed.labels("ack"); processed.labels("nak")
    produceFailures.labels("timeout")
    produceFailures.labels("record_too_large")
    produceFailures.labels("hl7_parse")
    produceFailures.labels("other")

  def start(port: Int): HttpServer =
    DefaultExports.initialize() // JVM + process metrics
    warmup()
    val server = HttpServer.create(new InetSocketAddress(port), 0)
    server.createContext("/metrics", new MetricsHandler)
    server.createContext("/healthz", new OkHandler)
    server.createContext("/", new OkHandler)
    server.setExecutor(null)
    server.start()
    log.info(s"Metrics/health server listening on :$port (/metrics, /healthz)")
    server

  private class MetricsHandler extends HttpHandler:
    override def handle(ex: HttpExchange): Unit =
      try
        val writer = new StringWriter()
        TextFormat.write004(writer, registry.metricFamilySamples())
        val bytes = writer.toString.getBytes(UTF_8)
        ex.getResponseHeaders.set("Content-Type", TextFormat.CONTENT_TYPE_004)
        ex.sendResponseHeaders(200, bytes.length.toLong)
        val os = ex.getResponseBody
        try os.write(bytes) finally os.close()
      catch
        case e: Exception =>
          log.warn("Failed to serve /metrics", e)
          ex.sendResponseHeaders(500, -1)
          ex.close()

  private class OkHandler extends HttpHandler:
    override def handle(ex: HttpExchange): Unit =
      val bytes = "ok".getBytes(UTF_8)
      ex.sendResponseHeaders(200, bytes.length.toLong)
      val os = ex.getResponseBody
      try os.write(bytes) finally os.close()
