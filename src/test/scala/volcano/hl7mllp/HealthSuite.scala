package volcano.hl7mllp

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.util.concurrent.atomic.AtomicBoolean

// Exercises the metrics/health HTTP server end-to-end on an ephemeral port.
class HealthSuite extends munit.FunSuite:

  test("/healthz reflects the MLLP-liveness supplier; /metrics always serves") {
    val live   = new AtomicBoolean(true)
    val server = Metrics.start(0, () => live.get) // port 0 -> ephemeral
    try
      val port   = server.getAddress.getPort
      val client = HttpClient.newHttpClient()
      def status(path: String): Int =
        client
          .send(
            HttpRequest.newBuilder(URI.create(s"http://localhost:$port$path")).build(),
            HttpResponse.BodyHandlers.ofString()
          )
          .statusCode()

      assertEquals(status("/healthz"), 200, "live -> 200")
      assertEquals(status("/metrics"), 200, "metrics always 200")

      live.set(false)
      assertEquals(status("/healthz"), 503, "not live -> 503")
      assertEquals(status("/metrics"), 200, "metrics still served when not live")
    finally server.stop(0)
  }
