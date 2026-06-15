// Purpose: SBT config for Volcano HL7 MLLP → Kafka connector.

ThisBuild / scalaVersion := "3.3.7"
ThisBuild / version := "0.1.0"
ThisBuild / organization := "de.ume.volcano"

lazy val root = (project in file("."))
  .settings(
    name := "volcano-connector-hl7-mllp",
    libraryDependencies ++= Seq(
      "ca.uhn.hapi" % "hapi-base" % "2.6.0",
      "ca.uhn.hapi" % "hapi-structures-v25" % "2.6.0",
      "org.apache.kafka" % "kafka-clients" % "4.1.1",
      "org.slf4j" % "slf4j-api" % "2.0.17",
      "ch.qos.logback" % "logback-classic" % "1.5.22",
      "com.google.code.gson" % "gson" % "2.13.2",
      // Prometheus metrics + text exporter (HTTP server is the JDK's
      // com.sun.net.httpserver, no extra dep). hotspot = JVM/process metrics.
      "io.prometheus" % "simpleclient" % "0.16.0",
      "io.prometheus" % "simpleclient_common" % "0.16.0",
      "io.prometheus" % "simpleclient_hotspot" % "0.16.0",
      // Test framework.
      "org.scalameta" %% "munit" % "1.0.0" % Test
    ),
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "services", xs @ _*) => MergeStrategy.concat
      case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
      case PathList("META-INF", xs @ _*) if xs.lastOption.exists(_.endsWith(".SF")) => MergeStrategy.discard
      case PathList("META-INF", xs @ _*) if xs.lastOption.exists(_.endsWith(".DSA")) => MergeStrategy.discard
      case PathList("META-INF", xs @ _*) if xs.lastOption.exists(_.endsWith(".RSA")) => MergeStrategy.discard
      case PathList("META-INF", xs @ _*) => MergeStrategy.first
      case "module-info.class" => MergeStrategy.discard
      case x => MergeStrategy.first
    }
  )