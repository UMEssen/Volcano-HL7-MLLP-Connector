// Purpose: SBT config for Volcano HL7 MLLP → Kafka connector.

ThisBuild / scalaVersion := "3.3.3"
ThisBuild / version := "0.1.0"
ThisBuild / organization := "de.ume.volcano"

lazy val root = (project in file("."))
  .settings(
    name := "volcano-connector-hl7-mllp",
    libraryDependencies ++= Seq(
      "ca.uhn.hapi" % "hapi-base" % "2.3",
      "ca.uhn.hapi" % "hapi-structures-v25" % "2.3",
      "org.apache.kafka" % "kafka-clients" % "3.7.0",
      "org.slf4j" % "slf4j-api" % "2.0.13",
      "ch.qos.logback" % "logback-classic" % "1.5.7" % Runtime,
      "com.google.code.gson" % "gson" % "2.11.0"
    ),
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case x => MergeStrategy.first
    }
  )