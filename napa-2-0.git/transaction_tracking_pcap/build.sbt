name := "TransactionTrackingbyPacket"

version := "1.0.0"

scalaVersion := "2.10.3"

EclipseKeys.createSrc := EclipseCreateSrc.Default + EclipseCreateSrc.Resource

unmanagedBase := baseDirectory.value / "lib"

libraryDependencies ++= Seq(
  "org.slf4j" % "slf4j-log4j12" % "1.7.6",
  "org.slf4j" % "slf4j-api" % "1.7.6",
  "org.apache.spark" % "spark-core_2.10" % "1.1.0" % "provided",
  "org.apache.spark" % "spark-streaming_2.10" % "1.1.0" % "provided",
  "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.1.0" excludeAll(
    ExclusionRule(organization = "org.eclipse.jetty.orbit"),
    ExclusionRule(organization = "org.apache.hadoop"),
    ExclusionRule(organization = "com.esotericsoftware.minlog")
  ),
  "org.apache.kafka" % "kafka_2.10" % "0.8.2.1",
  "org.apache.kafka" % "kafka-clients" % "0.8.2.1",
  "org.apache.avro" % "avro" % "1.7.7",
  "com.google.guava" % "guava" % "14.0.1",
  "org.pcap4j" % "pcap4j-core" % "1.4.0",
  "org.pcap4j" % "pcap4j-packetfactory-static" % "1.4.0",
  "org.apache.wink" % "wink-server" % "1.4",
  "org.apache.wink" % "wink-json4j" % "1.4",
  "org.apache.wink" % "wink-client" % "1.4",
  "org.json4s" %% "json4s-native" % "3.2.10",
  "org.json4s" %% "json4s-jackson" % "3.2.10"
)


