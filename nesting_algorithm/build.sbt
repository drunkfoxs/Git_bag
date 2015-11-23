name := "Nesting-Algorithm"

version := "1.0"

scalaVersion := "2.10.3"

crossPaths := false


libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "1.5.0" % "provided",
  "org.apache.spark" % "spark-streaming_2.10" % "1.5.0" % "provided",
  "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.5.0",
  "org.elasticsearch" % "elasticsearch" % "1.7.0",
  "org.apache.kafka" % "kafka_2.10" % "0.8.2.1" ,
//  "org.elasticsearch" % "elasticsearch-hadoop" % "2.1.1",
  "com.mashape.unirest" % "unirest-java" % "1.4.7",
  "org.scalaj" % "scalaj-http_2.10" % "1.1.6"
)

