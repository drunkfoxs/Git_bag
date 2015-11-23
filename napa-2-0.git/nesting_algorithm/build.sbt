
import sbt._
import Keys._
import sbtassembly.Plugin._
import AssemblyKeys._



assemblySettings

name := "Nesting Algorithm"

version := "1.0"

scalaVersion := "2.10.4"

javacOptions ++= Seq("-encoding","UTF-8")


libraryDependencies ++= Seq(
  ("org.apache.spark" % "spark-core_2.10" % "1.5.0" % "provided"),
  "org.elasticsearch" % "elasticsearch" % "1.7.0" % "provided"
)




