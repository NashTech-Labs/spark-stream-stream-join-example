organization := "knoldus"

name := "stream-join"

version := "0.1"

scalaVersion := "2.11.0"

libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % "2.4.3",
  "org.apache.spark" %% "spark-streaming" % "2.4.3",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.3",
  "org.apache.spark" %% "spark-sql" % "2.4.3",
  "net.manub" %% "scalatest-embedded-kafka" % "2.0.0" % Test,
  "org.scalatest" %% "scalatest" % "2.2.2" % Test
)

