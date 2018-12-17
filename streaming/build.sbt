name := "streaming"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.3.2" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.2" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-8" % "2.3.2" % "provided"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "1.0.0" % "provided"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"
libraryDependencies += "org.slf4j" % "slf4j-log4j12" % "1.7.25" % "test"