package com.vasileva.streaming.consumer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

/**
  * Streams data from Kafka and saves it to HDFS
  */
object StreamingConsumerMain {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("StreamingConsumer")
      .getOrCreate()

    val query = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
      .option("subscribe", args(0))
      .option("startingOffsets", "earliest")
      .load()
      .selectExpr("CAST(value AS STRING)")
      .writeStream
      .trigger(Trigger.ProcessingTime("2 seconds"))
      .format("csv")
      .outputMode(OutputMode.Append())
      .option("checkpointLocation", "/user/root/tmp/spark-streaming/checkpoint")
      .option("path", args(1))
      .option("quote", "")
      .start()
    query.processAllAvailable()
  }
}
