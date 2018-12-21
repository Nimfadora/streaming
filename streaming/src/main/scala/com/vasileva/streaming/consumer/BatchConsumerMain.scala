package com.vasileva.streaming.consumer

import org.apache.spark.sql.functions.{col, max}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.parsing.json.JSONObject

/**
  * Reads data from Kafka topic in batch mode, saves it to HDFS
  */
object BatchConsumerMain {
  val logger: Logger = LoggerFactory.getLogger(classOf[SparkSession])

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("BatchConsumer")
      .getOrCreate()

    logger.info("Executor started")

    val topic = args(0)
    var offset = "earliest"
    var latestOffsetByPartition: Map[String, Long] = Map()
    var continue = true

    while (continue) {
      logger.info("Worker started")
      val data = spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
        .option("subscribe", topic)
        .option("startingOffsets", offset)
        .option("endingOffsets", "latest")
        .load()

      data.selectExpr("CAST(value AS STRING)")
        .write
        .option("quote", "")
        .mode(SaveMode.Append)
        .csv(args(1))

      latestOffsetByPartition = findLatestOffset(data)
      offset = mapOffsetToJson(topic, latestOffsetByPartition)
      logger.info("offsets: " + offset)

      logger.info("Worker finished")

      if (latestOffsetByPartition.isEmpty) {
        continue = false
      } else {
        Thread.sleep(2000)
      }
    }
  }

  /**
    * Convert offsets by partitions map to offset JSON string
    *
    * @param topic                   Kafka topic
    * @param latestOffsetByPartition offsets by partitions
    * @return offset JSON string for Kafka
    */
  def mapOffsetToJson(topic: String, latestOffsetByPartition: Map[String, Long]): String = {
    JSONObject(Map(topic -> JSONObject(latestOffsetByPartition))).toString()
  }

  /**
    * Find latest received data offset from Kafka
    *
    * @param data data received from kafka
    * @return map of latest received offsets + 1 by partitions
    */
  def findLatestOffset(data: DataFrame): Map[String, Long] = {
    data.selectExpr("CAST(partition AS INTEGER) AS partition", "CAST(offset AS LONG) AS offset", "CAST(value AS STRING) AS value")
      .filter(col("value").isNotNull && (col("value") =!= ""))
      .groupBy(col("partition"))
      .agg(max(col("offset")))
      .collect()
      .map(r => (r.getInt(0).toString, r.getLong(1) + 1))
      .toList.toMap
  }
}