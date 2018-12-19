package com.vasileva.streaming.consumer

import com.vasileva.streaming.producer.BookingRecordGenerator
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

class BatchConsumerJob(spark: SparkSession, topic: String, filename: String) extends Runnable {
  val logger: Logger = LoggerFactory.getLogger(classOf[BookingRecordGenerator])

  override def run(): Unit = {
    logger.info("Worker started")
    spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
      //ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG
      //      .option(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
      //      .option(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .option("endingOffsets", "latest")
      .load()
      .selectExpr("CAST(value AS STRING)")
      .write
      .option("quote", "")
      .mode(SaveMode.Append)
      .csv(filename)
    logger.info("Worker finished")
  }
}
