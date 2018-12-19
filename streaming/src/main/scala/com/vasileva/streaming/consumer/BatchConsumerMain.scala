package com.vasileva.streaming.consumer

import java.util.concurrent.{Executors, TimeUnit}

import com.vasileva.streaming.producer.BookingRecordGenerator
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

object BatchConsumerMain {
  val logger: Logger = LoggerFactory.getLogger(classOf[BookingRecordGenerator])

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("BatchConsumer")
      .getOrCreate()
    val consumerJob = new BatchConsumerJob(spark, args(0), args(1))

    logger.info("Executor started")
    val executor = Executors.newSingleThreadScheduledExecutor()
    executor.scheduleAtFixedRate(consumerJob, 0, 5, TimeUnit.SECONDS)
    executor.awaitTermination(args(2).toLong, TimeUnit.MINUTES)
    spark.stop()
  }
}
