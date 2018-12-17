package com.vasileva.streaming

import java.util.Properties
import java.util.concurrent.{Executors, TimeUnit}

import org.apache.kafka.clients.producer.KafkaProducer

class ProducerMain {
  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:6667")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val threadsCount = args(0).toInt
    val topic = args(1)
    val sendDelay = args(2).toLong
    val terminationTimeout = args(3).toLong

    val kafkaProducer = new KafkaProducer[String, String](props)
    val executor = Executors.newFixedThreadPool(threadsCount)

    for (_ <- 1 to threadsCount) {
      executor.submit(new BookingRecordGenerator(kafkaProducer, topic, sendDelay))
    }

    executor.awaitTermination(terminationTimeout, TimeUnit.MINUTES)
  }
}
