package com.vasileva.streaming.producer

import java.util.Properties
import java.util.concurrent.{Executors, TimeUnit}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}

/**
  * Produces booking records in several threads and sends them to kafka topic specified
  */
object ProducerMain {

  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "sandbox-hdp.hortonworks.com:6667")
    props.put(ProducerConfig.ACKS_CONFIG, "all")
    props.put(ProducerConfig.RETRIES_CONFIG, "0")
    props.put(ProducerConfig.LINGER_MS_CONFIG, "1")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

    val threadsCount = args(0).toInt
    val topic = args(1)
    val sendDelay = args(2).toLong
    val terminationTimeout = args(3).toLong

    val executor = Executors.newFixedThreadPool(threadsCount)
    val kafkaProducer = new KafkaProducer[String, String](props)
    try {
      for (_ <- 1 to threadsCount) {
        executor.submit(new BookingRecordGenerator(kafkaProducer, topic, sendDelay))
      }
      executor.awaitTermination(terminationTimeout, TimeUnit.MINUTES)
    } finally {
      executor.shutdown()
      kafkaProducer.close()
    }
  }
}
