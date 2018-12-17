package com.vasileva.streaming

import java.util.Properties
import java.util.concurrent.Executors

class ProducerMain {
  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:6667")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new BookingRecordProducer(props)
    val threadsCount = args(0).toInt

    val executor = Executors.newFixedThreadPool(threadsCount)
    for( a <- 1 to threadsCount){

    }
    executor.submit()

  }
}
