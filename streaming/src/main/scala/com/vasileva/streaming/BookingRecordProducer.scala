package com.vasileva.streaming

import java.util.Properties

import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}

import scala.util.{Failure, Success}

class BookingRecordProducer(properties: Properties) extends Runnable {

  val producer = new KafkaProducer[String, String](properties)
  val topic: String = properties.get("kafka.topic").toString
  val delay: Long = properties.get("kafka.send.delay").toString.toLong

  override def run(): Unit = {
    while (!Thread.currentThread().isInterrupted) {
      send()
    }
  }

  def send(): Unit = {
    producer.send(new ProducerRecord[String, String](topic, BookingRecordFactory.generateRecord()), callback())
  }

  private def callback(): Callback = {
    new Callback {
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
        val result = if (exception == null) Success(metadata) else Failure(exception)
      }
    }
  }
}
