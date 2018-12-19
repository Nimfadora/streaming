package com.vasileva.streaming.producer

import org.apache.kafka.clients.producer._
import org.slf4j.{Logger, LoggerFactory}

class BookingRecordGenerator(kafkaProducer: Producer[String, String], topic: String, delayMs: Long) extends Runnable {
  val logger: Logger = LoggerFactory.getLogger(classOf[BookingRecordGenerator])

  override def run(): Unit = {
    while (!Thread.currentThread().isInterrupted) {
      logger.info("Record is submitted")
      send(BookingRecordFactory.generateRecord())
      Thread.sleep(delayMs)
    }
  }

  def send(record: String): Unit = {
    kafkaProducer.send(new ProducerRecord[String, String](topic, record), callback())
  }

  private def callback(): Callback = {
    new Callback {
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
        if (exception != null) {
          logger.error("Exception while sending record: " + exception.getMessage)
        }
      }
    }
  }
}
