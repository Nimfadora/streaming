package com.vasileva.streaming.producer

import org.apache.kafka.clients.producer._
import org.slf4j.{Logger, LoggerFactory}

/**
  * Continuously generating booking records and sending them to Kafka topic
  *
  * @param kafkaProducer Kafka producer
  * @param topic         Kafka topic
  * @param delayMs       delay between sends
  */
class BookingRecordGenerator(kafkaProducer: Producer[String, String], topic: String, delayMs: Long) extends Runnable {
  val logger: Logger = LoggerFactory.getLogger(classOf[BookingRecordGenerator])

  /**
    * Continuously sending records
    */
  override def run(): Unit = {
    while (!Thread.currentThread().isInterrupted) {
      logger.info("Record is submitted")
      send(BookingRecordFactory.generateRecord())
      Thread.sleep(delayMs)
    }
  }

  /**
    * Send record to Kafka topic
    *
    * @param record record to send
    */
  def send(record: String): Unit = {
    kafkaProducer.send(new ProducerRecord[String, String](topic, record), callback())
  }

  /**
    * Generates callback function that is called when record is sent or sending is failed
    *
    * @return callback
    */
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
