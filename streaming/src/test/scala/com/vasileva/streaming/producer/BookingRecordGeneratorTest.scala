package com.vasileva.streaming.producer

import org.apache.kafka.clients.producer.{MockProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.scalatest.{BeforeAndAfter, FunSuite}

class BookingRecordGeneratorTest extends FunSuite with BeforeAndAfter {

  var kafkaProducer: MockProducer[String, String] = _

  before {
    kafkaProducer = new MockProducer[String, String](true, new StringSerializer(), new StringSerializer())
  }

  test("BookingRecordGenerator.testSend") {
    val recordGenerator = new BookingRecordGenerator(kafkaProducer, "test", 500)
    recordGenerator.send("test_record")

    val producedRecords = kafkaProducer.history()

    assert(producedRecords.size === 1)
    assert(producedRecords.get(0) === new ProducerRecord("test", "test_record"))
  }

  test("BookingRecordGenerator.testRecordGeneration") {
    val recordGenerator = new BookingRecordGenerator(kafkaProducer, "test", 500)
    val producerThread: Thread = new Thread(recordGenerator)
    producerThread.start()
    Thread.sleep(1000)
    producerThread.interrupt()
    val producedRecords = kafkaProducer.history()

    assert(producedRecords.size === 2)

    val record = producedRecords.get(0).value()
    val columns = record.split(",")

    assert(columns.size === 24)
    // check boolean fields
    assert(columns(8) === "1" || columns(8) === "0")
    assert(columns(9) === "1" || columns(9) === "0")
    assert(columns(18) === "1" || columns(18) === "0")
  }

}
