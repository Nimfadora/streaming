package com.vasileva.streaming.consumer

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.{BeforeAndAfter, FunSuite}

class BatchConsumerTest extends FunSuite with BeforeAndAfter {

  val schema = new StructType(Array(
    StructField("key", StringType, nullable = true),
    StructField("value", StringType, nullable = true),
    StructField("topic", StringType, nullable = true),
    StructField("partition", IntegerType, nullable = true),
    StructField("offset", LongType, nullable = true),
    StructField("timestamp", TimestampType, nullable = true),
    StructField("timestampType", IntegerType, nullable = true)
  ))

  var spark: SparkSession = _

  before {
    spark = SparkSession.builder.appName("Batch consumer test").master("local[*]").getOrCreate
  }

  after {
    spark.close
  }

  test("BatchConsumerMain.findLatestOffset") {
    val filename = getClass.getResource("/batch.csv").getPath
    val data = spark.read.schema(schema).csv(filename)
    val offsetMap = BatchConsumerMain.findLatestOffset(data)
    assert(offsetMap.nonEmpty)
    assert(offsetMap === Map("0" -> 176))
  }

  test("BatchConsumerMain.findLatestOffsetNoDataPassed") {
    val data = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
    val offsetMap = BatchConsumerMain.findLatestOffset(data)
    assert(offsetMap.isEmpty)
  }

  test("BatchConsumerMain.mapOffsetToJson") {
    val offsetString = BatchConsumerMain.mapOffsetToJson("topic", Map("0" -> 100, "1" -> 200))
    assert(offsetString === "{\"topic\" : {\"0\" : 100, \"1\" : 200}}")
  }

  test("BatchConsumerMain.mapOffsetToJsonEmptyMapPassed") {
    val offsetString = BatchConsumerMain.mapOffsetToJson("topic", Map())
    assert(offsetString === "{\"topic\" : {}}")
  }
}
