package com.vasileva.spark

import java.io.File

import org.apache.spark.sql.types._
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.scalatest.{BeforeAndAfter, FunSuite}

import scala.io.Source

class CsvIOUtilsTest extends FunSuite with BeforeAndAfter {
  var tmpFolder: File = _
  var spark: SparkSession = _

  before {
    spark = SparkSession.builder.appName("Most booked by locals country").master("local[*]").getOrCreate
    tmpFolder = createTmpFolder
  }

  after {
    spark.close

    if (tmpFolder.exists) {
      tmpFolder.listFiles.foreach(file => file.delete)
      tmpFolder.delete
    }
  }

  test("CsvIOUtils.readCsv") {
    assert(CsvIOUtils.readCsv(getClass.getResource("/ioutils/dataset.csv").getPath, spark).count === 99)
  }

  test("CsvIOUtils.inputDoesNotExist") {
    val ex = intercept[AnalysisException] {
      CsvIOUtils.readCsv("/outils/file_does_not_exist.csv", spark)
    }
    assert(ex.getMessage.startsWith("Path does not exist:"))
    assert(ex.getMessage.contains("file_does_not_exist.csv"))
  }

  test("CsvIOUtils.readEmptyDataset") {
    val filename = getClass.getResource("/ioutils/empty_dataset.csv").getPath
    assert(CsvIOUtils.readCsv(filename, spark).count === 0)
  }

  test("CsvIOUtils.readEmptyFile") {
    val filename = getClass.getResource("/ioutils/empty_file.csv").getPath
    assert(CsvIOUtils.readCsv(filename, spark).count === 0)
  }

  test("CsvIOUtils.readCsvWithSchema") {
    val filename = getClass.getResource("/ioutils/dataset.csv").getPath
    assert(CsvIOUtils.readCsv(filename, spark, schema).schema === schema)
  }

  test("CsvIOUtils.readCsvWithWrongSchema") {
    val filename = getClass.getResource("/ioutils/wrong_schema.csv").getPath
    val data = CsvIOUtils.readCsv(filename, spark, schema)

    assert(data.schema === schema)
    assert(data.count === 99)
    assert(data.take(1)(0).toString === "[null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null]" )
  }

  test("CsvIOUtils.writeCsv") {
    val filename = new File(tmpFolder, "result.csv")
    val ss = spark
    import ss.implicits._
    val data = Seq(("Max", 33), ("Adam", 32), ("Muller", 62))

    CsvIOUtils.writeCsv(data.toDF("name", "age"), filename.getPath)

    // spark create some metadata, so we should filter
    val resultFiles = filename.listFiles.filter(f => f.getName.endsWith(".csv"))
    val expected = Seq("name,age", "Max,33", "Adam,32", "Muller,62").iterator

    assert(resultFiles.length === 1)
    assert(expected.zip(Source.fromFile(resultFiles(0)).getLines).forall(x => x._1 == x._2))
  }

  test("CsvIOUtils.writeEmptyDataset") {
    val filename = new File(tmpFolder, "result.csv")
    CsvIOUtils.writeCsv(spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema), filename.getPath)
    assert(!filename.listFiles.exists(f => f.getName.endsWith(".csv")))
  }

  test("CsvIOUtils.outputAlreadyExists") {
    val filename = new File(tmpFolder, "result.csv")
    filename.createNewFile

    val ex = intercept[AnalysisException] {
      val ss = spark
      import ss.implicits._
      val data = Seq(("Max", 33), ("Adam", 32), ("Muller", 62))

      CsvIOUtils.writeCsv(data.toDF("name", "age"), filename.getPath)
    }
    assert(ex.getMessage.contains("already exists"))
    assert(ex.getMessage.contains("result.csv"))


  }

  def createTmpFolder: File = {
    val javaTmpFolder = System.getProperty("java.io.tmpdir")
    var folder: File = null

    do {
      folder = new File(javaTmpFolder, "scalatest-" + System.nanoTime)
    }
    while (!folder.mkdir)
    folder
  }

  val schema = new StructType(Array(
    StructField("date_time", StringType, nullable = true),
    StructField("site_name", IntegerType, nullable = true),
    StructField("posa_continent", IntegerType, nullable = true),
    StructField("user_location_country", IntegerType, nullable = true),
    StructField("user_location_region", IntegerType, nullable = true),
    StructField("user_location_city", IntegerType, nullable = true),
    StructField("orig_destination_distance", DoubleType, nullable = true),
    StructField("user_id", IntegerType, nullable = true),
    StructField("is_mobile", BooleanType, nullable = true),
    StructField("is_package", BooleanType, nullable = true),
    StructField("channel", IntegerType, nullable = true),
    StructField("srch_ci", StringType, nullable = true),
    StructField("srch_co", StringType, nullable = true),
    StructField("srch_adults_cnt", IntegerType, nullable = true),
    StructField("srch_children_cnt", IntegerType, nullable = true),
    StructField("srch_rm_cnt", IntegerType, nullable = true),
    StructField("srch_destination_id", IntegerType, nullable = true),
    StructField("srch_destination_type_id", IntegerType, nullable = true),
    StructField("is_booking", IntegerType, nullable = true),
    StructField("cnt", StringType, nullable = true),
    StructField("hotel_continent", IntegerType, nullable = true),
    StructField("hotel_country", IntegerType, nullable = true),
    StructField("hotel_market", IntegerType, nullable = true),
    StructField("hotel_cluster", IntegerType, nullable = true)
  ))
}
