package com.vasileva.spark

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object CsvIOUtils {

  /**
    * Reads csv file to dataset with schema from header (default) or provided one
    * @param filename input filename
    * @param spark spark session
    * @param schema schema to use
    * @return dataset with data from file
    */
  def readCsv(filename: String, spark: SparkSession, schema: StructType = null): Dataset[Row] = {
    spark.read.schema(schema).option("header", "true").csv(filename)
  }

  /**
    * Writes dataset to csv with header
    * @param data data to be written
    * @param filename output file name
    */
  def writeCsv(data: Dataset[Row], filename: String): Unit = {
    data.coalesce(1).write.option("header", "true").csv(filename)
  }
}
