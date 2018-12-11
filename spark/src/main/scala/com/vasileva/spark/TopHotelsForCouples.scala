package com.vasileva.spark

import org.apache.spark.sql.functions.{col, lit, sum}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
  * Finds top three most popular hotels between couples.
  */
object TopHotelsForCouples {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Top hotels for couples")
      .getOrCreate

    // reading data
    val data = CsvIOUtils.readCsv(args(0), spark)

    // computing result
    val topHotelsForCouples = getTopThreeHotelsForCouples(data)

    // writing result
    CsvIOUtils.writeCsv(topHotelsForCouples, args(1))

    spark.close
  }

  /**
    * Find top three most popular hotels between couples.
    *
    * @param data hotel searches data
    * @return top three most popular hotels where adults number is 2
    */
  def getTopThreeHotelsForCouples(data: Dataset[Row]): Dataset[Row] = {
    data.filter(col("srch_adults_cnt") === "2")
      .select(col("hotel_continent"), col("hotel_country"), col("hotel_market"), lit(1).as("searches_count"))
      .groupBy(col("hotel_continent"), col("hotel_country"), col("hotel_market"))
      .agg(sum(col("searches_count")).as("searches_count"))
      .sort(col("searches_count").desc)
      .limit(3)
  }
}
