package com.vasileva.spark

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lit, max, sum}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
  * Finds most popular country where hotels are booked and searched from the same country.
  */
object MostBookedByLocalsCountry {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Most booked by locals country")
      .getOrCreate

    // reading data
    val data = CsvIOUtils.readCsv(args(0), spark)

    // computing result
    val mostBookedByLocalsCountry = getMostBookedByLocalsCountry(data)

    // writing result
    CsvIOUtils.writeCsv(mostBookedByLocalsCountry, args(1))

    spark.close
  }

  /**
    * Finds most popular country where hotels are booked and searched from the same country.
    *
    * @param data hotel searches data
    * @return most popular country with number of bookings from the same country
    */
  def getMostBookedByLocalsCountry(data: Dataset[Row]): Dataset[Row] = {
    data.filter(col("is_booking") === "1")
      .filter(col("user_location_country") === col("hotel_country"))
      .select(col("hotel_country"), lit(1).as("bookings_count"))
      .groupBy(col("hotel_country")).agg(sum(col("bookings_count")).as("bookings_count"))
      .withColumn("max_booking", max(col("bookings_count")).over(Window.partitionBy()))
      .filter(col("bookings_count") === col("max_booking"))
      .drop(col("max_booking"))
  }
}
