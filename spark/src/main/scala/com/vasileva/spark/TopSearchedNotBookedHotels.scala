package com.vasileva.spark

import org.apache.spark.sql.functions.{col, lit, sum}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
  * Finds top 3 hotels where people with children are interested but not booked in the end.
  */
object TopSearchedNotBookedHotels {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Top not booked hotels")
      .getOrCreate

    // reading data
    val data = CsvIOUtils.readCsv(args(0), spark, schema)

    // computing result
    val topNotBookedHotels = getTopNotBookedHotelsByPeopleWithChildren(data)

    // writing result
    CsvIOUtils.writeCsv(topNotBookedHotels, args(1))

    spark.close
  }

  /**
    * Finds top 3 hotels where people with children are interested but not booked in the end.
    *
    * @param data hotel search requests
    * @return top three most popular not booked hotels within people with children
    */
  def getTopNotBookedHotelsByPeopleWithChildren(data: Dataset[Row]): Dataset[Row] = {
    data.filter(col("srch_children_cnt") > 0)
      .select(col("hotel_continent"), col("hotel_country"), col("hotel_market"),
        col("is_booking"), lit(1).as("searches_count"))
      .groupBy(col("hotel_continent"), col("hotel_country"), col("hotel_market"))
      .agg(sum(col("searches_count")).as("searches_count"), sum(col("is_booking")).as("bookings_count"))
      .filter(col("bookings_count") === 0)
      .select(col("hotel_continent"), col("hotel_country"), col("hotel_market"), col("searches_count"))
      .sort(col("searches_count").desc)
      .limit(3)
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
