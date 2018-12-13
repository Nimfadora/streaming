package com.vasileva.spark

import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.{BeforeAndAfter, FunSuite}

class TopSearchedNotBookedHotelsTest extends FunSuite with BeforeAndAfter {

  var spark: SparkSession = _

  before {
    spark = SparkSession.builder.appName("Top not booked hotels").master("local[*]").getOrCreate
  }

  after {
    spark.close
  }

  test("TopSearchedNotBookedHotels.getTopNotBookedHotelsByPeopleWithChildren") {
    val filename = getClass.getResource("/top_searched_not_booked_hotels.csv").getPath
    val data = CsvIOUtils.readCsv(filename, spark)
    val topHotels = TopSearchedNotBookedHotels.getTopNotBookedHotelsByPeopleWithChildren(data).collect
    val expectedTopHotels = Array(
      Row.apply("3", "151", "69", 17),
      Row.apply("2", "50", "680", 8),
      Row.apply("2", "50", "675", 6))

    assert(topHotels.length === 3)
    assert(topHotels.deep === expectedTopHotels.deep)
  }

  test("TopSearchedNotBookedHotels.noSearchRequestsFromPeopleWithChildren") {
    val filename = getClass.getResource("/no_children.csv").getPath
    val data = CsvIOUtils.readCsv(filename, spark)
    assert(TopSearchedNotBookedHotels.getTopNotBookedHotelsByPeopleWithChildren(data).count === 0)
  }
}
