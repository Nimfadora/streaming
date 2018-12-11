package com.vasileva.spark

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FunSuite}

class MostBookedByLocalsCountryTest extends FunSuite with BeforeAndAfter {

  var spark: SparkSession = _

  before {
    spark = SparkSession.builder.appName("Most booked by locals country").master("local[*]").getOrCreate
  }

  after {
    spark.close
  }

  test("MostBookedByLocalsCountry.getMostBookedByLocalsCountry") {
    val filename = getClass.getResource("/most_booked_by_locals_country.csv").getPath
    val data = CsvIOUtils.readCsv(filename, spark)
    val country = MostBookedByLocalsCountry.getMostBookedByLocalsCountry(data).collect
    assert(country.length === 1)
    assert(country(0).toString === "[151,10]")
  }

  test("MostBookedByLocalsCountry.noSearchRequestsFromLocals") {
    val filename = getClass.getResource("/not_local_bookings_only.csv").getPath
    val data =  CsvIOUtils.readCsv(filename, spark)
    assert(MostBookedByLocalsCountry.getMostBookedByLocalsCountry(data).collect.isEmpty)
  }
}
