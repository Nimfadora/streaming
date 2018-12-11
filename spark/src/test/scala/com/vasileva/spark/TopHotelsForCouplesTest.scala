package com.vasileva.spark

import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.{BeforeAndAfter, FunSuite}

class TopHotelsForCouplesTest extends FunSuite with BeforeAndAfter {

  var spark: SparkSession = _

  before {
    spark = SparkSession.builder.appName("Top hotels for couples").master("local[*]").getOrCreate
  }

  after {
    spark.close
  }

  test("TopHotelsForCouples.getTopThreeHotelsForCouples") {
    val filename = getClass.getResource("/top_hotels_for_couples.csv").getPath
    val data = CsvIOUtils.readCsv(filename, spark)
    val topHotels = TopHotelsForCouples.getTopThreeHotelsForCouples(data).collect
    val expectedTopHotels = Array(
      Row.apply("3", "151", "69", 37),
      Row.apply("2", "50", "680", 10),
      Row.apply("2", "50", "191", 9))

    assert(topHotels.length === 3)
    assert(topHotels.deep === expectedTopHotels.deep)
  }

  test("TopHotelsForCouples.noSearchRequestsFromCouples") {
    val filename = getClass.getResource("/no_couples.csv").getPath
    val data = CsvIOUtils.readCsv(filename, spark)
    assert(TopHotelsForCouples.getTopThreeHotelsForCouples(data).count === 0)
  }
}
