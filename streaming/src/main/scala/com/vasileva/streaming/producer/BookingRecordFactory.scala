package com.vasileva.streaming.producer

import java.util.Date
import java.util.concurrent.ThreadLocalRandom

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.commons.math3.util.Precision

/**
  * Generates booking record with random but realistic content
  */
object BookingRecordFactory {
  private val dateFmt = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
  private val intervalStart: Date = dateFmt.parse("2013-01-01 00:00:00")
  private val intervalEnd: Date = dateFmt.parse("2015-12-31 00:00:00")
  private val requestMaxDate: Date = dateFmt.parse("2015-11-31 00:00:00")
  private val MAX_INDEX = 10000

  /**
    * Generate random booking record.
    *
    * @return booking record comma-delimited string
    */
  def generateRecord(): String = {
    val random = ThreadLocalRandom.current
    val requestTime = new Date(random.nextLong(intervalStart.getTime, requestMaxDate.getTime))
    val bookingStart = new Date(random.nextLong(requestTime.getTime, intervalEnd.getTime))
    val bookingEnd = new Date(random.nextLong(bookingStart.getTime, intervalEnd.getTime))

    Array(dateFmt.format(requestTime), // date_time
      random.nextInt(0, MAX_INDEX), // site_name
      random.nextInt(0, MAX_INDEX), // posa_continent
      random.nextInt(0, MAX_INDEX), // user_location_country
      random.nextInt(0, MAX_INDEX), // user_location_region
      random.nextInt(0, MAX_INDEX), // user_location_city
      Precision.round(random.nextDouble(0, MAX_INDEX), 2), // orig_destination_distance
      random.nextInt(0, MAX_INDEX), // user_id
      random.nextInt(2), // is_mobile
      random.nextInt(2), // is_package
      random.nextInt(0, MAX_INDEX), // channel
      dateFmt.format(bookingStart), // srch_ci
      dateFmt.format(bookingEnd), // srch_co
      random.nextInt(0, MAX_INDEX), // srch_adults_cnt
      random.nextInt(0, MAX_INDEX), // srch_children_cnt
      random.nextInt(0, MAX_INDEX), // srch_rm_cnt
      random.nextInt(0, MAX_INDEX), // srch_destination_id
      random.nextInt(0, MAX_INDEX), // srch_destination_type_id
      random.nextInt(2), // is_booking
      random.nextInt(0, MAX_INDEX), // cnt
      random.nextInt(0, MAX_INDEX), // hotel_continent
      random.nextInt(0, MAX_INDEX), // hotel_country
      random.nextInt(0, MAX_INDEX), // hotel_market
      random.nextInt(0, MAX_INDEX) // hotel_cluster
    ).mkString(",")
  }
}
