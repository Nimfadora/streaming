package com.vasileva.streaming

import java.util.Date
import java.util.concurrent.ThreadLocalRandom

import org.apache.commons.lang3.time.FastDateFormat

object BookingRecordFactory {
  private val dateFmt = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
  private val intervalStart: Date = dateFmt.parse("2013-01-01 00:00:00")
  private val intervalEnd: Date = dateFmt.parse("2015-12-31 00:00:00")
  private val requestMaxDate: Date = dateFmt.parse("2015-11-31")

  def generateRecord(): String = {
    val random = ThreadLocalRandom.current
    val requestTime = new Date(random.nextLong(intervalStart.getTime, requestMaxDate.getTime))
    val bookingStart = new Date(random.nextLong(requestTime.getTime, intervalEnd.getTime))
    val bookingEnd = new Date(random.nextLong(bookingStart.getTime, intervalEnd.getTime))

    Array(dateFmt.format(requestTime), // date_time
      random.nextInt, // site_name
      random.nextInt, // posa_continent
      random.nextInt, // user_location_country
      random.nextInt, // user_location_region
      random.nextInt, // user_location_city
      random.nextDouble, // orig_destination_distance
      random.nextInt, // user_id
      random.nextInt(2), // is_mobile
      random.nextInt(2), // is_package
      random.nextInt, // channel
      dateFmt.format(bookingStart), // srch_ci
      dateFmt.format(bookingEnd), // srch_co
      random.nextInt, // srch_adults_cnt
      random.nextInt, // srch_children_cnt
      random.nextInt, // srch_rm_cnt
      random.nextInt, // srch_destination_id
      random.nextInt, // srch_destination_type_id
      random.nextInt(2), // is_booking
      random.nextLong, // cnt
      random.nextInt, // hotel_continent
      random.nextInt, // hotel_country
      random.nextInt, // hotel_market
      random.nextInt // hotel_cluster
    ).mkString
  }

}
