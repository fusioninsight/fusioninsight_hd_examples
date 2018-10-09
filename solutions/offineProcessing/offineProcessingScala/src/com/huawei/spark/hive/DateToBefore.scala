package com.huawei.spark.hive

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
object DateToBefore  {

  def beforeTime(): Long = {
    val calender = Calendar.getInstance()
    calender.add(Calendar.MONTH,-6)
    val timeMillis = calender.getTimeInMillis
    return timeMillis
  }

  def transformDate(strTime:String):Long = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val timeMillis = dateFormat.parse(strTime).getTime
    return timeMillis
  }
}