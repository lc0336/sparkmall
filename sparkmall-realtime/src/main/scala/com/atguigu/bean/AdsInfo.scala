package com.atguigu.bean

import java.sql.Date
import java.text.SimpleDateFormat

case class AdsInfo(ts: Long, area: String, city: String, userId: String, adsId: String) {
  val dayString: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date(ts))
  override def toString: String = s"$dayString:$area:$city:$adsId"


}
