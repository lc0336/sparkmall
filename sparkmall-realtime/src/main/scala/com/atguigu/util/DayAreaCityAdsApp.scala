package com.atguigu.util

import java.text.SimpleDateFormat
import java.util.Date

import com.atguigu.bean.AdsInfo
import com.atguigu.sparkmall.common.util.RedisUtil
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object DayAreaCityAdsApp {

  val key = "day:area:city:ads"

  def calcDayAreaCityAdsClickCount(ssc: StreamingContext, filteredAdsInfoDSteam: DStream[AdsInfo]) = {

    //1.统计结果,未分组排序的数据
    val adsInfoOne = filteredAdsInfoDSteam.map {
      //AdsInfo(时间,用户,区域,_,广告)
      case AdsInfo(ts, area, city, _, adsId) => {
        val day: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date(ts))
        (s"$day:$area:$city:$adsId", 1)
      }
    }
    //有状态转换     (seq: Seq[Int], option: Option[Int]) seq：表示前面所有的值的；option：表示后面进入的数据集合
    val resultDStream = adsInfoOne.updateStateByKey((seq: Seq[Int], option: Option[Int]) => {
      Some(seq.sum + option.getOrElse(0))//getOrElse 防止出现null 有就取值，没有就给0
    })

    //2.写到redis中  resultDStream已经经过bykey了
    resultDStream.foreachRDD(rdd => {
      rdd.foreachPartition(it => {
        //foreachPartition 取除来的数据是一个Iterator集合 需要遍历
        //建立redis来连接
        val client: Jedis = RedisUtil.getJedisClient
        val list: List[(String, Int)] = it.toList
        list.foreach {
          case (field, value) => {
            client.hset(key, field, value.toString)
          }
        }
        client.close()
      })
    })

    resultDStream  //给后面需求使用
  }
}
/*
每天各地区各城市各广告的点击流量实时统计
day:area:city:ads

redis key value 设计

key								value(hash)
"day:area:city:ads"				field						value
								2019-07-20:华北:北京:1		10

----

DStream[AdsInfo]
=> DSteam[("2019-07-20:华北:北京:1", 1)]  .reduceByKey

=> DSteam[("2019-07-20:华北:北京:1", 100)]

=> 向redis写入
 */