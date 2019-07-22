package com.atguigu.util

import java.text.SimpleDateFormat

import com.atguigu.bean.AdsInfo
import com.atguigu.sparkmall.common.util.RedisUtil
import org.apache.spark.streaming.{Minutes, Seconds}
import org.apache.spark.streaming.dstream.DStream
import org.json4s.jackson.JsonMethods
import redis.clients.jedis.Jedis

object LastHourAdsClickApp {


  // 最近 1 小时广告点击量实时统计
  def statLastHourAdsClick(filteredDStream: DStream[AdsInfo]) = {
    //1.设置窗口步长
    val windowDStream = filteredDStream.window(Minutes(60), Seconds(6))
    //1.1转换数据结构
    val groupAdsCountDstream = windowDStream.map(ads => {
      ((ads.adsId, ads.houreMinutes), 1)
    }).reduceByKey(_ + _).map {
      //格式调整
      case ((adsId, houreMinutes), count) => (adsId, (houreMinutes, count))
    }.groupByKey //统计单个广告的点击量


    //2.转换数据格式
    val jsonCountDStream = groupAdsCountDstream.map {
      case (adsId, it) => {
        //将list转换成json集合
        //fastjson 对java 的数据结构支持的比较好，对scala独有的结构支持的不好
        import org.json4s.JsonDSL._
        //隐式转换
        val hourMinutesJson: String = JsonMethods.compact(JsonMethods.render(it))
        (adsId, hourMinutesJson)
      }
    }

    //3. 将数据写入到redis中
    jsonCountDStream.foreachRDD(rdd => {
      //      val result = rdd.collect//获取数据
      //      import collection.JavaConversions._//隐式转换
      //      val client: Jedis = RedisUtil.getJedisClient
      //      client.hmset("last_hour_ads_click", result.toMap)//数据已hash的形式插入到redis中
      //      client.close()

      rdd.foreachPartition{rdd=>
        val client = RedisUtil.getJedisClient
        rdd.foreach{
          case (adsId,click)=>
          client.hset("last_hour_ads_click",adsId,click)
        }
        client.close()
      }
    })
  }
}
