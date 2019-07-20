package com.atguigu

import com.atguigu.bean.AdsInfo
import com.atguigu.sparkmall.common.util.MyKafkaUtil
import com.atguigu.util.{BlackListApp, DayAreaAdsTop3App, DayAreaCityAdsApp}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object RealTimeApp {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "atguigu")
    // 1. 创建 SparkConf 对象
    val conf: SparkConf = new SparkConf()
      .setAppName("RealTimeApp")
      .setMaster("local[*]")
    // 2. 创建 SparkContext 对象
    val sc = new SparkContext(conf)
    // 3. 创建 StreamingContext
    val ssc = new StreamingContext(sc, Seconds(2))
    //        ssc.checkpoint("hdfs://hadoop201:9000/sparkmall0225")
    sc.setCheckpointDir("hdfs://hadoop103:9000/sparkmall")
    // 4. 得到 DStream
    val recordDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getDStream(ssc, "ads_log")

    val adsInfoDStream: DStream[AdsInfo] = recordDStream.map(record => {
      val msg: String = record.value() // 取出其中的value
      val arr: Array[String] = msg.split(",") // 切割并封装到 AdsInfo中
      AdsInfo(arr(0).toLong, arr(1), arr(2), arr(3), arr(4))
    })


    // 需求1: 将每天对某个广告点击超过 100 次的用户拉入黑名单。
    val filteredAdsInfoDSteam: DStream[AdsInfo] = BlackListApp.filterBlackList(ssc, adsInfoDStream)
    //BlackListApp.checkUserToBlackList(ssc, filteredAdsInfoDSteam)

    // 需求2:每天各地区各城市各广告的点击流量实时统计
     val lastDStream = DayAreaCityAdsApp.calcDayAreaCityAdsClickCount(ssc, filteredAdsInfoDSteam)


    // 需求3:每天每地区热门广告 Top3
    DayAreaAdsTop3App.calcDayAreaAdsTop3(ssc, lastDStream)

    ssc.start()
    ssc.awaitTermination()
  }
}
