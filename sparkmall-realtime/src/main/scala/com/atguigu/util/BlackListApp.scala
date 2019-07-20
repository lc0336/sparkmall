package com.atguigu.util

import com.atguigu.bean.AdsInfo
import com.atguigu.sparkmall.common.util.RedisUtil
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object BlackListApp {

  //redis 的一些相关参数
  val countKey = "day:userID:adsId"
  val blackList = "blacklist"

  //检查用户是否要加入黑名单
  def checkUserToBlackList(ssc: StreamingContext, adsInfoDStream: DStream[AdsInfo]) = {
    adsInfoDStream.foreachRDD(rdd => {
      rdd.foreachPartition(adsInfoIt => {
        //1.统计每天每用户每广告的点击量
        val client: Jedis = RedisUtil.getJedisClient //jedis 只能在executor 端创建，不能再driver端创建
        adsInfoIt.foreach(adsInfo => {
          val field = s"${adsInfo.dayString}:${adsInfo.userId}:${adsInfo.adsId}"
          //返回就是增加后的值  count=1 所有点击量
          val clickCount = client.hincrBy(countKey, field, 1)

          //2.加入黑名单
          //val clickCount = client.hget(countKey, field)
          if (clickCount.toLong > 1000000) {//判断是否大于100 大于100 加入到blacklist 中
            client.sadd(blackList, adsInfo.userId)
          }
        })
        client.close()//不是真正的关闭连接, 而是把这个连接交个连接池管理
      })
    })
  }

  def filterBlackList(ssc:StreamingContext,adsInfoDStream: DStream[AdsInfo])={
    //黑名单再实时更新，所以黑名单也应该实时的获取，一个周期获取一次黑名单
    adsInfoDStream.transform(rdd=>{
      val client = RedisUtil.getJedisClient
      //使用广播变量，获取已经放入黑名单的用户  在每个executor 中获取
      val blackBC = ssc.sparkContext.broadcast(client.smembers(blackList))
      client.close()
      //过滤掉放入广播变量中的用户
      rdd.filter(adsInfo=>{
        val blackListDs = blackBC.value
        !blackListDs.contains(adsInfo.userId)//去除在广播变量中的用户
      })
    })
  }
}