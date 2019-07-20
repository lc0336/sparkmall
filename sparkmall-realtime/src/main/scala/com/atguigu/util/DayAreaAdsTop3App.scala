package com.atguigu.util


import com.atguigu.sparkmall.common.util.RedisUtil
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.json4s.jackson.JsonMethods
import redis.clients.jedis.Jedis

object DayAreaAdsTop3App {

  val areaTop3 = "area:ads:top3"

  //每天每地区热门广告 Top3
  def calcDayAreaAdsTop3(ssc: StreamingContext, lastDStream: DStream[(String, Int)]) = {
    //1.去掉城市维度，然后再做聚合lastDStream[(2019-07-20:华南:深圳:2,1)]
    val dayAreaAdsCount = lastDStream.map {
      case (str, count) => {
        // (2019-07-20:华南:深圳:2)
        val Array(day, area, _, adsId) = str.split(":")
        (s"$day:$area:$adsId", count) //过滤掉城市维度
      }
    }.reduceByKey(_ + _) //获取每天，每个地区，每个广告的点击量
      .map {
      case (s, count) => {
        val Array(day, area, adsId) = s.split(":")
        (day, (area, (adsId, count)))
      }
    }

    //2.按照key进行分组  ，分出每天的数据 (2019-07-20(华南,2,1)):2号广告 1次
    val dayArea: DStream[(String, Iterable[(String, (String, Int))])] = dayAreaAdsCount.groupByKey
    val temp1: DStream[(String, Map[String, Iterable[(String, (String, Int))]])] = dayArea.map {
      case (day, it) => {
        (day, it.groupBy(_._1)) //按照地区分组
      }
    }


    //2.1 对数据结构做调整 排序 ，取前三  （day,[area,(adsId,count)]）
    val temp2: DStream[(String, Map[String, List[(String, Int)]])] = temp1.map {
      case (day, map1) => {
        val map2 = map1.map { // Map(华东 -> List((4,27), (1,26), (5,22))
          case (area, it) => (area, it.map(_._2).toList.sortBy(-_._2).take(3))
        }
        (day, map2)
      }
    }

    //3.写到mysql中
    //3.1 将list 转成json字符串(day,[area,(adsId,count)])
    val dayAreaAdsCountJson = temp2.map {
      case (day, map) => {
        val stringToTuples = map.map {
          case (area, adsCountList) => {
            //将list转换成json集合
            //fastjson 对java 的数据结构支持的比较好，对scala独有的结构支持的不好
            import org.json4s.JsonDSL._ // 提供隐式转换
            val jsonString: String = JsonMethods.compact(JsonMethods.render(adsCountList))
            (area, jsonString)
          }
        }
        (day, stringToTuples)
      }
    }

    //3.2 写入到redis中
    dayAreaAdsCountJson.foreachRDD(rdd => {
      rdd.foreachPartition(it => {
        //获取redis的客户端
        val client: Jedis = RedisUtil.getJedisClient
        import scala.collection.JavaConversions._//提供隐式转换函数
        it.foreach {
          case (day, map) => {
            client.hmset(areaTop3 + day, map)//将数据写入到redis中
          }
        }
        client.close()
      })
    })
  }
}

/*
key										value
area:ads:top3:2019-03-23				field				vlue(json格式的字符串)
										华南                {广告1: 1000, 广告2: 500}
										华北                {广告3: 1000, 广告1: 500}

数据来自上个需求

RDD[(s"$day:$area:$city:$adsId", 1)] .map
=> RDD[(s"$day:$area:$adsId", 1)] reduceByKey
=> RDD[(s"$day:$area:$adsId", count)] .map
=> RDD[(day, (area, (adsId, count)))]  .groupByKey
=> RDD[(day, Iterable[(area, (adsId, count))])] 对内部的Iterable做groupBy
=> RDD[(day, Map[area, Iterable[(area, (adsId, count))]])] .map


倒推:
=> RDD[(day, Map[area, Iterable[(adsId, count)])])] .map
=> RDD[(day, Map[(area, List[(adsId, count)])])] 排序, 取前3, 变成json格式
=> RDD[(day, Map[(area, "{adsId, count}")])]

=> client.hmset("area:ads:top3:"+day, Map[(area, "{adsId, count}")])
 */