package com.atguigu.app

import java.text.DecimalFormat

import com.atguigu.sparkmall.common.bean.UserVisitAction
import com.atguigu.sparkmall.common.util.JDBCUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object PageConversionApp {
  def calcPageConversion(spark: SparkSession, userVisitActionRDD: RDD[UserVisitAction], targetPageFlow: String, taskId: String): Unit ={
    //1，获取所有页面
    val targetPage = targetPageFlow.split(",")
    val par = targetPage.slice(0,targetPage.length-1)
    val post = targetPage.slice(1,targetPage.length)
    //2，获取符合跳转的页面情况
    val strings = par.zip(post).map {
      case (k, v) => k + "->" + v
    }

    //3，过滤出来目标页面的点击记录
    val rdd1 = userVisitActionRDD.filter(uva=>targetPage.contains(uva.page_id.toString))

    // 计算分母 [page, count]: 每个目标页面的点击次数   reduceByKey countByKey
    val rdd2 = rdd1.map(usa=>(usa.page_id,1)).countByKey()

    //计算分子
    val allTargetFlows = rdd1.groupBy(_.session_id).flatMap {
      case (sessionId, uvas) => {
        val sortedUVAList = uvas.toList.sortBy(_.action_time)
        val pre = sortedUVAList.slice(0, sortedUVAList.length - 1)
        val post = sortedUVAList.slice(1, sortedUVAList.length)
        val prePost = pre.zip(post)
        //存储每个session中所有的目标调转流
        val papeFlows = prePost.map {
          case (u1, u2) => u1.page_id + "->" + u2.page_id
        }

        //得到每个session中所有的目标调转流
        val allTargetFlows = papeFlows.filter(flow => strings.contains(flow))
        allTargetFlows
      }
    }.map((_, null)).countByKey()


    //计算页面跳转率
    val df = new DecimalFormat("0.00%")
    val resultMAP = allTargetFlows.map {
      case (flow, count) => {
        val page = flow.split("->")(0).toLong
        (flow, df.format(count.toDouble / rdd2(page)))
      }
    }

    val functionToTuples = resultMAP.toList.sortBy{
      case (x,y)=>{
        x.split("->")(0).toInt
      }
    }
    //写入到mysql数据库中
    val args = functionToTuples.map {
      case (flow, rate) => Array[Any](taskId, flow, rate)
    }
    JDBCUtil.executeUpdate("truncate page_conversion_rate", null)
    JDBCUtil.("insert into page_conversion_rate values(?, ?, ?)", args)
  }
}

/*
了解计算哪些页面:
	1,2,3,4,5,6,7

	1->2  / 1的页面的总的点击数
	2->3  / 2的页面...
		...


1. 过滤	RDD[UserVisitAction] , 只剩下目标页面的点击行为

2. 按照页面聚合, 求出来分母

3. 分子:
	假设: arr = Array[1,2,3,4,5,6,7]
	拼一个跳转流  "1->2"  "2->3"

	arr1 = 1,2,3,4,5,6
	arr2 = 2,3,4,5,6,7

	arr3 = arr1.zip(arr2)

	RDD[UserVisitAction] 按照session做分组, 每组内按照时间升序

	session1:   1,3,4
	session2:   1,2,3,4
	session3:   1,4,2,3,4

	rdd1 = 0- len-2
	rdd1 = 1- len-1

	rdd1.zip(rdd2).filter(...)

	最终想要的所有的跳转流. 按照调转流做分组, 分别统计...

 */