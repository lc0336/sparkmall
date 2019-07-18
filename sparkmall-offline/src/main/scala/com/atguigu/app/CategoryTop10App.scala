package com.atguigu.app

import com.atguigu.acc.MapAcc
import com.atguigu.sparkmall.common.bean.UserVisitAction
import com.atguigu.util.{CategoryCountInfo, JDBCUtil}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object CategoryTop10App {
  // 统计   计算
  def statCategoryTop10(spark: SparkSession, userVisitActionRDD: RDD[UserVisitAction], taskId: String):List[CategoryCountInfo] = {

    val acc = new MapAcc
    spark.sparkContext.register(acc)
    userVisitActionRDD.foreach(action => {
      acc.add(action)
    })
    //获取前10
    val sortedList = acc.value.toList.sortBy {
      case (_, (c1, c2, c3)) => (-c1, -c2, -c3)
    }.take(10)

    //改变数据结构
    val argsList: List[CategoryCountInfo] = sortedList.map {
      case (k, v) => CategoryCountInfo(
        taskId,
        k,
        v._1,
        v._2,
        v._3
      )
    }

    val args = argsList.map(info => Array(info.taskId, info.categoryId, info.clickCount, info.orderCount, info.payCount))
    //插入数据库
    JDBCUtil.executeUpdate("truncate table category_top10", null)
    JDBCUtil.executeBatchUpdate("insert into category_top10 values(?, ?, ?, ?, ?)", args)

    argsList

  }
}
