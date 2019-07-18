package com.atguigu.app

import com.atguigu.sparkmall.common.bean.UserVisitAction
import com.atguigu.util.CategoryCountInfo
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object CategorySessionApp {
  def statCategoryTop10Session(spark: SparkSession, categoryTop10: List[CategoryCountInfo],
                               userVisitActionRDD: RDD[UserVisitAction], taskId: String): Unit = {

    //1.过滤掉category 不在前10的日志
    //1.1 得到top10 的到 categoryId
    val categoryIdTop10 = categoryTop10.map(_.categoryId)
    val categoryIdTop10DB = spark.sparkContext.broadcast(categoryIdTop10)

  }
}
