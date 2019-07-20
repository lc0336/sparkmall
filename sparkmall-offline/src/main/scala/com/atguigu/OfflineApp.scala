package com.atguigu

import java.util.UUID

import com.alibaba.fastjson.JSON
import com.atguigu.app.{AreaProductTop3, CategorySessionApp, CategoryTop10App, PageConversionApp}
import com.atguigu.sparkmall.common.bean.UserVisitAction
import com.atguigu.sparkmall.common.util.ConfigurationUtil
import com.atguigu.util.{CategoryCountInfo, Condition}
import org.apache.ivy.core.module.descriptor.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object OfflineApp {

  def readUserVisitActionRDD(spark: SparkSession, condition: Condition): RDD[UserVisitAction] = {
    var sql =
      s"""
         |select
         | v.*
         |from user_visit_action v join user_info u on v.user_id=u.user_id
         |where 1=1
             """.stripMargin

    if (isNotEmpty(condition.startDate)) {
      sql += s" and date>='${condition.startDate}'"
    }
    if (isNotEmpty(condition.endDate)) {
      sql += s" and date<='${condition.endDate}'"
    }
    if (condition.startAge > 0) {
      sql += s" and u.age>=${condition.startAge}"
    }
    if (condition.endAge > 0) {
      sql += s" and u.age<=${condition.endAge}"
    }
    import spark.implicits._
    spark.sql("use sparkmall")
    spark.sql(sql).as[UserVisitAction].rdd
  }

  /**
    * 读取过滤条件
    */
  def readCondition: Condition = {
    val conditionString: String = ConfigurationUtil("conditions.properties").getString("condition.params.json")
    JSON.parseObject(conditionString, classOf[Condition])
  }

  def main(args: Array[String]): Unit = {

    //先把userAction。。读出去
    System.setProperty("HADOOP_USER_NAME", "atguigu")

    val spark = SparkSession.builder()
      .appName("OfflineApp")
      .master("local[2]")
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setCheckpointDir("hdfs://hadoop103:9000/sparkmall")

    val userVisitActionRDD: RDD[UserVisitAction] = readUserVisitActionRDD(spark, readCondition)
    userVisitActionRDD.cache()
    userVisitActionRDD.checkpoint()

    val taskId = UUID.randomUUID().toString
    /*
        println("开始任务 1")
        val categoryTop10: List[CategoryCountInfo] = CategoryTop10App.statCategoryTop10(spark, userVisitActionRDD, taskId)

        println("开始任务 2")
        CategorySessionApp.statCategoryTop10Session(spark, categoryTop10, userVisitActionRDD, taskId)

        println("开始任务 3")
        PageConversionApp.calcPageConversion(spark, userVisitActionRDD, readCondition.targetPageFlow, taskId)
    */
    // 需求4:
    AreaProductTop3.statAreaProductTop3(spark, taskId)


  }
}
