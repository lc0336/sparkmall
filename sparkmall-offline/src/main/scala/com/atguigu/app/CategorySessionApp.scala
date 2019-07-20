package com.atguigu.app

import java.util.Properties

import com.atguigu.sparkmall.common.bean.UserVisitAction
import com.atguigu.util.{CategoryCountInfo, CategorySession}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

object CategorySessionApp {
  def statCategoryTop10Session(spark: SparkSession, categoryTop10: List[CategoryCountInfo],
                               userVisitActionRDD: RDD[UserVisitAction], taskId: String): Unit = {

    //1.过滤掉category 不在前10的日志
    //1.1 得到top10 的到 categoryId
    val categoryIdTop10: List[String] = categoryTop10.map(_.categoryId)


    //判断单个数据
    /*//广播变量
    val categoryIdTop10DB: Broadcast[List[String]] = spark.sparkContext.broadcast(categoryIdTop10)
    //1.2 过滤出来categoryIdTop10 的日志
    val filteredActionRDD: RDD[UserVisitAction] = userVisitActionRDD.filter(info => categoryIdTop10DB.value.contains(info.click_category_id + ""))
    //转换结果
    val categorySessionCountRDD: RDD[((Long, String), Int)] = filteredUserIdList.map(userAction => ((userAction.click_category_id, userAction.session_id), 1))
      .reduceByKey(_ + _)

    // 3. 统计每个品类top10.  => RDD[categoryId, (sessionId, count)] => RDD[categoryId, Iterable[(sessionId, count)]]
    val categorySessionGrouped: RDD[(Long, Iterable[(String, Int)])] = categorySessionCountRDD.map {
      case ((cid, sid), count) => (cid, (sid, count))
    }.groupByKey

    // 4. 对每个 Iterable[(sessionId, count)]进行排序, 并取每个Iterable的前10
    // 5. 把数据封装到 CategorySession 中
    val sortedCategorySession: RDD[CategorySession] = categorySessionGrouped.flatMap {
      case (cid, it) => {
        it.toList.sortBy(_._2)(Ordering.Int.reverse).take(10).map {
          item => CategorySession(taskId, cid.toString, item._1, item._2)
        }
      }
    }

    // 6. 写入到 mysql 数据库
    val categorySessionArr = sortedCategorySession.collect.map(item => Array(item.taskId, item.categoryId, item.sessionId, item.clickCount))
    JDBCUtil.executeUpdate("truncate category_top10_session_count", null)
    JDBCUtil.executeBatchUpdate("insert into category_top10_session_count values(?, ?, ?, ?)", categorySessionArr)

    */

    //多个判断
    val filteredUserIdList: RDD[UserVisitAction] = userVisitActionRDD.filter(uva => {
      if (uva.click_category_id != -1) {
        categoryIdTop10.contains(uva.click_category_id) //判断点击action是否在top10中
      } else if (uva.order_category_ids != null) {
        val cids = uva.order_category_ids.split(",")
        categoryIdTop10.intersect(cids).nonEmpty //判断点击事件
      } else if (uva.pay_category_ids != null) {
        val cids = uva.pay_category_ids.split(",")
        categoryIdTop10.intersect(cids).nonEmpty //判断支付行为是都在top10中
      } else {
        false
      }
    })


    //统计每个品类的top10 session
    // 3.1 => RDD[(categoryId, sessionId)] .map
    //=> RDD[(categoryId, sessionId), 1)]
    val categroySession: RDD[((String, String), Int)] = filteredUserIdList.flatMap(uva => {
      if (uva.click_category_id != -1) {
        Array(((uva.click_category_id + "", uva.session_id), 1))
      } else if (uva.order_category_ids != null) {
        val cids: Array[String] = uva.order_category_ids.split(",")
        categoryIdTop10.intersect(cids).map(cid => {
          ((cid, uva.session_id), 1)
        })
      } else {
        val cids: Array[String] = uva.pay_category_ids.split(",")
        categoryIdTop10.intersect(cids).map(cid => {
          ((cid, uva.session_id), 1)
        })
      }
    })


    //3.2 聚合 => RDD[(categoryId, sessionId), count)]  => RDD[(categoryId, (sessionId, count))]
    val categorySessionCount = categroySession.reduceByKey(_ + _).map {
      case ((cid, sid), count) => (cid, (sid, count))
    }

    //3.3 分组降序 取前10
    val categorySessionContTop10 = categorySessionCount.groupByKey().map {
      case (cid, sessionIt) => {
        (cid, sessionIt.toList.sortBy(_._2)(Ordering.Int.reverse).take(10))
      }
    }

    //
    import spark.implicits._
    val resultRDD = categorySessionContTop10.flatMap {
      case (cid, sessionIt) => {
        sessionIt.map {
          case (sid, count) => CategorySession(taskId, cid, sid, count)
        }
      }
    }


    //利用sparkjdbc的方式写入到mysql中  写入到mysql数据库中
    val properties = new Properties()
    properties.setProperty("user", "root")
    properties.setProperty("password", "123456")
    resultRDD.toDF.write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://hadoop103:3306/sparkmall", "category_top10_session_count", properties)
  }

}
