package com.atguigu.app

import java.util.Properties

import com.atguigu.acc.AreaClickUDAF
import com.atguigu.sparkmall.common.util.ConfigurationUtil
import org.apache.spark.sql.{SaveMode, SparkSession}

object AreaProductTop3 {
  def statAreaProductTop3(spark: SparkSession, taskId: String) = {

    spark.sql("use sparkmall")
    spark.udf.register("remark", new AreaClickUDAF)
    // 1. 用行为表和城市表做一个连, 得到地区和城市信息  t1
    spark.sql(
      """
        |select
        |	c.*,
        |	p.product_name,
        |	v.click_product_id
        |from user_visit_action v join city_info c join product_info p on v.city_id=c.city_id and v.click_product_id=p.product_id
        |where click_product_id>-1
      """.stripMargin).createOrReplaceTempView("t1")

    // 2. 按照地区分组, 然后统计每个产品的点击的数量   t2
    spark.sql(
      """
        |select
        |	area,
        |	product_name,
        |	count(*) click_count,
        | remark(city_name) remark
        |from t1
        |group by t1.area, t1.product_name
      """.stripMargin).createOrReplaceTempView("t2")

    // 3. 按照点击数降序 t3
    spark.sql(
      """
        |select
        |	*,
        |	rank() over(partition by t2.area order by click_count desc) rank
        |from t2
      """.stripMargin).createOrReplaceTempView("t3")

    val conf = ConfigurationUtil("config.properties")
    val props = new Properties()
    props.setProperty("user",conf.getString("jdbc.user"))
    //props.setProperty("user", "root")
    props.setProperty("password", "123456")

    // 4. 取前3
    spark.sql(
      """
        |select
        |	area,
        | product_name,
        | click_count,
        | remark
        |from t3
        |where rank <= 3
      """.stripMargin) .write.mode(SaveMode.Overwrite).jdbc(conf.getString("jdbc.url"),"area_click_top10",props)

  }
}
