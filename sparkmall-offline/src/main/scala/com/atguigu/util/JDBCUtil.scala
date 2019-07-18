package com.atguigu.util

import java.util.Properties

import com.alibaba.druid.pool.DruidDataSourceFactory
import com.atguigu.sparkmall.common.util.ConfigurationUtil

object JDBCUtil {



  val dataSource = initConnection()

  def initConnection() = {
    val properties = new Properties()
    val conf = ConfigurationUtil("config.properties")
    properties.setProperty("driverClassName", "com.mysql.jdbc.Driver")
    properties.setProperty("url", conf.getString("jdbc.url"))
    properties.setProperty("username", conf.getString("jdbc.user"))
    properties.setProperty("password", conf.getString("jdbc.password"))
    properties.setProperty("maxActive", conf.getString("jdbc.maxActive"))
    DruidDataSourceFactory.createDataSource(properties)

  }

  //执行单条语句
  def executeBatchUpdate(sql: String, args: Array[Any]): Unit = {
    val con = dataSource.getConnection()
    con.setAutoCommit(false)//是否执行一条语句就自动提交一次
    val ps = con.prepareStatement(sql)
    if (args!=null && args.length>0){
      (0 until args.length).foreach{
        x=>ps.setObject(x+1,args(x))//?????
      }
    }
    ps.executeUpdate()
    con.commit()
  }


  //执行批处理
  def  executeBatchUpdate(sql: String, argsList: Iterable[Array[Any]]): Unit ={
    val con = dataSource.getConnection()
    con.setAutoCommit(false)//是否执行一条语句就自动提交一次
    val ps = con.prepareStatement(sql)
    argsList.foreach{
      case args:Array[Any]=>{
        (0 until args.length).foreach{
          i=>ps.setObject(i+1,args(i))
        }
        ps.addBatch()
      }
    }
    ps.executeBatch()
    con.commit()
  }


  //提交单条数据
  def executeUpdate(sql: String, value: Null) = {
    val con = dataSource.getConnection()
    con.setAutoCommit(false)//是否执行一条语句就自动提交一次
    val ps = con.prepareStatement(sql)
    ps.executeUpdate()
    con.commit()
  }

}
