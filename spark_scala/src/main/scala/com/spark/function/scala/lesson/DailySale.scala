package com.spark.function.scala.lesson

import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession, functions}

/**
 * @author yangqian
 * @date 2021/5/13
 */
object DailySale {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("DailySale")
    val sc = new SparkContext(conf)
    val sqlContext = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    // 说明一下，业务的特点
    // 实际上呢，我们可以做一个，单独统计网站登录用户的销售额的统计
    // 有些时候，会出现日志的上报的错误和异常，比如日志里丢了用户的信息，那么这种，我们就一律不统计了

    // 模拟数据
    val userSaleLog = Array(
      "2015-10-01,55.05,1122",
      "2015-10-01,23.15,1133",
      "2015-10-01,15.20,",    // 模拟异常数据
      "2015-10-02,56.05,1144",
      "2015-10-02,78.87,1155",
      "2015-10-02,113.02,1123"
    )
    val userSaleLogRDD = sc.parallelize(userSaleLog, 3)

    val filteredUserSaleLogRDD = userSaleLogRDD.filter(log => if (log.split(",").length == 0) true else false)

    val userSaleRowRDD = filteredUserSaleLogRDD.map(line => Row(
      line.split(",")(0),
      line.split(",")(1).toDouble,
      line.split(",")(2).toInt
    ))

    val structType = StructType(Array(
      StructField("date", StringType, true),
      StructField("sale_amount", DoubleType, true),
      StructField("userId", IntegerType, true)
    ))

    val userSaleDS = sqlContext.createDataFrame(userSaleRowRDD, structType)

    import sqlContext.implicits._

    // 统计每日的销售数据
    userSaleDS.groupBy("date")
      .agg('date, functions.sum('sale_amount))
      .rdd
      .map(row => Row(row(1), row(2)))
      .collect()
      .foreach(println)


  }

}
