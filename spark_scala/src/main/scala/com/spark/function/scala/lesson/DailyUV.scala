package com.spark.function.scala.lesson

import org.apache.spark.sql.functions.countDistinct
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}

/**
 * @date 2021/5/13
 */
object DailyUV {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("DailyUV")
    val sc = new SparkContext(conf)
    val sqlContext = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    import sqlContext.implicits._

    // 模拟用户的访问日志，日志使用逗号分隔，第一列是日期，第二列是用户id
    val userAccessLog = Array(
      "2021-10-01,1122",
      "2021-10-01,1122",
      "2021-10-01,1123",
      "2021-10-01,1124",
      "2021-10-01,1124",
      "2021-10-02,1122",
      "2021-10-02,1121",
      "2021-10-02,1123",
      "2021-10-02,1123"
    )
    val userAccessLogRDD = sc.parallelize(userAccessLog, 3)

    // 将模拟出来的用户访问日志RDD转换为DataSet
    // 将普通的RDD转换为元素为Row的RDD
    val userAccessLogRowRDD = userAccessLogRDD.map(line => Row(line.split(",")(0), line.split(",")(1).toInt))
    // 构造DataSet元数据
    val structType = StructType(Array(
      StructField("date", StringType, true),
      StructField("userId", IntegerType, true)
    ))
    // 构造DataSet
    val userAccessLogRowDS = sqlContext.createDataFrame(userAccessLogRowRDD, structType)

    userAccessLogRowDS.groupBy("date")
      .agg('date, countDistinct('userId))
      .rdd
      .map(row => Row(row(1), row(2)))
      .collect()
      .foreach(println)

  }

}
