package com.spark.project.app.scala.lesson

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @author frankq
 * @date 2021/6/30
 */
object AppLogSparkScala {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("AppLogSpark")
      .setMaster("local")
    val sc = SparkSession.builder().config(conf).getOrCreate().sparkContext

    // 读取文件数据
    val accessLogLinesRDD = sc.textFile("/Users/qian/WorkSpaces/own-workspace/2021/spark_code_repository/spark_java/src/main/resources/log_files/access.log")
    val pairAccessLogInfoRDD = accessLogLinesRDD.map(line => {
      val log = line.split("\t")
      val timeStamp = log(0).toLong
      val deviceId = log(1).toString
      val upTraffic = log(2).toLong
      val downTraffic = log(3).toLong
      val accessLog = new AccessLogInfo(time = timeStamp, up = upTraffic, down = downTraffic)
      (deviceId, accessLog)
    })

    // 根据deviceID执行reduceByKey的聚合操作
    // 将相同deviceID的数据聚合起来
    val aggAccessLogInfoRDD = pairAccessLogInfoRDD.reduceByKey(
      (accessLog1, accessLog2) => {
        val timestamp = Math.min(accessLog1.timestamp, accessLog2.timestamp)
        val upTraffic = accessLog1.upTraffic + accessLog2.upTraffic
        val downTraffic = accessLog1.downTraffic + accessLog2.downTraffic
        new AccessLogInfo(time = timestamp, up = upTraffic, down = downTraffic)
      }
    )

    // 将Key使用SortKey替代
    val mapRDDKeyToSortKeyRDD = aggAccessLogInfoRDD.map(
      pair => {
        val sortKey = new AccessLogSortKey(time = pair._2.timestamp, up = pair._2.upTraffic, down = pair._2.downTraffic)
        (sortKey, pair._1)
      }
    )

    // 按照倒序排列
    val sortedAccessLogRDD = mapRDDKeyToSortKeyRDD.sortByKey(false)

    // 取top10的数据
    val top10RDD = sortedAccessLogRDD.take(10)

    top10RDD.foreach(log => {
      println(s"device_id:${log._2}, up_traffic:${log._1.upTraffic}, down_traffic:${log._1.downTraffic}")
    })

  }

}
