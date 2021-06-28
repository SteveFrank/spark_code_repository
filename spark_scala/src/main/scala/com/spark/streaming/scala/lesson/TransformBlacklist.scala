package com.spark.streaming.scala.lesson

import org.apache.spark.SparkConf
import org.apache.spark.streaming.Durations.seconds
import org.apache.spark.streaming.StreamingContext

/**
 * @date 2021/5/18
 */
object TransformBlacklist {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TransformBlacklist").setMaster("local[2]")

    val ssc = new StreamingContext(conf, seconds(5))

    val blacklist = Array(("tom", true))
    val blacklistRDD = ssc.sparkContext.parallelize(blacklist, 5)

    val adsLogDStream = ssc.socketTextStream("localhost", 9999)
    // (username, log)
    val userAdsClickLogDStream = adsLogDStream.map(log => {
      (log.split(" ")(1), log)
    })

    // 进行过滤处理
    val validAdsClickDStream = userAdsClickLogDStream.transform(adsLog => {
      // leftOuterJoin 之后的结果
      // Tuple2<String, Tuple2<String,Optional<Boolean>>>
      val joinedLogRDD = adsLog.leftOuterJoin(blacklistRDD)

      val filteredLogRDD = joinedLogRDD.filter(log => {
        if (log._2._2.getOrElse(false)) {
          false
        } else {
          true
        }
      })
      val resultLogRDD = filteredLogRDD.map(log => log._2._1)
      resultLogRDD
    })

    validAdsClickDStream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
