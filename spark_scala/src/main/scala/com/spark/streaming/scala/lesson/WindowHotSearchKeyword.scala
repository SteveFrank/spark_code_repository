package com.spark.streaming.scala.lesson

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Durations, Seconds, StreamingContext}

/**
 * @date 2021/5/18
 */
object WindowHotSearchKeyword {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("WindowHotSearchKeyword")
    val ssc = new StreamingContext(conf, Durations.seconds(1))

    val searchKeywordLogDStream = ssc.socketTextStream("localhost", 9999)
    val keywordDStream = searchKeywordLogDStream.map(log => log.split(" ")(1))

    val keywordPairDStream = keywordDStream.map(keyword => (keyword, 1))
    val keywordCountDStream = keywordPairDStream.reduceByKeyAndWindow(
      (v1: Int, v2: Int) => v1 + v2,
      Seconds(60), Seconds(10))
    val finalKeywordRDD = keywordCountDStream.transform(keywordCountsRDD => {
      // 1、反转（count, keyword）
      val countKeywordRDD = keywordCountsRDD.map(tuple => (tuple._2, tuple._1))
      // 2、sorted(降序)
      val sortedKeywordRDD = countKeywordRDD.sortByKey(false)
      // 3、反转（keyword, count）
      val keywordCountSortedRDD = sortedKeywordRDD.map(tuple => (tuple._2, tuple._1))
      // 4、取前3
      val hotTop3Keyword = keywordCountSortedRDD.take(3)
      // 5、处理结果打印
      println("=================================")
      for (tuple <- hotTop3Keyword) {
        println(s"hot top3 keyword:${tuple._1}")
      }
      println("=================================")

      keywordCountsRDD

    })

    finalKeywordRDD.print()

    ssc.start()
    ssc.awaitTermination()

  }
}
