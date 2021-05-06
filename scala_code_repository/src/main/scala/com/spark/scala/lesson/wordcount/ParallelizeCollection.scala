package com.spark.scala.lesson.wordcount

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yangqian
 * @date 2021/5/6
 */
object ParallelizeCollection {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("ParallelizeCollection")
      .setMaster("local");
    val sc = new SparkContext(conf)

    val numbers = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13)
    // 创建5个Partition
    val numberRDD = sc.parallelize(numbers, 5)
    println(numberRDD.partitions.length)
    val sum = numberRDD.reduce(_ + _)
    println(sum)

  }

}
