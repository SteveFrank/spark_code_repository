package com.spark.scala.lesson.secondarySort

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @date 2021/5/9
 */
object SecondarySort {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Accumulator").setMaster("local")
    val sc = new SparkContext(conf)
    // 创建lines RDD
    val lines = sc.textFile("/Users/qian/WorkSpaces/own-workspace/2021/spark_code_repository/spark_java/src/main/resources/sort.txt")
    val sortKeyMapRDD = lines.map(line => {
      val nums = line.split(" ")
      val secondarySortKey = new SecondarySortKey(nums(0).toInt, nums(1).toInt)
      (secondarySortKey, line)
    })
    val sortResult = sortKeyMapRDD.sortByKey(false)
    val sortLines = sortResult.map(r => r._2)
    sortLines.foreach(line => println(line))

  }

}
