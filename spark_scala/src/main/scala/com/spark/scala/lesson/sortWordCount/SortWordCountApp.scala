package com.spark.scala.lesson.sortWordCount

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @date 2021/5/9
 */
object SortWordCountApp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Accumulator").setMaster("local")
    val sc = new SparkContext(conf)
    // 创建lines RDD
    val lines = sc.textFile("/Users/qian/WorkSpaces/own-workspace/2021/spark_code_repository/spark_java/src/main/resources/spark.txt")
    val words = lines.flatMap(line => line.split(" "))
    val pairs = words.map(word => (word, 1))
    // 统计
    val wordCount = pairs.reduceByKey(_ + _)
    // 翻转
    val wordCountRevert = wordCount.map(t1 => (t1._2, t1._1))
    // 排序
    val wordCountSort = wordCountRevert.sortByKey(false)
    // 翻转
    val wordCountResult = wordCountSort.map(t2 => (t2._2, t2._1))
    // 打印结果
    wordCountResult.foreach(t => println(t._1 + ", times:" + t._2))
  }
}
