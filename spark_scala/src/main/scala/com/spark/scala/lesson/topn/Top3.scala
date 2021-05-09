package com.spark.scala.lesson.topn

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yangqian
 * @date 2021/5/9
 */
object Top3 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Top3").setMaster("local")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("/Users/qian/WorkSpaces/own-workspace/2021/spark_code_repository/spark_java/src/main/resources/top.txt")
    val pairs = lines.map(line => (line.toInt, line.toInt))
    val sortedParis = pairs.sortByKey(false)
    val sortedNums = sortedParis.map(pair => pair._1)
    val result = sortedNums.take(3)
    for (r <- result) {
      println(r)
    }
  }

}
