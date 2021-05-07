package com.spark.scala.lesson.wordcount

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yangqian
 * @date 2021/5/7
 */
object LineCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LineCount").setMaster("local")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("/Users/qian/WorkSpaces/own-workspace/2021/spark_code_repository/wordcount/src/main/resources/spark.txt")
    val pairs = lines.map{ line => (line, 1) }
    val lineCounts = pairs.reduceByKey(_ + _)

    lineCounts.foreach(lineCount => println(lineCount._1 + " appears " + lineCount._2 + " times. "))
  }

}
