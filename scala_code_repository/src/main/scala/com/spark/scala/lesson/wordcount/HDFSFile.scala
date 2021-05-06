package com.spark.scala.lesson.wordcount

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yangqian
 * @date 2021/5/6
 */
object HDFSFile {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("HDFSFile").setMaster("local")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("hdfs://master:9000/wordcount/spark.txt", 1)
    val count = lines.map{ line => line.length() }.reduce(_ + _)
    println("file's count is " + count)
  }

}
