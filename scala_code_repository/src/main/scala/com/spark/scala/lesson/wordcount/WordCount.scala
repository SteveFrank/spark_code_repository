package com.spark.scala.lesson.wordcount

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yangqian
 * @date 2021/4/29
 */
object WordCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCountScala")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("hdfs://master:9000/wordcount/spark.txt")
    val words = lines.flatMap( line => line.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)

    wordCounts.foreach(wordCount => println(wordCount._1 + " appeared " + wordCount._2 + " times."))
  }

}
