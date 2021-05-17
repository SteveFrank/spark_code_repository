package com.spark.streaming.scala.lesson

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 *
 * 基于HDFS的实时streaming流
 *
 * @author yangqian
 * @date 2021/5/17
 */
object StreamingHDFSWordCount {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("HDFSWordCount")
    val ssc = new StreamingContext(conf, Seconds(5))

    val lines = ssc.textFileStream("hdfs://master:9000/spark_study/wordcount_dir")
    val words = lines.flatMap(line => line.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)

    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()

  }
}
