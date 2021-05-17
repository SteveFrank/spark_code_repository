package com.spark.streaming.scala.lesson

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author yangqian
 * @date 2021/5/17
 */
object UpdateStateByKeyWordCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("UpdateStateByKeyWordCount")
    val ssc = new StreamingContext(conf, Seconds(5))
//    ssc.checkpoint("hdfs://master:9000/check_point/ch/wordcount_checkpoint")
    ssc.checkpoint("/Users/qian/WorkSpaces/own-workspace/2021/spark_code_repository/check_point/ch/wordcount_checkpoint")

    val lines = ssc.socketTextStream("localhost", 9999)
    val words = lines.flatMap { _.split(" ") }
    val pairs = words.map { word => (word, 1) }
    val wordCounts = pairs.updateStateByKey((values: Seq[Int], state: Option[Int]) => {
      var newValue = state.getOrElse(0)
      for (value <- values) {
        newValue += value
      }
      Option(newValue)
    })
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()

  }

}
