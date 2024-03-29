package com.spark.scala.lesson.variable

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @date 2021/5/9
 */
object BroadcastVariable {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("BroadcastVariable").setMaster("local")
    val sc = new SparkContext(conf)

    val factor = 3;
    val factorBroadcast = sc.broadcast(factor)

    val numberArray = Array(1, 2, 3, 4, 5)
    val numbers = sc.parallelize(numberArray, 1)
    val multipleNumbers = numbers.map(num => num * factorBroadcast.value)
    multipleNumbers.foreach(num => println(num))
  }
}
