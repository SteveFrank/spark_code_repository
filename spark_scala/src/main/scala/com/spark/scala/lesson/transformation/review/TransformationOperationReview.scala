package com.spark.scala.lesson.transformation.review

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yangqian
 * @date 2021/6/6
 */
object TransformationOperationReview {

  def main(args: Array[String]): Unit = {
//    map()
//    filter()
//    filterMap()
//    groupByKey()
    join()
  }

  def join(): Unit = {
    val conf = new SparkConf()
      .setAppName("join")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val studentList = Array(
      Tuple2(1, "leo"),
      Tuple2(2, "jack"),
      Tuple2(3, "tom"))

    val scoreList = Array(
      Tuple2(1, 100),
      Tuple2(2, 90),
      Tuple2(3, 60))

    val studentRdd = sc.parallelize(studentList)
    val scoreRdd = sc.parallelize(scoreList)

    val studentScoreRdd = studentRdd.join(scoreRdd)

    studentScoreRdd.foreach(ssr => {
      println(s"id:${ssr._1}, name:${ssr._2._1}, score:${ssr._2._2}")
    })

  }

  def groupByKey(): Unit = {
    val conf = new SparkConf()
      .setAppName("GroupByKeyTransformationOperationReview")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val classScore = Array(
      Tuple2("class1", 80),Tuple2("class1", 81),Tuple2("class1", 82),
      Tuple2("class2", 76),Tuple2("class2", 79),Tuple2("class2", 85),
      Tuple2("class3", 90)
    )
    val rdd = sc.parallelize(classScore)
    val classScoreRdd = rdd.groupByKey()
    classScoreRdd.foreach(score => {
      println(score._1)
      score._2.foreach(s => {
        println(s"===> $s ")
      })
    })
  }

  def filterMap(): Unit = {
    val conf = new SparkConf()
      .setAppName("FilterTransformationOperation")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val arr = Array("spark text", "Hadoop spark")
    val rdd = sc.parallelize(arr, 4)
    val words = rdd.flatMap(line => line.split(" "))
    words.foreach(word => println(s"$word"))
  }

  def filter(): Unit = {
    val conf = new SparkConf()
      .setAppName("FilterTransformationOperation")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val arr = Array(1,2,3,4,5,6,7,8,9,0)
    val rdd = sc.parallelize(arr, 4)
    val rddFilter = rdd.filter(num => num % 2 == 0)
    rddFilter.foreach(num => println(s"$num"))
  }

  def map(): Unit = {
    val conf = new SparkConf()
      .setAppName("MapTransformationOperation")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val arr = Array(1,2,3,4,5,6,7,8,9,0)
    val rdd = sc.parallelize(arr, 4)
    val sum = rdd.reduce(_ + _)
    println(s"sum = $sum")
  }

}
