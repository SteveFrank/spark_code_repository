package com.spark.scala.lesson.transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yangqian
 * @date 2021/5/8
 */
object TransformationOperation {

  def main(args: Array[String]): Unit = {
//    map()
//    filter()
//    flatMap()
//    groupByKey()
//    reduceByKey()
//    sortByKey()
    join()
  }

  def join() {
    val conf = new SparkConf()
      .setAppName("join")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val studentList = Array(
      Tuple2(1, "leo"),
      Tuple2(2, "jack"),
      Tuple2(3, "tom"));

    val scoreList = Array(
      Tuple2(1, 100),
      Tuple2(2, 90),
      Tuple2(3, 60));

    val students = sc.parallelize(studentList);
    val scores = sc.parallelize(scoreList);

    val studentScores = students.join(scores)

    studentScores.foreach(studentScore => {
      println("student id: " + studentScore._1);
      println("student name: " + studentScore._2._1)
      println("student socre: " + studentScore._2._2)
      println("=======================================")
    })
  }

  def sortByKey() {
    val conf = new SparkConf()
      .setAppName("sortByKey")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val scoreList = Array(Tuple2(65, "leo"), Tuple2(50, "tom"),
      Tuple2(100, "marry"), Tuple2(85, "jack"))
    val scores = sc.parallelize(scoreList, 1)
    val sortedScores = scores.sortByKey(false)

    sortedScores.foreach(studentScore => println(studentScore._1 + ": " + studentScore._2))
  }

  def reduceByKey() {
    val conf = new SparkConf()
      .setAppName("groupByKey")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val scoreList = Array(Tuple2("class1", 80), Tuple2("class2", 75),
      Tuple2("class1", 90), Tuple2("class2", 60))
    val scores = sc.parallelize(scoreList, 1)
    val totalScores = scores.reduceByKey(_ + _)

    totalScores.foreach(classScore => println(classScore._1 + ": " + classScore._2))
  }

  def groupByKey() {
    val conf = new SparkConf()
      .setAppName("groupByKey")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val scoreList = Array(
      Tuple2("class1", 80), Tuple2("class2", 75),
      Tuple2("class1", 90), Tuple2("class2", 60)
    )
    val scores = sc.parallelize(scoreList, 1)
    val groupedScores = scores.groupByKey()

    groupedScores.foreach(score => {
      println(score._1);
      score._2.foreach { singleScore => println(singleScore) };
      println("=============================")
    })
  }

  def flatMap(): Unit = {
    val conf = new SparkConf().setAppName("flatMap").setMaster("local")
    val sc = new SparkContext(conf)
    val lines = Array("hello world", "hello scala", "hello Java")
    val linesRDD = sc.parallelize(lines)
    val words = linesRDD.flatMap(line => line.split(" "))
    words.foreach(word => println(word))
  }

  def filter(): Unit = {
    val conf = new SparkConf().setAppName("filter").setMaster("local")
    val sc = new SparkContext(conf)
    val numbers = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val numbersRDD = sc.parallelize(numbers)
    val multipleNumberRDD = numbersRDD.filter(num => num % 2 == 0)
    multipleNumberRDD.foreach(num => println(num))
  }

  def map(): Unit = {
    val conf = new SparkConf().setAppName("map").setMaster("local")
    val sc = new SparkContext(conf)
    val numbers = Array(1, 2, 3, 4, 5)
    val numbersRDD = sc.parallelize(numbers)
    val multipleNumberRDD = numbersRDD.map(num => num * 2)
    multipleNumberRDD.foreach(number => println(number))
  }

}
