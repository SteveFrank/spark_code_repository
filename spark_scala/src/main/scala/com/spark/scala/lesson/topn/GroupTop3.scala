package com.spark.scala.lesson.topn

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @date 2021/5/10
 */
object GroupTop3 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("groupTop3").setMaster("local")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("/Users/qian/WorkSpaces/own-workspace/2021/spark_code_repository/spark_java/src/main/resources/score.txt")
    val groupClassScoreRdd = lines.map(line => (line.split(" ")(0), line.split(" ")(1)))
    val groupClassScoreListRdd = groupClassScoreRdd.groupByKey()
    val sortClassScoreListRdd = groupClassScoreListRdd.map(classScore => {
      val className = classScore._1
      val scores = classScore._2.iterator
      val top3 = new Array[Int](3)
      var flag = true
      while (scores.hasNext) {
        val score = scores.next
        for (i <- 0 until 3 if flag) {
          if (top3(i) == 0) {
            top3(i) = score.toInt
          } else if (score.toInt > top3(i)) {
            val tmp = top3(i)
            top3(i) = score.toInt
            if (i < top3.length - 1) {
              top3(i + 1) = tmp
              flag = false
            }
          }
        }
      }
      (className, top3)
    })
    sortClassScoreListRdd.foreach(r => println(s"className:${r._1}, score Top3:${r._2.mkString("Array(", ", ", ")")}"))
  }

}
