package com.spark.sql.scala.lesson.dataset

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @date 2021/5/12
 */
object ManuallySpecifyOptions {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ManuallySpecifyOptions").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = SparkSession.builder().config(conf).getOrCreate()

    val peopleDS = sqlContext.read.format("json")
      .load("/Users/qian/WorkSpaces/own-workspace/2021/spark_code_repository/spark_java/src/main/resources/spark_sql/people.json")
    peopleDS.select("name")
      .write.format("parquet")
      .save("/Users/qian/WorkSpaces/own-workspace/2021/spark_code_repository/spark_java/src/main/resources/spark_sql/peopleName_scala.parquet")
  }

}
