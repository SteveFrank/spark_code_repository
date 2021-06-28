package com.spark.sql.scala.lesson.dataset

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @date 2021/5/12
 */
object GenericLoadSave {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("GenericLoadSave").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = SparkSession.builder().config(conf).getOrCreate()

    val userDS = sqlContext.read.load("/Users/qian/WorkSpaces/own-workspace/2021/spark_code_repository/spark_java/src/main/resources/spark_sql/users.parquet")
    userDS.show()
  }

}
