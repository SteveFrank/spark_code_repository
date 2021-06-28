package com.spark.parquet.scala.lesson

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @date 2021/5/12
 */
object ParquetLoadData {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ParquetLoadData").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = SparkSession.builder().config(conf).getOrCreate();

    val userDS = sqlContext
      .read
      .parquet("/Users/qian/WorkSpaces/own-workspace/2021/spark_code_repository/spark_java/src/main/resources/parquet/users.parquet")

    userDS.createOrReplaceTempView("users")

    val userNamesDS = sqlContext.sql("select name from users")
    userNamesDS.printSchema()
    val userNamesRDD = userNamesDS.rdd
    userNamesRDD.collect().foreach(name => println(name))
  }

}
