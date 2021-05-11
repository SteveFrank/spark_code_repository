package com.spark.sql.scala.lesson

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yangqian
 * @date 2021/5/11
 */
object DataSetCreate {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DataSetCreate")
    val sc = new SparkContext(conf)
    // 注意重新学习SparkSession(使用方法略有不同)
    // SparkSession 可以替代 SqlContext
    val sqlContext = new SQLContext(sc)
    val dataset = sqlContext.read.json("hdfs://master:9000/spark_sql_file/students.json")
    dataset.show()
    dataset.show()
    dataset.printSchema()
    dataset.select("name").show()
    dataset.select(dataset("name"), dataset("age") + 1).show()
    dataset.filter(dataset("age") > 18).show()
    dataset.groupBy("age").count().show()
  }

}
