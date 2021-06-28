package com.spark.sql.scala.lesson.dataset

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession, types}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @date 2021/5/12
 */
object RDD2DataFrameProgrammatically {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDD2DataFrameProgrammatically").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = SparkSession.builder().config(conf).getOrCreate()

    // 1、转换为普通的RDD
    val studentRDD = sc
      .textFile("/Users/qian/WorkSpaces/own-workspace/2021/spark_code_repository/spark_java/src/main/resources/spark_sql/students.txt")
      .map(line => {
        val lineSplit = line.split(",")
        Row(lineSplit(0).toInt, lineSplit(1).toString, lineSplit(2).toInt)
      })

    // 2、通过编程的方式动态的构造元数据
    val structType = StructType(Array(
      StructField("id", IntegerType, true),
      StructField("name", StringType, true),
      StructField("age", IntegerType, true)
    ))

    // 3、进行RDD与DataFrame之间的转换
    val studentDF = sqlContext.createDataFrame(studentRDD, structType)

    studentDF.createOrReplaceTempView("students")

    val teenagerDF = sqlContext.sql("select id, name, age from students where age <= 18")
    val teenagerRDD = teenagerDF.rdd.collect().foreach( row => println(row) )

  }

}
