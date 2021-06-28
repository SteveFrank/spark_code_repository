package com.spark.sql.scala.lesson.dataset

import com.spark.sql.scala.lesson.dataset.entity.Student
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * 要注意区别DataSet 和 DataFrame的区别
 *
 * 利用反射（隐式转换）的方式转为为DataSet
 *
 * @date 2021/5/12
 */
object RDD2DataSetReflection {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDD2DataSetReflection").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = SparkSession.builder().config(conf).getOrCreate()

    val studentRDD = sc
      .textFile("/Users/qian/WorkSpaces/own-workspace/2021/spark_code_repository/spark_java/src/main/resources/spark_sql/students.txt")
      .map(line => line.split(","))
      .map(arr => Student(arr(0).toInt, arr(1).toString, arr(2).toInt))

    import sqlContext.implicits._

    val studentDF = sqlContext.createDataset(studentRDD)
    studentDF.printSchema()

    // 与 DataFrame的registerTempTable类似
    studentDF.createOrReplaceTempView("students")

    val teenagerDF = sqlContext.sql("select id, name, age  from students where age <= 18")
    val teenagerRDD = teenagerDF.rdd

    teenagerRDD
      .map(row => Student(row(0).toString.toInt, row(1).toString, row(2).toString.toInt))
      .collect()
      .foreach { stu => println(stu.id + ":" + stu.name + ":" + stu.age) }

    println("=========================================================")

    teenagerRDD.map { row => Student(row.getAs[Int]("id"), row.getAs[String]("name"), row.getAs[Int]("age")) }
      .collect()
      .foreach { stu => println(stu.id + ":" + stu.name + ":" + stu.age) }

    println("=========================================================")

    val studentResultRDD = teenagerRDD.map(row => {
      val map = row.getValuesMap[Any](Array("id", "name", "age"))
      Student(map("id").toString.toInt, map("name").toString, map("age").toString.toInt)
    })

    studentResultRDD.collect().foreach { stu => println(stu.id + ":" + stu.name + ":" + stu.age) }

  }
}
