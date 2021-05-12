package com.spark.parquet.scala.lesson

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yangqian
 * @date 2021/5/12
 */
object ParquetMergeSchema {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ParquetMergeSchema").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = SparkSession.builder().config(conf).getOrCreate()

    import sqlContext.implicits._

    // 1、创建DataSet作为学生的基本信息，写入parquet文件中
    val studentsWithNameAge = Array(("leo", 23), ("jack", 25)).toSeq
    val studentsWithNameAgeDS = sc.parallelize(studentsWithNameAge, 2).toDF("name", "age")
    studentsWithNameAgeDS
      .write
      .format("parquet")
      .mode(SaveMode.Append)
      .save("/Users/qian/WorkSpaces/own-workspace/2021/spark_code_repository/spark_java/src/main/resources/parquet/students")

    // 2、创建第二个DataSet，作为学生的成绩信息，写入parquet文件中
    val studentsWithNameGrade = Array(("marry", "A"), ("tom", "B")).toSeq
    val studentsWithNameGradeDS = sc.parallelize(studentsWithNameGrade, 2).toDF("name", "grade")
    studentsWithNameGradeDS
      .write
      .format("parquet")
      .mode(SaveMode.Append)
      .save("/Users/qian/WorkSpaces/own-workspace/2021/spark_code_repository/spark_java/src/main/resources/parquet/students")

    // 3、读取的时候merge成为三列
    val students = sqlContext
      .read.option("mergeSchema", "true")
      .parquet("/Users/qian/WorkSpaces/own-workspace/2021/spark_code_repository/spark_java/src/main/resources/parquet/students")

    students.printSchema()
    students.show()


  }

}
