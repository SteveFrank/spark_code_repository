package com.spark.datasource.scala.lesson

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @date 2021/5/12
 */
object JSONDataSource {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("JSONDataSource").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = SparkSession.builder().config(conf).getOrCreate()

    // 创建学生成绩的DataSet
    val studentDS = sqlContext.read.format("json").load("/Users/qian/WorkSpaces/own-workspace/2021/spark_code_repository/spark_java/src/main/resources/json/students.json")
    // 创建临时表
    studentDS.createOrReplaceTempView("students")
    // 获取优秀学生的信息
    val studentScoreDS = sqlContext.sql("select name, score from students")
    studentScoreDS.printSchema()
    studentScoreDS.show()

    // 获取RDD数据
    val studentScoreRDD = studentScoreDS.rdd
    val goodStudentNames = studentScoreRDD.map(studentScore => studentScore(0)).collect()

    println(goodStudentNames)

    // 创建学生基本信息的数据
    // 创建学生基本信息DataFrame
    val studentInfoJSONs = Array(
      "{\"name\":\"Leo\", \"age\":18}",
      "{\"name\":\"Marry\", \"age\":17}",
      "{\"name\":\"Jack\", \"age\":19}")
    val studentInfoRDD = sc.parallelize(studentInfoJSONs, 3)
    val studentInfoDS = sqlContext.read.json(studentInfoRDD)

    // 查询现在分数大于80分学生的基本信息
    studentInfoDS.createOrReplaceTempView("student_infos")
    var sql = "select name,age from student_infos where name in ("
    for(i <- 0 until goodStudentNames.length) {
      sql += "'" + goodStudentNames(i) + "'"
      if(i < goodStudentNames.length - 1) {
        sql += ","
      }
    }
    sql += ")"

    // sql查询基本信息
    val goodStudentInfoRDD = sqlContext.sql(sql).rdd

    // join数据
    val joinRDD = studentScoreRDD
      .map(row => (row.getAs[String]("name"), row.getAs[Long]("score")))
      .join(goodStudentInfoRDD.map(row => (row.getAs[String]("name"), row.getAs[Long]("age"))))

    val joinRowsRDD = joinRDD.map(info => Row(info._1, info._2._1.toInt, info._2._2.toInt))

    // 类型创建
    val structType = StructType(Array(
      StructField("name", StringType, true),
      StructField("score", IntegerType, true),
      StructField("age", IntegerType, true)))

    val goodStudentsDS = sqlContext.createDataFrame(joinRowsRDD, structType)

    goodStudentsDS.show()

  }

}
