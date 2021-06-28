package com.spark.udaf.scala.lesson

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @date 2021/5/13
 */
object UDAF {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("UDAF")
    val sc = new SparkContext(conf)
    val sqlContext = SparkSession.builder().config(conf).getOrCreate()

    // 构造模拟数据
    val names = Array("leo", "Marry", "Jack", "Tom", "Tom", "Tom", "Leo")
    val namesRDD = sc.parallelize(names, 3)
    val namesRowRDD = namesRDD.map( name => Row(name) )
    val structType = StructType(Array(StructField("name", StringType, true)))
    val nameDS = sqlContext.createDataFrame(namesRowRDD, structType)

    // 注册表
    nameDS.createOrReplaceTempView("names")

    // 注册自定义函数
    sqlContext.udf.register("strCount", new StringCount)

    // 使用自定义函数
    sqlContext.sql("select name, strCount(name) from names group by name")
      .collect()
      .foreach(println)

  }

}
