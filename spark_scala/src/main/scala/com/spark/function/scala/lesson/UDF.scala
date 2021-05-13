package com.spark.function.scala.lesson

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
 * @author yangqian
 * @date 2021/5/13
 */
object UDF {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("UDF")
    val ssc = SparkSession.builder().config(conf)
    val sqlContext = ssc.getOrCreate()
    val sc = ssc.getOrCreate().sparkContext

    // 构造模拟数据
    val names = Array("Leo", "Marry", "Jack", "Tom")
    val namesRDD = sc.parallelize(names, 3)
    val namesRowRDD = namesRDD.map { name => Row(name) }
    val structType = StructType(Array(StructField("name", StringType, true)))
    val namesDF = sqlContext.createDataFrame(namesRowRDD, structType)

    namesDF.createOrReplaceTempView("names")

    // 自定义函数
    sqlContext.udf.register("strLen", (str: String) => str.length)

    // 使用自定义函数
    sqlContext.sql("select name,strLen(name) from names")
      .collect()
      .foreach(println)

  }

}
