package com.spark.streaming.scala.lesson

import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.streaming.Durations.seconds
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @date 2021/5/19
 */
object Top3HotProduct {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("Top3HotProduct")
    val ssc = new StreamingContext(conf, seconds(1))

    val clickLogDStream = ssc.socketTextStream("localhost", 9999)
    val pairClickLogDStream = clickLogDStream.map(log => {
      val logSplited = log.split(" ")
      (logSplited(2) + "_" + logSplited(1), 1)
    })

    val clickCountsDStream = pairClickLogDStream.reduceByKeyAndWindow(
      (v1: Int, v2: Int) => v1 + v2,
      Seconds(60),
      Seconds(10)
    )

    clickCountsDStream.foreachRDD(clickCountRDD => {
      val clickCountRowRDD = clickCountRDD.map(clickCount => {
        val category = clickCount._1.split("_")(0)
        val product = clickCount._1.split("_")(1)
        val click_count = clickCount._2
        Row(category, product, click_count)
      })
      val structFields = Array(
        StructField("category", StringType, true),
        StructField("product", StringType, true),
        StructField("click_count", IntegerType, true)
      )
      val structType = new StructType(structFields)
      val sqlContext = new HiveContext(clickCountRDD.context)

      val categoryClickCountDF = sqlContext.createDataFrame(clickCountRowRDD, structType)
      categoryClickCountDF.createOrReplaceTempView("product_click_log")

      val top3ProductDF = sqlContext.sql(
        "SELECT category,product,click_count "
          + "FROM ("
          + "SELECT "
          + "category,"
          + "product,"
          + "click_count,"
          + "row_number() OVER (PARTITION BY category ORDER BY click_count DESC) rank "
          + "FROM product_click_log"
          + ") tmp "
          + "WHERE rank<=3")

      top3ProductDF.show()


    })





    ssc.start()
    ssc.awaitTermination()


  }

}
