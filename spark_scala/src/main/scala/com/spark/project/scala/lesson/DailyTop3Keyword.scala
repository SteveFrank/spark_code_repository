package com.spark.project.scala.lesson

import org.apache.spark.sql.SparkSession

/**
 *
 * 数据文件 /resources/hive_data/keyword.txt
 *
 * 数据格式：
 * 日期 用户 搜索词 城市 平台 版本
 *
 * 需求：
 * 1、筛选出符合查询条件（城市、平台、版本）的数据
 * 2、统计出每天搜索uv排名前3的搜索词
 * 3、按照每天的top3搜索词的uv搜索总次数，倒序排序
 * 4、将数据保存到hive表中
 *
 * @date 2021/5/14
 */
object DailyTop3Keyword {

  def main(args: Array[String]): Unit = {
    val sqlContext = SparkSession.builder()
      .appName("DailyTop3Keyword")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .config("spark.sql.codegen", "false")
      .config("spark.sql.inMemoryColumnarStorage.compressed", "false")
      .config("spark.sql.inMemoryColumnarStorage.batchSize", "100")
      .enableHiveSupport()
      .getOrCreate()
    val sc = sqlContext.sparkContext

    // 伪造一份数据，数据的来源可能是MySQL等其他数据源


    // 读取文件为Data文件到RDD中
    val keywordsRDD = sc.textFile("hdfs://master:9000/spark_study/keywords.txt")



  }

}
