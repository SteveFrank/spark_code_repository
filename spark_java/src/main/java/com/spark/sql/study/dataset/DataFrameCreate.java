package com.spark.sql.study.dataset;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;


/**
 *
 * Java 代码无法在本地跑，需要打包后才可以跑
 *
 * 使用json文件创建dataframe
 *
 * @date 2021/5/11
 */
public class DataFrameCreate {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("DataFrameCreate");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = SparkSession.builder()
                .config(conf)
                .getOrCreate()
                .sqlContext();

        // Java 代码无法在本地跑，需要打包后才可以跑
        // Spark2.X DataFrame和DataSet的API进行了统一
        Dataset<Row> dataset = sqlContext.read().json("hdfs://master:9000/spark_sql_file/students.json");
        dataset.show();
        // 打印DataFrame
        dataset.printSchema();
        // 查询某列的所有数据
        dataset.select("name").show();
        // 查询某几列所有的数据，并对列进行计算
        dataset.select(dataset.col("name"), dataset.col("age").plus(1)).show();
        // 根据某一列的值进行过滤
        dataset.filter(dataset.col("age").gt(18)).show();
        // 根据某一列进行分组，然后进行聚合
        dataset.groupBy(dataset.col("age")).count().show();
    }

}
