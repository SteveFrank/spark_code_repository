package com.spark.parquet.study.autoPartition;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

/**
 *
 * 自动分区推断
 *
 * @author yangqian
 * @date 2021/5/12
 */
public class ParquetPartitionDiscovery {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("ParquetPartitionDiscovery");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = SparkSession.builder().config(conf).getOrCreate().sqlContext();
        Dataset<Row> userDS = sqlContext.read().parquet("hdfs://master:9000/spark-study/users/gender=male/country=US/users.parquet");
        userDS.printSchema();
        userDS.show();
    }

}
