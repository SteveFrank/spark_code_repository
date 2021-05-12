package com.spark.sql.study.dataset;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**
 * @author yangqian
 * @date 2021/5/12
 */
public class ManuallySpecifyOptions {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("ManuallySpecifyOptions").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        Dataset<Row> peopleDS = sqlContext
                .read()
                .format("json")
                .load("/Users/qian/WorkSpaces/own-workspace/2021/spark_code_repository/spark_java/src/main/resources/spark_sql/people.json");

        peopleDS.select("name")
                .show();

        peopleDS.select("name")
                .write().format("parquet")
                .save("/Users/qian/WorkSpaces/own-workspace/2021/spark_code_repository/spark_java/src/main/resources/spark_sql/peopleName_java.parquet");

    }

}
