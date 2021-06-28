package com.spark.sql.study.dataset;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**
 * @date 2021/5/12
 */
public class GenericLoadSave {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("GenericLoadSave").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        Dataset<Row> userDS = sqlContext.read().load("/Users/qian/WorkSpaces/own-workspace/2021/spark_code_repository/spark_java/src/main/resources/spark_sql/users.parquet");
        userDS.printSchema();

        userDS.show();

        userDS.select("name", "favorite_color").show();
        userDS.select("name", "favorite_color").write().save("/Users/qian/WorkSpaces/own-workspace/2021/spark_code_repository/spark_java/src/main/resources/spark_sql/namesAndFavColors.parquet");
    }

}
