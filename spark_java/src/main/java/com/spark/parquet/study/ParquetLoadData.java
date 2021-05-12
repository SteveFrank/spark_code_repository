package com.spark.parquet.study;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import java.util.List;

/**
 *
 * 操作列式数据
 * 如何处理列式数据
 * @author yangqian
 * @date 2021/5/12
 */
public class ParquetLoadData {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("ParquetLoadData").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = SparkSession.builder().config(conf).getOrCreate().sqlContext();

        // 读取数据创建DataSet
        Dataset<Row> userDS = sqlContext.read().parquet("/Users/qian/WorkSpaces/own-workspace/2021/spark_code_repository/spark_java/src/main/resources/parquet/users.parquet");
        // 将DataFrame注册为临时表，然后使用SQL查询需要的数据
        userDS.createOrReplaceTempView("users");
        Dataset<Row> userNamesDS = sqlContext.sql("select name from users");
        // 查询出来的数据进行transformation操作，处理数据，然后打印数据
        List<String> userNames = userNamesDS.javaRDD().map(new Function<Row, String>() {
            private static final long serialVersionUID = 1457671469889960630L;

            @Override
            public String call(Row row) throws Exception {
                return "Name : " + row.getString(0);
            }
        }).collect();

        for (String name : userNames) {
            System.out.println(name);
        }

    }

}
