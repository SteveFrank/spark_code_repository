package com.spark.datasource.study;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

/**
 * @author yangqian
 * @date 2021/5/12
 */
public class JdbcDataSource {

    public static void main(String[] args) {
        SQLContext sqlContext = SparkSession.builder()
                .master("local[1]")
                .appName("JdbcDataSource")
                .config("spark.sql.codegen", "false")
                .config("spark.sql.inMemoryColumnarStorage.compressed", "false")
                .config("spark.sql.inMemoryColumnarStorage.batchSize", "100")
                .getOrCreate()
                .sqlContext();
        // 总结一下
        // jdbc数据源
        // 首先，是通过SQLContext的read系列方法，将mysql中的数据加载为DataFrame
        // 然后可以将DataFrame转换为RDD，使用Spark Core提供的各种算子进行操作
        // 最后可以将得到的数据结果，通过foreach()算子，写入mysql、hbase、redis等等db / cache中

        // 分别将mysql中两张表的数据加载为DataFrame
        Map<String, String> options = new HashMap<String, String>();
        options.put("url", "jdbc:mysql://slave01:3306/test_spark_db");
        options.put("user", "root");
        options.put("password", "Hadoop123!@#");
        options.put("dbtable", "student_infos");
        Dataset studentInfosDF = sqlContext.read().format("jdbc")
                .options(options).load();
        studentInfosDF.show();

        options.put("dbtable", "student_scores");
        Dataset studentScoresDF = sqlContext.read().format("jdbc")
                .options(options).load();
        studentScoresDF.show();
    }

}
