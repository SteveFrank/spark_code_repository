package com.spark.datasource.study;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

/**
 *
 * Hive数据源
 *
 * @author yangqian
 * @date 2021/5/12
 */
public class HiveDataSource {

    public static void main(String[] args) {
        SQLContext hiveContext = SparkSession.builder()
                .appName("HiveDataSource")
                .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
                .config("spark.sql.codegen", "false")
                .config("spark.sql.inMemoryColumnarStorage.compressed", "false")
                .config("spark.sql.inMemoryColumnarStorage.batchSize", "100")
                .enableHiveSupport()
                .getOrCreate()
                .sqlContext();

        // 将学生基本信息导入student_infos表
        hiveContext.sql("DROP TABLE IF EXISTS student_infos");
        // 判断student_infos表是否不存在，如果不存在，则创建该表
        hiveContext.sql("CREATE TABLE IF NOT EXISTS student_infos (name STRING, age INT)");
        // 将学生基本信息数据导入student_infos表
        // VM /home/hadoop/App/Spark/spark_data
        // HECS /home/hadoop/ext_data
        hiveContext.sql("LOAD DATA "
                + "LOCAL INPATH '/home/hadoop/ext_data/student_infos.txt' "
                + "INTO TABLE student_infos");

        // 用同样的方式给student_scores导入数据
        hiveContext.sql("DROP TABLE IF EXISTS student_scores");
        hiveContext.sql("CREATE TABLE IF NOT EXISTS student_scores (name STRING, score INT)");
        // VM /home/hadoop/App/Spark/spark_data
        // HECS /home/hadoop/ext_data
        hiveContext.sql("LOAD DATA "
                + "LOCAL INPATH '/home/hadoop/ext_data/student_scores.txt' "
                + "INTO TABLE student_scores");

        // 第二个功能，执行sql还可以返回DataFrame，用于查询

        // 执行sql查询，关联两张表，查询成绩大于80分的学生
        Dataset<Row> goodStudentsDF = hiveContext.sql("SELECT si.name, si.age, ss.score "
                + "FROM student_infos si "
                + "JOIN student_scores ss ON si.name=ss.name "
                + "WHERE ss.score>=80");

        // 第三个功能，可以将DataFrame中的数据，理论上来说，DataFrame对应的RDD的元素，是Row即可
        // 将DataFrame中的数据保存到hive表中

        // 接着将DataFrame中的数据保存到good_student_infos表中
        hiveContext.sql("DROP TABLE IF EXISTS good_student_infos");
        goodStudentsDF.write().saveAsTable("good_student_infos");

        // 第四个功能，可以用table()方法，针对hive表，直接创建DataFrame

        // 然后针对good_student_infos表，直接创建DataFrame
        hiveContext.table("good_student_infos").show();
    }

}
