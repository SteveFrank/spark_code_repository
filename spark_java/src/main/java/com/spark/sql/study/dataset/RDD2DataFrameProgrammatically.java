package com.spark.sql.study.dataset;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * @author yangqian
 * @date 2021/5/12
 */
public class RDD2DataFrameProgrammatically {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("RDD2DataFrameProgrammatically");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = SparkSession.builder().config(conf).getOrCreate().sqlContext();

        final JavaRDD<String> lines = sc.textFile("/Users/qian/WorkSpaces/own-workspace/2021/spark_code_repository/spark_java/src/main/resources/spark_sql/students.txt");

        // 1、往Row中赛数据，需要注意使用什么格式塞入数据，就使用什么格式进行转换
        JavaRDD<Row> studentRDD = lines.map(new Function<String, Row>() {
            private static final long serialVersionUID = -9223067078328557408L;

            @Override
            public Row call(String line) throws Exception {
                String[] lineSplit = line.split(",");
                return RowFactory.create(Integer.valueOf(lineSplit[0]), lineSplit[1], Integer.parseInt(lineSplit[2]));
            }
        });

        // 2、动态构造元数据
        List<StructField> structFields = new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField("id", DataTypes.IntegerType, true));
        structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));

        StructType structType = DataTypes.createStructType(structFields);

        // 3、使用动态构造元数据，将RDD转换为DataFrame
        Dataset<Row> studentDF = sqlContext.createDataFrame(studentRDD, structType);

        studentDF.registerTempTable("students");

        Dataset<Row> teenagerDF = sqlContext.sql("select id, name, age from students where age <= 18");

        List<Row> rows = teenagerDF.javaRDD().collect();
        for (Row row : rows) {
            System.out.println(row);
        }

    }

}
