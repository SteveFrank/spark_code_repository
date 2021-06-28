package com.spark.datasource.study;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Int;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * @date 2021/5/12
 */
public class JsonDataSource {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local").setAppName("JsonDataSource");
        JavaSparkContext sc = new JavaSparkContext(conf);

        SQLContext sqlContext = new SQLContext(sc);

        Dataset<Row> studentScoreDS = sqlContext.read().json("/Users/qian/WorkSpaces/own-workspace/2021/spark_code_repository/spark_java/src/main/resources/json/students.json");
        studentScoreDS.createOrReplaceTempView("student_scores");

        // 学生的成绩DataSet
        Dataset<Row> goodStudentScoresDS = sqlContext.sql("select name, score from student_scores where score >= 80");

        List<String> goodStudentNames = goodStudentScoresDS.javaRDD().map(new Function<Row, String>() {
            private static final long serialVersionUID = 2285732451218399708L;
            @Override
            public String call(Row row) throws Exception {
                return row.getString(0);
            }
        }).collect();

        // 针对包含json传的javaRDD创建DataSet
        List<String> studentInfoJSONS = new ArrayList<String>();
        studentInfoJSONS.add("{\"name\":\"Leo\", \"age\":18}");
        studentInfoJSONS.add("{\"name\":\"Marry\", \"age\":17}");
        studentInfoJSONS.add("{\"name\":\"Jack\", \"age\":19}");

        JavaRDD<String> studentInfoJSONsRDD = sc.parallelize(studentInfoJSONS);
        Dataset<Row> studentInfosDS = sqlContext.read().json(studentInfoJSONsRDD);

        // 针对学生基本信息构建临时表
        studentInfosDS.createOrReplaceTempView("student_infos");

        String sql = "select name, age from student_infos where name in (";

        for (int i = 0; i < goodStudentNames.size(); i++) {
            sql += "'" + goodStudentNames.get(i) + "'";
            if (i < goodStudentNames.size() - 1) {
                sql += ",";
            }
        }
        sql += ")";
        // 学生基本信息的DataSet
        Dataset<Row> goodsStudentInfosDS = sqlContext.sql(sql);

        goodsStudentInfosDS.printSchema();
        goodsStudentInfosDS.show();

        JavaPairRDD<String, Tuple2<Integer, Integer>> goodStudentsRDD =
                goodStudentScoresDS.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
                    private static final long serialVersionUID = -5282421837742849450L;

                    @Override
                    public Tuple2<String, Integer> call(Row row) throws Exception {
                        return new Tuple2<String, Integer>(
                                row.getString(0),
                                Integer.valueOf(String.valueOf(row.getLong(1)))
                        );
                    }
                }).join(goodsStudentInfosDS.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
                    private static final long serialVersionUID = -3482294033491512059L;

                    @Override
                    public Tuple2<String, Integer> call(Row row) throws Exception {
                        return new Tuple2<String, Integer>(
                                row.getString(0),
                                Integer.valueOf(String.valueOf(row.getLong(1)))
                        );
                    }
                }));
        // 然后将封装在RDD中的好学生的全部信息，转换为一个JavaRDD<Row>的格式
        // （将JavaRDD，转换为DataFrame）
        JavaRDD<Row> goodStudentRowsRDD = goodStudentsRDD.map(

                new Function<Tuple2<String,Tuple2<Integer,Integer>>, Row>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Row call(
                            Tuple2<String, Tuple2<Integer, Integer>> tuple)
                            throws Exception {
                        return RowFactory.create(tuple._1, tuple._2._1, tuple._2._2);
                    }

                });

        // 创建一份元数据，将JavaRDD<Row>转换为DataFrame
        List<StructField> structFields = new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("score", DataTypes.IntegerType, true));
        structFields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
        StructType structType = DataTypes.createStructType(structFields);

        Dataset<Row> goodStudentsDF = sqlContext.createDataFrame(goodStudentRowsRDD, structType);

        goodStudentsDF.printSchema();
        goodStudentsDF.show();

        // 将好学生的全部信息保存到一个json文件中去
        // （将DataFrame中的数据保存到外部的json文件中去）
        goodStudentsDF.write().format("json").save("/Users/qian/WorkSpaces/own-workspace/2021/spark_code_repository/spark_java/src/main/resources/json/good-students");

    }

}
