package com.spark.study.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

/**
 * @date 2021/5/7
 */
public class LineCount {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("LineCount").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> lines = sc.textFile("/Users/qian/WorkSpaces/own-workspace/2021/spark_code_repository/wordcount/src/main/resources/spark.txt");

        // 执行mapToPair算子，将每一行映射成为(line, 1)的这种key-value对的格式
        // 然后后面才能够统计每一行的出现次数
        JavaPairRDD<String, Integer> pairs = lines.mapToPair(new PairFunction<String, String, Integer>() {
            private static final long serialVersionUID = 8596914800309727875L;
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });
        // 对pairs RDD执行reduceByKey算子，统计出每一行出现的总数
        JavaPairRDD<String, Integer> lineCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = -5506528970018801327L;
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        lineCounts.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            private static final long serialVersionUID = 192246454982815086L;
            @Override
            public void call(Tuple2<String, Integer> t) throws Exception {
                System.out.println(t._1 + " appears " + t._2 + " times. ");
            }
        });
        sc.close();
    }

}
