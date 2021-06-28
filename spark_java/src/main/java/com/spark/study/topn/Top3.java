package com.spark.study.topn;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.List;

/**
 * @date 2021/5/9
 */
public class Top3 {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Top3").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("/Users/qian/WorkSpaces/own-workspace/2021/spark_code_repository/spark_java/src/main/resources/top.txt");
        JavaPairRDD<Integer, Integer> pairs = lines.mapToPair(new PairFunction<String, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(String t) throws Exception {
                return new Tuple2<Integer, Integer>(Integer.valueOf(t), Integer.valueOf(t));
            }
        });
        JavaPairRDD<Integer, Integer> sortedPairs = pairs.sortByKey(false);

        JavaRDD<Integer> sortedNumbers = sortedPairs.map(new Function<Tuple2<Integer, Integer>, Integer>() {
            @Override
            public Integer call(Tuple2<Integer, Integer> v1) throws Exception {
                return v1._1;
            }
        });

        List<Integer> top3 = sortedNumbers.take(3);
        System.out.println(top3);

        sc.close();


    }

}
