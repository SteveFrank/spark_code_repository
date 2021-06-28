package com.spark.study.transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @author frankq
 * @date 2021/6/28
 */
public class AggregateByKey {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("AggregateByKey")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("/Users/qian/WorkSpaces/own-workspace/2021/spark_code_repository/spark_java/src/main/resources/spark.txt");
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            private static final long serialVersionUID = -6425428993943774966L;

            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });

        JavaPairRDD<String, Integer> paris = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<>(word, 1);
            }
        });

        // aggregateByKey，分为三个参数
        // reduceByKey认为是aggregateByKey的简化版
        // aggregateByKey最重要的一点是，多提供了一个函数，Seq Function
        // 就是说自己可以控制如何对每个partition中的数据进行先聚合，类似于mapreduce中的，map-side combine
        // 然后才是对所有partition中的数据进行全局聚合

        // 第一个参数是，每个key的初始值
        // 第二个是个函数，Seq Function，如何进行shuffle map-side的本地聚合
        // 第三个是个函数，Combiner Function，如何进行shuffle reduce-side的全局聚合

        JavaPairRDD<String, Integer> wordCounts = paris.aggregateByKey(
                0,
                (Function2<Integer, Integer, Integer>) Integer::sum,
                (Function2<Integer, Integer, Integer>) Integer::sum
        );

        List<Tuple2<String, Integer>> wordCountList = wordCounts.collect();
        for(Tuple2<String, Integer> wordCount : wordCountList) {
            System.out.println(wordCount);
        }

        sc.close();

    }

}
