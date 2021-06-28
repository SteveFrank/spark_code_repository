package com.spark.study.sortWordCount;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 *
 * 1、对文本文件内的每个单词都统计出其出现的次数。
 * 2、按照每个单词出现次数的数量，降序排序。
 *
 * @date 2021/5/9
 */
public class SortWordCountApp {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Accumulator").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        // 创建lines RDD
        JavaRDD<String> lines = sc.textFile("/Users/qian/WorkSpaces/own-workspace/2021/spark_code_repository/spark_java/src/main/resources/spark.txt");
        // 转为单词
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            private static final long serialVersionUID = 680144807634112999L;
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });
        // 转为键值对
        JavaPairRDD<String, Integer> wordsMap = words.mapToPair(new PairFunction<String, String, Integer>() {
            private static final long serialVersionUID = -6532864256147268632L;
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word, 1);
            }
        });
        // 累加
        JavaPairRDD<String, Integer> wordsCount = wordsMap.reduceByKey(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = -7872343918743824190L;
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        // key - value的翻转映射
        JavaPairRDD<Integer, String> wordsCountMapRevert = wordsCount.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            private static final long serialVersionUID = 4823747091090019174L;
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> m) throws Exception {
                return new Tuple2<Integer, String>(m._2, m._1);
            }
        });
        // 按照key进行排序
        JavaPairRDD<Integer, String> wordsCountSort = wordsCountMapRevert.sortByKey(false);
        // 再次翻转
        JavaPairRDD<String, Integer> wordsCountResult = wordsCountSort.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            private static final long serialVersionUID = 4219176666458292100L;

            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> m) throws Exception {
                return new Tuple2<String, Integer>(m._2, m._1);
            }
        });
        // 打印结果
        wordsCountResult.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            private static final long serialVersionUID = 8201060646259180465L;
            @Override
            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                System.out.println(stringIntegerTuple2._1 + ",  times : " + stringIntegerTuple2._2);
            }
        });
        sc.close();
    }

}
