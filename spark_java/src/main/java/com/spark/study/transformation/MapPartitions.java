package com.spark.study.transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.*;

/**
 * mapPartitions 算子
 */
public class MapPartitions {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("MapPartitions")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 模拟数据输入
        List<String> studentNames = Arrays.asList("张三", "李四", "王二", "麻子");
        JavaRDD<String> studentNamesRDD = sc.parallelize(studentNames, 2);

        final Map<String, Double> studentScoreMap = new HashMap<String, Double>(4);
        studentScoreMap.put("张三", 278.5);
        studentScoreMap.put("李四", 290.0);
        studentScoreMap.put("王二", 301.0);
        studentScoreMap.put("麻子", 205.0);

        // mapPartitions
        // 类似map，不同之处在于，map算子，一次就处理一个partition中的一条数据
        // mapPartitions算子，一次处理一个partition中所有的数据

        // 推荐使用场景
        // 如果RDD的数据量不是特别大，那么建议采用mapPartitions算子替代map算子，可以加快处理速度
        // 但是如果你的RDD的数据量不是特别大，比如10亿，那么不建议使用mapPartitions，可能会造成内存的溢出
        // 利用 mapPartitions 转化为RDD
        JavaRDD<Double> studentScoresRDD = studentNamesRDD.mapPartitions(
                (FlatMapFunction<Iterator<String>, Double>) iterator -> {
                    // 因为算子一次处理一个partition的所有数据
                    // call函数接收的参数，是iterator类型，代表了partition中所有数据的迭代器
                    // 返回的是一个iterable类型，代表了返回多条记录，通常使用List类型
                    List<Double> studentScoreList = new ArrayList<Double>();
                    while (iterator.hasNext()) {
                        String studentName = iterator.next();
                        Double studentScore = studentScoreMap.get(studentName);
                        studentScoreList.add(studentScore);
                    }
                    return studentScoreList.iterator();
                });

        // 打印最终的结果
        for (Double studentScore : studentScoresRDD.collect()) {
            System.out.println(studentScore);
        }

        sc.close();
    }

}
