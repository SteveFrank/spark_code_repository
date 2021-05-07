package com.spark.study.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;
import java.util.List;

/**
 * @author yangqian
 * @date 2021/5/6
 */
public class ParallelizeCollection {

    public static void main(String[] args) {

        // 1、创建SparkConf
        SparkConf conf = new SparkConf().setAppName("ParallelizeCollection").setMaster("local");
        // 2、创建JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3、要通过并行化集合的方式创建RDD，调用SparkContext以及其子类的parallelize()方法
        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> numberRDD = sc.parallelize(numbers);

        // 4、执行Reduce算子操作
        int sum = numberRDD.reduce(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = -7813707909699977411L;
            @Override
            public Integer call(Integer num1, Integer num2) throws Exception {
                return num1 + num2;
            }
        });

        System.out.println("1 到 10 累加和为:" + sum);

        // 关闭JavaSparkContext
        sc.close();

    }
}
