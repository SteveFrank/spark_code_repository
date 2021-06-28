package com.spark.study.variable;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

import java.util.Arrays;
import java.util.List;

/**
 * @date 2021/5/9
 */
public class BroadcastVariable {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("BroadcastVariable").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        final int factor = 3;
        final Broadcast<Integer> factoBroadcast = sc.broadcast(factor);
        List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> numbers = sc.parallelize(numberList);

        // 让集合中的每个数字都乘以外部定义的那个factor值
        JavaRDD<Integer> multipleNumbers = numbers.map(new Function<Integer, Integer>() {
            private static final long serialVersionUID = -1119341473428413030L;
            @Override
            public Integer call(Integer v1) throws Exception {
                // 共享变脸
                int factor = factoBroadcast.value();
                return v1 * factor;
            }
        });

        multipleNumbers.foreach(new VoidFunction<Integer>() {
            private static final long serialVersionUID = 6332910179680904591L;
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });

        sc.close();

    }

}
