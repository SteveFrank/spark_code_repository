package com.spark.study.variable;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.util.AccumulatorV2;

import java.util.Arrays;
import java.util.List;

/**
 * @date 2021/5/9
 */
public class AccumulatorVariable {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Accumulator").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        final Accumulator<Integer> sum = sc.accumulator(0);
        List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> numbers = sc.parallelize(numberList);
        numbers.foreach(new VoidFunction<Integer>() {
            private static final long serialVersionUID = -6623107919175446580L;
            @Override
            public void call(Integer integer) throws Exception {
                sum.add(integer);
            }
        });
        System.out.println(sum);
        sc.close();
    }

}
