package com.spark.study.persist;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

/**
 * @date 2021/5/9
 */
public class PersistOperation {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Persist").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // cache() 操作 或者 persist() 操作的使用，都是有规则的
        // 必须在transformation或者textFile等创建了一个RDD之后，直接连续调用cache()或者persist()才可以
        // 如果先创建了一个RDD, 然后单独另起一行执行是没有作用的，会报错，大量文件会丢失
        JavaRDD<String> lines
                = sc
                .textFile("/Users/qian/WorkSpaces/own-workspace/2021/spark_code_repository/spark_java/src/main/resources/spark.txt")
                .persist(StorageLevel.MEMORY_AND_DISK());
//                .cache();
        long beginTime = System.currentTimeMillis();

        long count = lines.count();
        System.out.println(count);

        long endTime = System.currentTimeMillis();
        System.out.println("1 ==> cost " + (endTime - beginTime) + " milliseconds");

        beginTime = System.currentTimeMillis();
        count = lines.count();
        System.out.println(count);
        endTime = System.currentTimeMillis();
        System.out.println("2 ==> cost " + (endTime - beginTime) + " milliseconds");
        sc.close();
    }

}
