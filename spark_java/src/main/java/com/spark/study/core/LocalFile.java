package com.spark.study.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

/**
 * @author yangqian
 * @date 2021/5/7
 */
public class LocalFile {

    public static void main(String[] args) {
        // 创建SparkConf
        SparkConf conf = new SparkConf().setAppName("LocalFile").setMaster("local");
        // 创建JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);
        // 加载创建本地文件的RDD
        JavaRDD<String> lines = sc.textFile("/Users/qian/WorkSpaces/own-workspace/2021/spark_code_repository/wordcount/src/main/resources/spark.txt");
        // 统计文本的子树
        JavaRDD<Integer> lineLength = lines.map(new Function<String, Integer>() {
            private static final long serialVersionUID = -8964556304799736656L;

            @Override
            public Integer call(String v1) throws Exception {
                return v1.length();
            }
        });
        int count = lineLength.reduce(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = -7270559097077079630L;

            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        System.out.println(count);
        sc.close();
    }

}
