package com.spark.study.secondarySort;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

/**
 * @author yangqian
 * @date 2021/5/9
 */
public class SecondarySort {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Accumulator").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        // 创建lines RDD
        JavaRDD<String> lines = sc.textFile("/Users/qian/WorkSpaces/own-workspace/2021/spark_code_repository/spark_java/src/main/resources/sort.txt");

        JavaPairRDD<SecondarySortKey, String> pairs = lines.mapToPair(new PairFunction<String, SecondarySortKey, String>() {
            private static final long serialVersionUID = -87765156597589016L;

            @Override
            public Tuple2<SecondarySortKey, String> call(String line) throws Exception {
                String[] lineSplited = line.split(" ");
                SecondarySortKey key = new SecondarySortKey(
                        Integer.parseInt(lineSplited[0]), Integer.parseInt(lineSplited[1]));
                return new Tuple2<SecondarySortKey, String>(key, line);
            }
        });

        JavaPairRDD<SecondarySortKey, String> sortedPairs = pairs.sortByKey();
        JavaRDD<String> sortedLines = sortedPairs.map(new Function<Tuple2<SecondarySortKey, String>, String>() {
            private static final long serialVersionUID = 3583042731442802959L;

            @Override
            public String call(Tuple2<SecondarySortKey, String> v1) throws Exception {
                return v1._2;
            }
        });
        sortedLines.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

        sc.close();
    }

}
