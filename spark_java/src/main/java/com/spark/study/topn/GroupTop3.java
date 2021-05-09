package com.spark.study.topn;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * @author yangqian
 * @date 2021/5/9
 */
public class GroupTop3 {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("GroupTop3").setMaster("local");
        final JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("/Users/qian/WorkSpaces/own-workspace/2021/spark_code_repository/spark_java/src/main/resources/score.txt");
        JavaPairRDD<String, Integer> scoreWithClass = lines.mapToPair(new PairFunction<String, String, Integer>() {
            private static final long serialVersionUID = 5927267795933596641L;

            @Override
            public Tuple2<String, Integer> call(String line) throws Exception {
                return new Tuple2<String, Integer>(line.split(" ")[0], Integer.parseInt(line.split(" ")[1]));
            }
        });
        // 按照班分组数据（分数）
        JavaPairRDD<String, Iterable<Integer>> groupPairs = scoreWithClass.groupByKey();

        JavaPairRDD<String, Iterable<Integer>> top3Score = groupPairs.mapToPair(new PairFunction<Tuple2<String, Iterable<Integer>>, String, Iterable<Integer>>() {
            private static final long serialVersionUID = -3928427695844412087L;

            @Override
            public Tuple2<String, Iterable<Integer>> call(
                    Tuple2<String, Iterable<Integer>> classScores) throws Exception {
                String className = classScores._1;
                Iterator<Integer> scores = classScores._2.iterator();
                Integer[] top3 = new Integer[3];
                while (scores.hasNext()) {
                    Integer score = scores.next();
                    for (int i = 0; i < 3; i++) {
                        if (top3[i] == null) {
                            top3[i] = score;
                        } else if (score > top3[i]) {
                            int tmp = top3[i];
                            top3[i] = score;

                            if (i < top3.length - 1) {
                                top3[i + 1] = tmp;
                            }
                        }
                    }
                }
                return new Tuple2<String, Iterable<Integer>>(className, Arrays.asList(top3));
            }
        });

        top3Score.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            private static final long serialVersionUID = -7251855902775553443L;

            @Override
            public void call(Tuple2<String, Iterable<Integer>> tuple2) throws Exception {
                System.out.println(tuple2._1 + "  --  " + tuple2._2);
            }
        });

    }

}
