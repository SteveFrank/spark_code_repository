package com.spark.streaming.study;

import com.spark.util.ConnectionPool;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @date 2021/5/18
 */
public class PersistWordCount {

    public static void main(String[] args) throws InterruptedException {

        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("PersistWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        jssc.checkpoint("/Users/qian/WorkSpaces/own-workspace/2021/spark_code_repository/check_point/ch/wordcount_checkpoint");

        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);

        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }

        });

        JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public Tuple2<String, Integer> call(String word)
                            throws Exception {
                        return new Tuple2<String, Integer>(word, 1);
                    }
                });

        JavaPairDStream<String, Integer> wordCounts =
                pairs.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public Optional<Integer> call(List<Integer> values, Optional<Integer> state) throws Exception {
                        Integer newValue = 0;
                        if(state.isPresent()) {
                            newValue = state.get();
                        }
                        for(Integer value : values) {
                            newValue += value;
                        }
                        return Optional.of(newValue);
                    }
                });


        wordCounts.foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {
            private static final long serialVersionUID = 3623889479147422048L;

            @Override
            public void call(JavaPairRDD<String, Integer> pairRDD) throws Exception {
                pairRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Integer>>>() {
                    private static final long serialVersionUID = -2190847972701113727L;
                    @Override
                    public void call(Iterator<Tuple2<String, Integer>> tuple) throws Exception {
                        // 给每个partition，获取一个连接
                        Connection conn = ConnectionPool.getConnection();

                        // 遍历partition中的数据，使用一个连接，插入数据库
                        Tuple2<String, Integer> wordCount = null;
                        while(tuple.hasNext()) {
                            wordCount = tuple.next();
                            String sql = "insert into test_spark_db.wordcount(word, count) values('" + wordCount._1 + "'," + wordCount._2 + ")";
                            Statement stmt = conn.createStatement();
                            stmt.executeUpdate(sql);
                        }

                        // 用完以后，将连接还回去
                        ConnectionPool.returnConnection(conn);
                    }
                });
            }
        });


        jssc.start();
        jssc.awaitTermination();
        jssc.close();

    }

}
