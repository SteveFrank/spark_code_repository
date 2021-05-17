package com.spark.streaming.study;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @author yangqian
 * @date 2021/5/17
 */
public class KafkaReceiverWordCount {

    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("KafkaReceiverWordCount");

        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        // 使用KafkaUtils.createStream()方法，针对Kafka 的输入流数据
        Map<String, Integer> topicThreadMap = new HashMap<String, Integer>(1);
        topicThreadMap.put("word_count", 1);

        JavaPairReceiverInputDStream<String, String> lines
                = KafkaUtils.createStream(jssc, "172.16.1.135:2181", "DefaultConsumerGroup", topicThreadMap);

        // 然后开发wordcount逻辑
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<Tuple2<String,String>, String>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public Iterator<String> call(Tuple2<String, String> tuple)
                            throws Exception {
                        return Arrays.asList(tuple._2.split(" ")).iterator();
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

        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }
                });

        wordCounts.print();

        jssc.start();
        jssc.awaitTermination();
        jssc.close();

    }

}
