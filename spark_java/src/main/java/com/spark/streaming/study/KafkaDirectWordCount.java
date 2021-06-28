package com.spark.streaming.study;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.*;

/**
 * 基于Kafka Direct方式的实时wordcount程序
 * @date 2021/5/17
 */
public class KafkaDirectWordCount {

    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("KafkaDirectWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        // 首先创建一份Kafka参数的map
        Map<String, String> kafkaParams = new HashMap<String, String>(1);
        kafkaParams.put("metadata.broker.list", "172.16.1.135:9092");

        // 然后创建一个Set在该集合中放入需要读取的topic
        Set<String> topics = new HashSet<String>(1);
        topics.add("word_count");

        // 创建输入的DStream
        JavaPairInputDStream<String, String> lines = KafkaUtils.createDirectStream(
                jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topics
        );

        // 执行word count操作
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<Tuple2<String, String>, String>() {
            private static final long serialVersionUID = -5816944499842738285L;
            @Override
            public Iterator<String> call(Tuple2<String, String> tuple2) throws Exception {
                return Arrays.asList(tuple2._2.split(" ")).iterator();
            }
        });

        JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            private static final long serialVersionUID = 2743943725203840205L;

            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word, 1);
            }
        });

        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = -8624478686913452572L;

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
