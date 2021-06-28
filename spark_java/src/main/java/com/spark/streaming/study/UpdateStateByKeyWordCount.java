package com.spark.streaming.study;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 *
 * 全局计数
 * 基于updateStateByKey算子实现缓存机制的实时wordcount程序
 *
 * 为每个key维护一个state
 *
 * @date 2021/5/17
 */
public class UpdateStateByKeyWordCount {

    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("UpdateStateByKeyWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        // 第一点：如果要使用updateStateByKey算子，就必须设置一个checkpoint目录，开启checkpoint机制
        // 这样的话才能吧每个key对应的state除了内存中有，你那么是不是也要checkpoint一份
        // 因为你要长期保存一份key的state的话，那么spark streaming是要求必须用checkpoint的，以便于在内存数据丢失的时候，可以在checkpoint中恢复数据

        // 开启checkpoint机制
        jssc.checkpoint("hdfs://master:9000/check_point/ch/wordcount_checkpoint");
//        jssc.checkpoint("/Users/qian/WorkSpaces/own-workspace/2021/spark_code_repository/check_point/ch/wordcount_checkpoint");
        // 然后先实现一个基础的wordcount逻辑
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);

        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            private static final long serialVersionUID = -6501620095026705202L;
            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });

        JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            private static final long serialVersionUID = -2356107376676525881L;
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word, 1);
            }
        });

        // 到了这里，就不一样了，之前的话，是不是直接就是pairs.reduceByKey
        // 然后，就可以得到每个时间段的batch对应的RDD，计算出来的单词计数
        // 然后，可以打印出那个时间段的单词计数
        // 但是，有个问题，你如果要统计每个单词的全局的计数呢？
        // 就是说，统计出来，从程序启动开始，到现在为止，一个单词出现的次数，那么就之前的方式就不好实现
        // 就必须基于redis这种缓存，或者是mysql这种db，来实现累加

        // 但是，我们的updateStateByKey，就可以实现直接通过Spark维护一份每个单词的全局的统计次数

        // 可见 updateStateByKey 的最大作用就是可以做到全局统计的作用
        JavaPairDStream<String, Integer> wordCounts = pairs.updateStateByKey(
            /*
             * 这里的 optional 相当于 Scala 中的样例类，就是Option
             * 它代表了一个值的存在状态，可能存在，也可能不存在
             */
            new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
                private static final long serialVersionUID = 7505132090048986325L;
                @Override
                public Optional<Integer> call(List<Integer> values, Optional<Integer> state) throws Exception {
                    // 首先定义一个全局的单词计数
                    Integer newValue = 0;
                    // 其次判断，state是否存在，如果不存在，说明是一个key第一次出现
                    // 如果存在，说明这个key之前已经统计过了全局的次数了
                    if (state.isPresent()) {
                        newValue = state.get();
                    }
                    // 接着，将本次新出现的值，都累加到newValue上去，就是一个key目前的全局统计
                    // 次数
                    for (Integer value : values) {
                        newValue += value;
                    }
                    return Optional.of(newValue);
                }
            });

        // 到这里为止，相当于是，每个batch过来是，计算到pairs DStream，就会执行全局的updateStateByKey
        // 算子，updateStateByKey返回的JavaPairDStream，其实就代表了每个key的全局的计数
        // 打印出来
        wordCounts.print();

        jssc.start();
        jssc.awaitTermination();
        jssc.close();

    }

}
