package com.spark.streaming.study;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * 基于transform的实时广告计费日志黑名单过滤
 * 类比于广告业务的大数据系统（黑心商家的胡乱点击）
 *
 * @date 2021/5/17
 */
public class TransformBlacklist {

    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("TransformBlacklist");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        // 用户对于网站上的广告进行点击
        // 点击后是否进行实时计费，点一次，算一次钱
        // 对于帮助无量商家刷广告的人，建立一套黑名单机制
        // 只要是黑名单中的用户点击广告，就进行过滤

        // 模拟一份黑名单RDD
        List<Tuple2<String, Boolean>> blacklist = new ArrayList<Tuple2<String, Boolean>>();
        blacklist.add(new Tuple2<String, Boolean>("tom", true));
        final JavaPairRDD<String, Boolean> blacklistRDD = jssc.sparkContext().parallelizePairs(blacklist);

        // 上报的日志格式进行简化，date username方式
        final JavaReceiverInputDStream<String> adsClickDStream = jssc.socketTextStream("localhost", 9999);

        // 所以要对于输入的数据，进行一下抓换操作 转换为 (username, date username)
        // 以便于，后面对于每个batch RDD，对于定义好的黑名单RDD进行join操作
        JavaPairDStream<String, String> userAdsClickLogDStream = adsClickDStream.mapToPair(
                new PairFunction<String, String, String>() {
            private static final long serialVersionUID = -6763852093297742422L;
            @Override
            public Tuple2<String, String> call(String adsClickLog) throws Exception {
                return new Tuple2<String, String>(adsClickLog.split(" ")[1], adsClickLog);
            }
        });

        // 然后，就可以进行transform操作，将每个batch的RDD，与黑名单的RDD进行join、filter、map等操作
        // 实时进行黑名单的过滤
        JavaDStream<String> validAdsClickLogDStream = userAdsClickLogDStream.transform(new Function<JavaPairRDD<String, String>, JavaRDD<String>>() {
            private static final long serialVersionUID = 8771224496965138322L;
            @Override
            public JavaRDD<String> call(JavaPairRDD<String, String> userAdsClickLogRDD) throws Exception {
                // 这里为什么用左外连接？
                // 因为，并不是每个用户都存在于黑名单中的
                // 所以，如果直接用join，那么没有存在于黑名单中的数据，会无法join到
                // 就给丢弃掉了
                // 所以，这里用leftOuterJoin，就是说，哪怕一个user不在黑名单RDD中，没有join到
                // 也还是会被保存下来的
                JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> joinedRDD
                        = userAdsClickLogRDD.leftOuterJoin(blacklistRDD);

                // 左外连接后，执行filter算子，过滤无效的数据
                JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> filteredRDD
                        = joinedRDD.filter(new Function<Tuple2<String, Tuple2<String, Optional<Boolean>>>, Boolean>() {
                    private static final long serialVersionUID = 1518803694632344142L;
                    @Override
                    public Boolean call(Tuple2<String, Tuple2<String, Optional<Boolean>>> tuple) throws Exception {
                        // 这里的tuple就是每个用户对应的访问日志，和在黑名单中的状态
                        if (tuple._2._2().isPresent() && tuple._2._2().get()) {
                            return false;
                        }
                        return true;
                    }
                });

                // 此时的filteredRDD中，就只剩下没有被黑名单过滤的用户点击了
                // 进行map操作，转换为需要的格式
                JavaRDD<String> validAdsClickLogRDD = filteredRDD.map(new Function<Tuple2<String, Tuple2<String, Optional<Boolean>>>, String>() {
                    private static final long serialVersionUID = 3747537728304566022L;
                    @Override
                    public String call(Tuple2<String, Tuple2<String, Optional<Boolean>>> tuple) throws Exception {
                        return tuple._2._1;
                    }
                });

                return validAdsClickLogRDD;
            }
        });

        validAdsClickLogDStream.print();
        jssc.start();
        jssc.awaitTermination();
        jssc.close();

    }

}
