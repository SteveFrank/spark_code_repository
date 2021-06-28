package com.spark.project.app.study;

import com.alibaba.fastjson.JSON;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.List;

/**
 * @author frankq
 * @date 2021/6/28
 */
public class AppLogSpark {

    public static void main(String[] args) {
        // 创建Spark配置和上下文对象
        SparkConf conf = new SparkConf()
                .setAppName("AppLogSpark")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 读取日志文件，并创建一个RDD
        // 使用SparkContext的textFile()方法，即可读取本地磁盘文件，或者是HDFS上的文件
        // 创建出来一个初始的RDD，其中包含了日志文件中的所有数据
        JavaRDD<String> accessLogRDD = sc.textFile(
                "/Users/qian/WorkSpaces/own-workspace/2021/spark_code_repository/spark_java/src/main/resources/log_files/access.log");

        // 将RDD映射为key-value格式，为后面的reduceByKey聚合做准备
        JavaPairRDD<String, AccessLogInfo> accessLogPairRDD
                = mapAccessLogRDD2Pair(accessLogRDD);

        // 根据deviceID进行聚合操作
        // 获取每个deviceID的总上行流量、总下行流量、最早访问时间戳
        JavaPairRDD<String, AccessLogInfo> aggrAccessLogPairRDD
                = aggregateByDeviceID(accessLogPairRDD);

        // 将按deviceID聚合RDD的key映射为二次排序key，value映射为deviceID
        JavaPairRDD<AccessLogSortKey, String> accessLogSortRDD
                = mapRDDKey2SortKey(aggrAccessLogPairRDD);

        // 执行二次排序操作，按照上行流量、下行流量以及时间戳进行倒序排序
        // false 倒序
        // true  正序
        JavaPairRDD<AccessLogSortKey ,String> sortedAccessLogRDD =
                accessLogSortRDD.sortByKey(false);

        // 获取top10数据
        List<Tuple2<AccessLogSortKey, String>> top10DataList =
                sortedAccessLogRDD.take(10);
        for(Tuple2<AccessLogSortKey, String> data : top10DataList) {
            System.out.println(data._2 + ": " + JSON.toJSONString(data._1));
        }

        // 关闭Spark上下文
        sc.close();
    }

    /**
     * 将日志RDD映射为key-value的格式
     * @param accessLogRDD 日志RDD
     * @return key-value格式RDD
     */
    private static JavaPairRDD<String, AccessLogInfo> mapAccessLogRDD2Pair(JavaRDD<String> accessLogRDD) {
        return accessLogRDD.mapToPair((PairFunction<String, String, AccessLogInfo>) accessLog -> {
            // 根据 \t 进行四个字段的拆分
            String[] accessLogSplited = accessLog.split("\t");
            // 获取四个字段
            long timeStamp = Long.valueOf(accessLogSplited[0]);
            String deviceID = accessLogSplited[1];
            long upTraffic = Long.valueOf(accessLogSplited[2]);
            long downTraffic = Long.valueOf(accessLogSplited[3]);

            // 将时间戳、上行流量、下行流量，封装为自定义的可序列化对象
            AccessLogInfo accessLogInfo = new AccessLogInfo(timeStamp, upTraffic, downTraffic);

            return new Tuple2<>(deviceID, accessLogInfo);
        });
    }

    /**
     * 根据deviceID进行聚合操作
     * 计算出每个deviceID的总上行流量，总下行流量以及最早的访问时间
     * @param accessLogPairRDD 日志key-value格式的RDD
     * @return 按照deviceID聚合的RDD
     */
    private static JavaPairRDD<String, AccessLogInfo> aggregateByDeviceID(JavaPairRDD<String, AccessLogInfo> accessLogPairRDD) {
        return accessLogPairRDD.reduceByKey(
                (Function2<AccessLogInfo, AccessLogInfo, AccessLogInfo>) (accessLogInfo1, accessLogInfo2) -> {
            long timestamp = Math.min(accessLogInfo1.getTimestamp(), accessLogInfo2.getTimestamp());
            long upTraffic = accessLogInfo1.getUpTraffic() + accessLogInfo2.getUpTraffic();
            long downTraffic = accessLogInfo1.getDownTraffic() + accessLogInfo2.getDownTraffic();

            AccessLogInfo accessLogInfo = new AccessLogInfo(timestamp, upTraffic, downTraffic);
            return accessLogInfo;
        });
    }

    /**
     * 将RDD的key映射为二次排序key
     * @param aggrAccessLogPairRDD 按deviceID聚合RDD
     * @return 二次排序key RDD
     */
    private static JavaPairRDD<AccessLogSortKey, String> mapRDDKey2SortKey(
            JavaPairRDD<String, AccessLogInfo> aggrAccessLogPairRDD) {
        return aggrAccessLogPairRDD.mapToPair(
                (PairFunction<Tuple2<String, AccessLogInfo>, AccessLogSortKey, String>) tuple -> {
                    // 获取tuple数据进行转换
                    String deviceId = tuple._1;
                    AccessLogInfo accessLogInfo = tuple._2;

                    // 将日志信息封装为二次排序key
                    AccessLogSortKey accessLogSortKey = new AccessLogSortKey(
                            accessLogInfo.getUpTraffic(),
                            accessLogInfo.getDownTraffic(),
                            accessLogInfo.getTimestamp());
                    return new Tuple2<AccessLogSortKey, String>(accessLogSortKey, deviceId);
                }
        );
    }

}
