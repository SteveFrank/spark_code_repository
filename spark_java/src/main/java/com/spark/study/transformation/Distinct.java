package com.spark.study.transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;
import java.util.List;

/**
 * @author frankq
 * @date 2021/6/28
 */
public class Distinct {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("Distinct")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 使用distinct算子
        // 对于RDD进行数据的去重

        // uv 统计案例
        // uv：user view，每天每个用户可能对网站会点击多次
        // 此时，需要对用户进行去重，然后统计出每天有多少个用户访问了网站
        // 而不是所有用户访问了网站多少次（pv）

        List<String> accessLogs = Arrays.asList(
            "user1 2016-01-01 23:58:42",
            "user1 2016-01-01 23:58:43",
            "user1 2016-01-01 23:58:44",
            "user2 2016-01-01 12:58:42",
            "user2 2016-01-01 12:58:46",
            "user3 2016-01-01 12:58:42",
            "user4 2016-01-01 12:58:42",
            "user5 2016-01-01 12:58:42",
            "user6 2016-01-01 12:58:42",
            "user6 2016-01-01 12:58:45");
        JavaRDD<String> accessLogRDD = sc.parallelize(accessLogs);

        JavaRDD<String> userIdsRDD = accessLogRDD.map(new Function<String, String>() {
            private static final long serialVersionUID = -4927280390342819450L;
            @Override
            public String call(String v1) throws Exception {
                String userId = v1.split(" ")[0];
                return userId;
            }
        });

        JavaRDD<String> distinctUserIdsRDD = userIdsRDD.distinct();
        int uv = distinctUserIdsRDD.collect().size();
        System.out.println(uv);
    }

}
