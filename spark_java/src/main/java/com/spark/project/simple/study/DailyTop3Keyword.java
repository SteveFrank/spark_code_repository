package com.spark.project.simple.study;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.*;

/**
 * 数据文件 /resources/hive_data/keyword.txt
 *
 * 数据格式：
 * 日期 用户 搜索词 城市 平台 版本
 *
 * 需求：
 * 1、筛选出符合查询条件（城市、平台、版本）的数据(从其他数据源读取数据)
 * 2、统计出每天搜索uv排名前3的搜索词
 * 3、按照每天的top3搜索词的uv搜索总次数，倒序排序
 * 4、将数据保存到hive表中
 *
 * @author yangqian
 * @date 2021/5/13
 */
public class DailyTop3Keyword {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("DailyTop3Keyword");
        JavaSparkContext sc = new JavaSparkContext(conf);

        SQLContext sqlContext = SparkSession.builder()
                .appName("DailyTop3Keyword")
                .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
                .config("spark.sql.codegen", "false")
                .config("spark.sql.inMemoryColumnarStorage.compressed", "false")
                .config("spark.sql.inMemoryColumnarStorage.batchSize", "100")
                .enableHiveSupport()
                .getOrCreate().sqlContext();

        // 伪造一份从其他数据原来的查询数据
        Map<String, List<String>> queryParamMap = new HashMap<String, List<String>>(3);
        queryParamMap.put("city", Collections.singletonList("beijing"));
        queryParamMap.put("platform", Collections.singletonList("android"));
        queryParamMap.put("version", Arrays.asList("1.0", "1.2", "1.5", "2.0"));

        // 将查询到的数据设置为广播变量
        // 根据我们实现思路中的分析，这里最合适的方式，是将该查询参数Map封装为一个Broadcast广播变量
        // 这样可以进行优化，每个Worker节点，就拷贝一份数据即可
        final Broadcast<Map<String, List<String>>> queryParamMapBroadcast = sc.broadcast(queryParamMap);

        // 读取文件为RDD
        JavaRDD<String> rowsRDD
                = sc.textFile("hdfs://master:9000/spark_study/keywords.txt");
        // 根据从其他数据源读取到的数据进行过滤
        JavaRDD<String> filterRDD = rowsRDD.filter(new Function<String, Boolean>() {
            private static final long serialVersionUID = 5708610315579933182L;
            @Override
            public Boolean call(String line) throws Exception {
                String[] keywordSplit = line.split("\t");
                String city = keywordSplit[3];
                String platform = keywordSplit[4];
                String version = keywordSplit[5];
                Map<String, List<String>> queryParamMap = queryParamMapBroadcast.value();

//                List<String> cities = queryParamMap.get("city");
//                if (cities.size() > 0 && !cities.contains(city)) {
//                    return false;
//                }
//                List<String> platforms = queryParamMap.get("platform");
//                if (platforms.size() > 0 && !platforms.contains(platform)) {
//                    return false;
//                }
//                List<String> versions = queryParamMap.get("version");
//                if (versions.size() > 0 && !versions.contains(version)) {
//                    return false;
//                }
                return true;
            }
        });
        // 将filterRDD进行转换，为 key 日期_keyword value username的RDD
        JavaPairRDD<String, String> dateKeywordUserRDD = filterRDD.mapToPair(new PairFunction<String, String, String>() {
            private static final long serialVersionUID = -2927698133777535956L;

            @Override
            public Tuple2<String, String> call(String line) throws Exception {
                String[] keywordSplit = line.split("\t");
                String date = keywordSplit[0];
                String username = keywordSplit[1];
                String keyword = keywordSplit[2];
                return new Tuple2<String, String>(date + "_" + keyword, username);
            }
        });
        // groupByKey 转换成 date_keyword 用户访问集合的形式便于去重
        JavaPairRDD<String, Long> distinctUserRDD = dateKeywordUserRDD.groupByKey().mapToPair(new PairFunction<Tuple2<String, Iterable<String>>, String, Long>() {
            private static final long serialVersionUID = 4962490187200422508L;

            @Override
            public Tuple2<String, Long> call(Tuple2<String, Iterable<String>> users) throws Exception {
                Iterator<String> usersIterator = users._2.iterator();
                Set<String> distinctUsers = new HashSet<String>();
                while (usersIterator.hasNext()) {
                    distinctUsers.add(usersIterator.next());
                }
                return new Tuple2<String, Long>(users._1, Long.parseLong(distinctUsers.size() + ""));
            }
        });
        // 将RDD转换为Row
        JavaRDD<Row> rows = distinctUserRDD.map(new Function<Tuple2<String, Long>, Row>() {
            private static final long serialVersionUID = 1422999467960319680L;

            @Override
            public Row call(Tuple2<String, Long> tuple) throws Exception {
                String date = tuple._1.split("_")[0];
                String keyword = tuple._1.split("_")[1];
                long count = tuple._2;
                return RowFactory.create(date, keyword, count);
            }
        });

        // 创建各个列的属性
        List<StructField> schema = new ArrayList<StructField>();
        schema.add(DataTypes.createStructField("date", DataTypes.StringType, true));
        schema.add(DataTypes.createStructField("keyword", DataTypes.StringType, true));
        schema.add(DataTypes.createStructField("count", DataTypes.LongType, true));

        StructType structType = DataTypes.createStructType(schema);

        // 转换为表
        Dataset<Row> rowDataset = sqlContext.createDataFrame(rows, structType);
        rowDataset.createOrReplaceTempView("daily_keyword_uv");

        rowDataset.printSchema();

        // 使用SQL查询
        Dataset<Row> dailyTop3KeywordDS = sqlContext.sql("" +
                "select date, keyword, count from (" +
                "   select " +
                "       date, keyword, count, " +
                "       row_number() over(PARTITION BY date ORDER BY count DESC) as rank" +
                "   from daily_keyword_uv" +
                ") tmp where rank <= 3");
        // 将查询结果转换为RDD
        JavaRDD<Row> dailyTop3KeywordRDD = dailyTop3KeywordDS.javaRDD();
        JavaPairRDD<String, String> top3DateKeywordUvRDD = dailyTop3KeywordRDD.mapToPair(new PairFunction<Row, String, String>() {
            private static final long serialVersionUID = -565792431384705990L;
            @Override
            public Tuple2<String, String> call(Row row) throws Exception {
                String date = row.getAs("date").toString();
                String keyword = row.getAs("keyword").toString();
                String count = row.getAs("count").toString();
                return new Tuple2<String, String>(date, keyword + "_" + count);
            }
        });
        // 按照每天的UV总量进行RDD的转换
        JavaPairRDD<Long, String> uvDateKeywordsRDD = top3DateKeywordUvRDD.groupByKey().mapToPair(new PairFunction<Tuple2<String, Iterable<String>>, Long, String>() {
            private static final long serialVersionUID = -8713805667266644520L;
            @Override
            public Tuple2<Long, String> call(Tuple2<String, Iterable<String>> tuple2) throws Exception {
                String date = tuple2._1;
                Long totalUv = 0L;
                String dateKeywords = date;
                Iterator<String> keywordUvIterator = tuple2._2.iterator();
                while(keywordUvIterator.hasNext()) {
                    String keywordUv = keywordUvIterator.next();
                    Long uv = Long.valueOf(keywordUv.split("_")[1]);
                    totalUv += uv;
                    dateKeywords += "," + keywordUv;
                }
                return new Tuple2<Long, String>(totalUv, dateKeywords);
            }
        });
        // 按照每天的UV总量进行RDD的倒序排列
        JavaPairRDD<Long, String> sortedUvDateKeywordsRDD = uvDateKeywordsRDD.sortByKey(false);

        // 转换为Row
        JavaRDD<Row> sortedRowRDD = sortedUvDateKeywordsRDD.flatMap(new FlatMapFunction<Tuple2<Long, String>, Row>() {
            private static final long serialVersionUID = 5180920757004616966L;

            @Override
            public Iterator<Row> call(Tuple2<Long, String> tuple) throws Exception {

                String dateKeywords = tuple._2;
                String[] dateKeywordsSplited = dateKeywords.split(",");

                String date = dateKeywordsSplited[0];
                List<Row> rows = new ArrayList<Row>();
                rows.add(RowFactory.create(date,
                        dateKeywordsSplited[1].split("_")[0],
                        Long.valueOf(dateKeywordsSplited[1].split("_")[1])));
                rows.add(RowFactory.create(date,
                        dateKeywordsSplited[2].split("_")[0],
                        Long.valueOf(dateKeywordsSplited[2].split("_")[1])));
                rows.add(RowFactory.create(date,
                        dateKeywordsSplited[3].split("_")[0],
                        Long.valueOf(dateKeywordsSplited[3].split("_")[1])));
                return rows.iterator();
            }
        });

        Dataset<Row> finalDS = sqlContext.createDataFrame(sortedRowRDD, structType);
        finalDS.printSchema();
        finalDS.show();
        sqlContext.sql("DROP TABLE IF EXISTS daily_top3_keyword_uv");
        finalDS.write().saveAsTable("daily_top3_keyword_uv");
        sc.close();

    }

}
