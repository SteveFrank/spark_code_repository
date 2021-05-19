package com.spark.streaming.study;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * Spark Streaming 与 Spark SQL 整合使用
 *
 * 每隔10秒，统计最近60秒的，每个种类的每个商品的点击次数，然后统计出每个种类top3热门的商品。
 *
 * @author yangqian
 * @date 2021/5/19
 */
public class Top3HotProduct {

    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("Top3HotProduct");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));

        // 首先看一下，输入日志的格式
        // 姓名、商品、品类信息
        // leo iphone mobile_phone
        // frank iphone mobile_phone

        // 为什么基于socket,因为方便,后续可以直接切换为kafka数据源（无缝切换）
        // 获取输入数据流
        JavaReceiverInputDStream<String> productClickLogsDStream = jssc.socketTextStream("localhost", 9999);

        // 然后，应该是做一个映射，将每个种类的每个商品，映射为(category_product, 1)的这种格式
        // 从而在后面可以使用window操作，对窗口中的这种格式的数据，进行reduceByKey操作
        // 从而统计出来，一个窗口中的每个种类的每个商品的，点击次数
        JavaPairDStream<String, Integer> categoryProductPairDStream
                = productClickLogsDStream.mapToPair(new PairFunction<String, String, Integer>() {
            private static final long serialVersionUID = -7547361036773413995L;
            @Override
            public Tuple2<String, Integer> call(String clickLog) throws Exception {
                String[] productClickLogSplited = clickLog.split(" ");
                return new Tuple2<String, Integer>(productClickLogSplited[2] + "_" + productClickLogSplited[1], 1);
            }
        });

        // 执行window操作
        JavaPairDStream<String, Integer> categoryProductCountsDStream = categoryProductPairDStream.reduceByKeyAndWindow(
                new Function2<Integer, Integer, Integer>() {
                    private static final long serialVersionUID = -3396765675694386273L;

                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }
                }, Durations.seconds(60), Durations.seconds(10)
        );

        // 然后针对60秒内的每个种类的每个商品的点击次数
        // foreachRDD，在内部，使用Spark SQL执行top3热门商品的统计
        categoryProductCountsDStream.foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {
            private static final long serialVersionUID = 3345656418784203585L;

            @Override
            public void call(JavaPairRDD<String, Integer> pairRDD) throws Exception {
                // 1、转换为row
                JavaRDD<Row> categoryProductCountRowRDD = pairRDD.map(new Function<Tuple2<String, Integer>, Row>() {
                    private static final long serialVersionUID = -2835565232709582472L;
                    @Override
                    public Row call(Tuple2<String, Integer> categoryProductCount) throws Exception {
                        String category = categoryProductCount._1.split("_")[0];
                        String product = categoryProductCount._1.split("_")[1];
                        Integer click_count = categoryProductCount._2;
                        return RowFactory.create(category, product, click_count);
                    }
                });
                // 进行DataFrame的转换
                List<StructField> structFields = new ArrayList<StructField>();
                structFields.add(DataTypes.createStructField("category", DataTypes.StringType, true));
                structFields.add(DataTypes.createStructField("product", DataTypes.StringType, true));
                structFields.add(DataTypes.createStructField("click_count", DataTypes.IntegerType, true));
                StructType structType = DataTypes.createStructType(structFields);

                SQLContext sqlContext = new SQLContext(categoryProductCountRowRDD.context());

                Dataset<Row> categoryProductCountDF = sqlContext.createDataFrame(categoryProductCountRowRDD, structType);

                // 将60s内的数据转换为点击数据的临时表
                categoryProductCountDF.createOrReplaceTempView("product_click_log");

                // 执行SQL语句，针对上述的临时表，统计出每个种类下的，点击次数前3的商品
                Dataset<Row> top3ProductDF = sqlContext.sql("" +
                        "SELECT category, product, click_count from (" +
                        "   SELECT " +
                        "       category, " +
                        "       product, " +
                        "       click_count, " +
                        "       row_number() OVER (partition by category order by click_count desc) rank" +
                        "   FROM product_click_log" +
                        ") tmp where rank <= 3");
                top3ProductDF.printSchema();
                top3ProductDF.show();
            }
        });

        jssc.start();
        jssc.awaitTermination();
        jssc.close();

    }

}
