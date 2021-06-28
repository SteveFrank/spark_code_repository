package com.spark.streaming.study.review;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
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
 * @date 2021/6/9
 */
public class Top3HotProductReview {

    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("Top3HotProductReview");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));

        // 首先看一下，输入日志的格式
        // 姓名、商品、品类信息
        // leo iphone mobile_phone
        // frank iphone mobile_phone

        // 获取流
        JavaReceiverInputDStream<String> inputDStream = jssc.socketTextStream("localhost", 8888);
        // 转换流
        JavaPairDStream<String, Integer> pairDStream = inputDStream.mapToPair(clickLog -> {
            String[] clickData = clickLog.split(" ");
            return new Tuple2<String, Integer>(clickData[2] + "_" + clickData[1], 1);
        });
        // 累加，执行窗口操作计算出对应窗口时间内的总数数据
        // 每隔10秒，统计最近60秒的，每个种类的每个商品的点击次数，然后统计出每个种类top3热门的商品
        JavaPairDStream<String, Integer> categoryProductDStream = pairDStream.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = 3260769850464437127L;

            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        }, Durations.seconds(60), Durations.seconds(10));

        // foreach计算后进行操作
        categoryProductDStream.foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {
            private static final long serialVersionUID = 7891270627461357798L;

            @Override
            public void call(JavaPairRDD<String, Integer> categoryProductRDD) throws Exception {
                JavaRDD<Row> rowRDD = categoryProductRDD.map(new Function<Tuple2<String, Integer>, Row>() {
                    private static final long serialVersionUID = 7721685514800320291L;
                    @Override
                    public Row call(Tuple2<String, Integer> tuple2) throws Exception {
                        String categoryProduct = tuple2._1;
                        Integer count = tuple2._2;
                        String category = categoryProduct.split("_")[0];
                        String product = categoryProduct.split("_")[1];
                        return RowFactory.create(category, product, count);
                    }
                });
                // 进行转换
                List<StructField> structFields = new ArrayList<>(3);
                structFields.add(DataTypes.createStructField("category", DataTypes.StringType, true));
                structFields.add(DataTypes.createStructField("product", DataTypes.StringType, true));
                structFields.add(DataTypes.createStructField("click_count", DataTypes.IntegerType, true));
                StructType structType = DataTypes.createStructType(structFields);

                // 转换为表处理
                SQLContext sqlContext = new SQLContext(rowRDD.context());
                Dataset<Row> categoryProductCountDF = sqlContext.createDataFrame(rowRDD, structType);
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
