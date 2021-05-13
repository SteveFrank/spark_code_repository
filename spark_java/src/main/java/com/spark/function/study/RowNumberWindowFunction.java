package com.spark.function.study;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

/**
 *
 * 利用 row_number() 开窗函数统计TOP3的数据
 *
 * @author yangqian
 * @date 2021/5/13
 */
public class RowNumberWindowFunction {

    public static void main(String[] args) {
        SQLContext sqlContext = SparkSession.builder()
                .appName("RowNumberWindowFunction")
                .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
                .config("spark.sql.codegen", "false")
                .config("spark.sql.inMemoryColumnarStorage.compressed", "false")
                .config("spark.sql.inMemoryColumnarStorage.batchSize", "100")
                .enableHiveSupport()
                .getOrCreate()
                .sqlContext();

        // 创建销售额表，sales表
        sqlContext.sql("DROP TABLE IF EXISTS sales");
        sqlContext.sql("CREATE TABLE IF NOT EXISTS sales (product STRING,category STRING,revenue BIGINT)");
        sqlContext.sql("LOAD DATA LOCAL INPATH '/home/hadoop/App/Spark/spark_data/sales.txt' INTO TABLE sales");

        // 开始编写我们的统计逻辑，使用row_number()开窗函数
        // row_number() 开窗函数的作用
        // 主要就是给每个分组的数据，按照其排序的顺序，打上一个分组内的行号
        // 比如，有一个分组 date=20211001，其中有3条数据，1112，1121，1124
        // 那么对于这个分组的每一行使用row_number()开窗函数后，三行，一次会获得一个组内的行号
        // 行号从1开始递增，比如
        // 1122 1
        // 1121 2
        // 1124 3
        Dataset<Row> top3SaleDS = sqlContext.sql(""
                + "SELECT product, category, revenue FROM ("
                + " SELECT "
                + "     product, "
                + "     category, "
                + "     revenue, "
                + "     row_number() OVER (PARTITION BY category ORDER BY revenue DESC) as rank"
                + "     FROM sales"
                + ") temp_sales"
                + " WHERE rank <= 3");

        // 将每组数据的前3保存到一个表中
        sqlContext.sql("DROP TABLE IF EXISTS top3_sales");
        top3SaleDS.write().saveAsTable("top3_sales");
    }

}
