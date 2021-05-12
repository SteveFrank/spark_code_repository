package com.spark.sql.study.dataset;

import com.spark.sql.study.dataset.vo.Student;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import java.util.List;

/**
 *
 * 利用反射的方式
 *
 * @author yangqian
 * @date 2021/5/12
 */
public class RDD2DataSetReflection {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("RDD2DataSetReflection");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = SparkSession.builder().config(conf).getOrCreate().sqlContext();
        JavaRDD<String> lines = sc.textFile("/Users/qian/WorkSpaces/own-workspace/2021/spark_code_repository/spark_java/src/main/resources/spark_sql/students.txt");
        JavaRDD<Student> students = lines.map(new Function<String, Student>() {
            private static final long serialVersionUID = 3587019637184285751L;

            @Override
            public Student call(String v1) throws Exception {
                String[] studentInfo = v1.split(",");
                Student student = new Student();
                student.setId(Integer.parseInt(studentInfo[0]));
                student.setName(studentInfo[1]);
                student.setAge(Integer.parseInt(studentInfo[2]));
                return student;
            }
        });

        // 使用反射的方式转化为DataFrame
        // 将 Student.class 传入，其实就是使用反射的方式来创建 DataFrame
        // 因为Student.class本身就是反射的一个应用
        // 底层还是通过 student class 进行反射用于获取field
        // JavaBean必须实现Serializable接口，是可序列化的
        Dataset<Row> studentDF = sqlContext.createDataFrame(students, Student.class);
        // 注册为临时表
        studentDF.registerTempTable("students");
        // 针对students临时表执行sql语句
        Dataset<Row> teenagerDF = sqlContext.sql("select id, name, age from students where age >= 18");
        // 查询后转换为RDD
        JavaRDD<Row> teenagerRDD = teenagerDF.javaRDD();

        // 将RDD中的数据进行映射，成为Student
        JavaRDD<Student> teenagerStudentRDD = teenagerRDD.map(new Function<Row, Student>() {
            @Override
            public Student call(Row row) throws Exception {
                Student stu = new Student();
                stu.setId(row.getInt(0));
                stu.setName(row.getString(1));
                stu.setAge(row.getInt(2));
                return stu;
            }
        });

        // 将数据collect回来，打印出来
        List<Student> studentList = teenagerStudentRDD.collect();
        for (Student student : studentList) {
            System.out.println(student);
        }
    }


}