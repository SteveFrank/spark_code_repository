/home/hadoop/app/spark/spark-2.4.4-bin-hadoop2.6/bin/spark-submit \
--master yarn \
--num-executors 4 \
--executor-memory 512m \
--total-executor-cores 4 \
--class com.spark.study.core.WordCountCluster \
/home/hadoop/app/spark/spark_jar/spark_java-1.0.0-SNAPSHOT-jar-with-dependencies.jar \
