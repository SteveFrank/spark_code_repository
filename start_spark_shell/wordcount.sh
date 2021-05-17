/home/hadoop/App/Spark/spark-2.3.3-bin-hadoop2.6/bin/spark-submit \
--executor-memory 1G \
--driver-memory 1G \
--executor-cores 2 \
--class com.spark.study.core.WordCountCluster \
/home/hadoop/App/Spark/run_jar/spark_java-1.0.0-SNAPSHOT-jar-with-dependencies.jar \
