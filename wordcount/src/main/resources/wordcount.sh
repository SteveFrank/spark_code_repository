/home/hadoop/App/Spark/spark-2.3.3-bin-hadoop2.6/bin/spark-submit \
--executor-memory 512m \
--total-executor-cores 2 \
--class com.spark.study.core.WordCountCluster \
/home/hadoop/App/Spark/run_jar/wordcount-1.0.0-SNAPSHOT-jar-with-dependencies.jar \
