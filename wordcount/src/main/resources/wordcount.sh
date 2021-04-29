/home/hadoop/App/Spark/spark-2.3.3-bin-hadoop2.6/bin/spark-submit \
--master yarn \
--num-executors 2 \
--driver-memory 10m \
--executor-memory 20m \
--executor-cores 3 \
--class com.spark.study.core.WordCountCluster \
/home/hadoop/App/Spark/run_jar/wordcount-1.0.0-SNAPSHOT-jar-with-dependencies.jar \
