#!/bin/bash

set -e

echo "====================================="
echo " ASM2 - Test Spark Job"
echo "====================================="

docker exec -it asm2-airflow-scheduler bash -c '
export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-17-openjdk-arm64}
export PATH=$JAVA_HOME/bin:$PATH

echo "JAVA_HOME=$JAVA_HOME"
echo "Testing spark-submit..."
spark-submit --version

echo "Running Spark job..."
spark-submit \
  --master local[*] \
  --packages org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 \
  --conf spark.mongodb.read.connection.uri=mongodb://mongo:27017 \
  --conf spark.mongodb.write.connection.uri=mongodb://mongo:27017 \
  /opt/airflow/spark/jobs/process_stackoverflow.py
'

echo ""
echo "Checking Spark output on host..."
ls -lah data/output/question_answer_count || true

echo ""
echo "Preview output:"
head -n 20 data/output/question_answer_count/part-*.csv || true