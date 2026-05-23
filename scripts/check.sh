#!/bin/bash

set -e

echo "====================================="
echo " ASM2 - Check Environment"
echo "====================================="

echo ""
echo "1. Docker Compose status:"
docker compose ps

echo ""
echo "2. Airflow executor:"
docker exec -it asm2-airflow-webserver airflow config get-value core executor || true

echo ""
echo "3. DAG import errors:"
docker exec -it asm2-airflow-webserver airflow dags list-import-errors || true

echo ""
echo "4. DAG list:"
docker exec -it asm2-airflow-webserver airflow dags list | grep asm2 || true

echo ""
echo "5. Java version:"
docker exec -it asm2-airflow-scheduler java -version || true

echo ""
echo "6. Spark version:"
docker exec -it asm2-airflow-scheduler spark-submit --version || true

echo ""
echo "7. MongoImport version:"
docker exec -it asm2-airflow-scheduler mongoimport --version || true

echo ""
echo "8. Spark connection:"
docker exec -it asm2-airflow-webserver airflow connections get spark_default || true

echo ""
echo "Check completed."