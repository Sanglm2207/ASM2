#!/bin/bash

set -e

echo "====================================="
echo " ASM2 - Setup Spark Connection"
echo "====================================="

echo "Deleting old spark_default connection if exists..."
docker exec -it asm2-airflow-webserver airflow connections delete spark_default || true

echo "Creating spark_default connection..."
docker exec -it asm2-airflow-webserver airflow connections add spark_default \
  --conn-type spark \
  --conn-host 'local[*]' \
  --conn-extra '{"deploy-mode":"client"}'

echo ""
echo "Current spark_default connection:"
docker exec -it asm2-airflow-webserver airflow connections get spark_default

echo "Done."