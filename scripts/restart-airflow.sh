#!/bin/bash

set -e

echo "====================================="
echo " ASM2 - Restart Airflow"
echo "====================================="

echo "Stopping Airflow services..."
docker compose stop airflow-webserver airflow-scheduler || true

echo "Removing stale webserver PID file..."
docker compose run --rm airflow-webserver bash -c 'rm -f /opt/airflow/airflow-webserver.pid' || true

echo "Starting Airflow services..."
docker compose up -d airflow-webserver airflow-scheduler

echo "Waiting for Airflow to reload..."
sleep 20

echo ""
echo "Container status:"
docker compose ps

echo ""
echo "Checking webserver:"
curl -I http://localhost:8080 || true

echo ""
echo "Checking DAG import errors..."
docker exec -it asm2-airflow-webserver airflow dags list-import-errors || true

echo ""
echo "Checking ASM2 DAG..."
docker exec -it asm2-airflow-webserver airflow dags list | grep asm2 || true

echo "Done."