#!/bin/bash

set -e

echo "====================================="
echo " ASM2 - Run CeleryExecutor"
echo "====================================="

if [ ! -f ".env" ]; then
  echo ".env not found. Creating from .env.example..."
  cp .env.example .env
fi

if [ ! -f "docker-compose.celery.yml" ]; then
  echo "docker-compose.celery.yml not found."
  exit 1
fi

docker compose down

docker compose -f docker-compose.celery.yml up -d

echo "====================================="
echo " CeleryExecutor environment started"
echo "====================================="
echo "Airflow UI : http://localhost:8080"
echo "Username   : admin"
echo "Password   : admin"
echo ""
echo "Check executor:"
echo "docker exec -it asm2-airflow-webserver airflow config get-value core executor"
echo ""
echo "Check worker:"
echo "docker logs -f asm2-airflow-worker"