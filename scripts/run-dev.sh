#!/bin/bash

set -e

echo "====================================="
echo " ASM2 - Run Dev Environment"
echo "====================================="

if [ ! -f ".env" ]; then
  echo ".env not found. Creating from .env.example..."
  cp .env.example .env
fi

mkdir -p dags spark/jobs scripts data/raw data/output logs plugins config

docker compose up -d

echo "====================================="
echo " Services are starting..."
echo "====================================="
echo "Airflow UI : http://localhost:8080"
echo "Username   : admin"
echo "Password   : admin"
echo "MongoDB    : mongodb://localhost:27017"
echo ""
echo "Next steps:"
echo "./scripts/check.sh"
echo "./scripts/setup-spark-connection.sh"