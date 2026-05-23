#!/bin/bash

set -e

echo "====================================="
echo " ASM2 - Build Docker Images"
echo "====================================="

if [ ! -f ".env" ]; then
  echo ".env not found. Creating from .env.example..."
  cp .env.example .env
fi

mkdir -p dags spark/jobs scripts data/raw data/output logs plugins config

docker compose down

docker compose build

echo "====================================="
echo " Build completed"
echo "====================================="
echo "Run project with:"
echo "./scripts/run-dev.sh"