#!/bin/bash

set -e

echo "====================================="
echo " ASM2 - Clean Rebuild"
echo "====================================="
echo "WARNING: This will rebuild images without cache."
echo "Volumes are kept unless you manually run docker compose down -v."
echo "====================================="

docker compose down

docker compose build --no-cache

docker compose up -d

echo "Clean rebuild completed."