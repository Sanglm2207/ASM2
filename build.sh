#!/bin/bash

set -e

echo "====================================="
echo " ASM2 Pipeline - Rebuild Docker Image"
echo "====================================="

docker compose down
docker compose build
docker compose up -d

echo "Done."