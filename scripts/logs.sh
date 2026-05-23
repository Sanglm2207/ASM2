#!/bin/bash

SERVICE=${1:-airflow-scheduler}

case "$SERVICE" in
  webserver)
    docker logs -f asm2-airflow-webserver
    ;;
  scheduler)
    docker logs -f asm2-airflow-scheduler
    ;;
  init)
    docker logs -f asm2-airflow-init
    ;;
  mongo)
    docker logs -f asm2-mongo
    ;;
  postgres)
    docker logs -f asm2-postgres
    ;;
  worker)
    docker logs -f asm2-airflow-worker
    ;;
  redis)
    docker logs -f asm2-redis
    ;;
  *)
    echo "Unknown service: $SERVICE"
    echo "Usage:"
    echo "./scripts/logs.sh webserver"
    echo "./scripts/logs.sh scheduler"
    echo "./scripts/logs.sh init"
    echo "./scripts/logs.sh mongo"
    echo "./scripts/logs.sh postgres"
    echo "./scripts/logs.sh worker"
    echo "./scripts/logs.sh redis"
    exit 1
    ;;
esac