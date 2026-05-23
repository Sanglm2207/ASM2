#!/bin/bash

docker compose restart airflow-webserver airflow-scheduler
docker exec -it asm2-airflow-webserver airflow dags list-import-errors