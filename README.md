## Environment Setup

This project uses Docker Compose as the main runtime environment.

Services included:

- Apache Airflow Webserver
- Apache Airflow Scheduler
- PostgreSQL for Airflow metadata database
- MongoDB for storing raw and processed data
- Apache Spark installed inside the Airflow image

Conda is optional and only used for local development or testing Python scripts outside Docker.

## Run with Docker

```bash
cp .env.example .env
./run.sh
```

Airflow UI:
```bash
http://localhost:8080
```

Default account:

username: admin
password: admin

Optional: Run local Python environment with Conda
```bash
conda env create -f environment.yml
conda activate asm2-spark-pipeline
```


---

