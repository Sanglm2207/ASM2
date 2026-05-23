# ASM2 - DEP303x: Data Pipeline cho dữ liệu lớn từ Cloud

## 1. Thông tin bài tập

**Môn học:** Dữ liệu lớn với Spark - DEP303x  
**Bài tập:** ASM2  
**Tên dự án:** Thiết lập Data Pipeline cho dữ liệu lớn từ Cloud

Dự án xây dựng một Data Pipeline hoàn chỉnh bằng **Apache Airflow**, **Apache Spark** và **MongoDB** để xử lý dữ liệu StackOverflow gồm hai file:

- `Questions.csv`
- `Answers.csv`

Pipeline thực hiện các bước chính:

1. Kiểm tra dữ liệu đã tồn tại hay chưa.
2. Xoá file cũ nếu cần chạy lại pipeline.
3. Tải file CSV từ Google Drive.
4. Import dữ liệu CSV vào MongoDB.
5. Dùng Spark xử lý dữ liệu.
6. Xuất kết quả ra CSV.
7. Import kết quả xử lý vào MongoDB.
8. Thiết lập các task chạy song song bằng `LocalExecutor`.
9. Cấu hình nâng cao `CeleryExecutor` với Redis và Worker.

---

## 2. Kiến trúc tổng quan

### 2.1. Kiến trúc LocalExecutor

Đây là kiến trúc chính dùng để chạy bài ASM2.

```text
+--------------------------+
|        Airflow UI        |
|  http://localhost:8080   |
+------------+-------------+
             |
             v
+--------------------------+
|   Airflow Webserver      |
+--------------------------+
             |
             v
+--------------------------+
|   Airflow Scheduler      |
|   Executor: LocalExecutor|
+------------+-------------+
             |
             +------------------+
             |                  |
             v                  v
+-------------------+   +-------------------+
|   MongoDB         |   |   Spark Local      |
|   Raw + Output DB |   |   spark-submit     |
+-------------------+   +-------------------+
             |
             v
+--------------------------+
|  PostgreSQL Metadata DB  |
|  Airflow metadata        |
+--------------------------+
```

### 2.2. Các service Docker

| Service | Vai trò |
|---|---|
| `asm2-postgres` | Metadata database cho Airflow |
| `asm2-mongo` | Lưu dữ liệu raw và dữ liệu output |
| `asm2-airflow-init` | Khởi tạo Airflow DB và user admin |
| `asm2-airflow-webserver` | Giao diện Airflow UI |
| `asm2-airflow-scheduler` | Điều phối và thực thi task với LocalExecutor |

---

## 3. Cấu trúc thư mục

```text
ASM2/
├── dags/
│   ├── asm2_stackoverflow_pipeline.py
│   └── utils/
│       ├── __init__.py
│       └── google_drive_downloader.py
├── spark/
│   └── jobs/
│       └── process_stackoverflow.py
├── scripts/
├── data/
│   ├── raw/
│   │   ├── Questions.csv
│   │   └── Answers.csv
│   └── output/
│       └── question_answer_count/
├── docker/
│   └── airflow/
│       └── Dockerfile
├── logs/
├── plugins/
├── .env
├── .env.example
├── .gitignore
├── docker-compose.yml
├── docker-compose.celery.yml
├── requirements.txt
├── environment.yml
├── run.sh
├── build.sh
├── restart-airflow.sh
└── README.md
```

---

## 4. Lưu ý về đường dẫn dữ liệu

Folder dữ liệu thật nằm trong project trên máy host:

```text
ASM2/data/raw
ASM2/data/output
```

Nhưng Airflow chạy bên trong Docker container, nên trong code DAG và Spark job phải dùng đường dẫn bên trong container:

```text
/opt/airflow/data/raw
/opt/airflow/data/output
```

Trong `docker-compose.yml`, folder `./data` được mount vào container:

```yaml
volumes:
  - ./data:/opt/airflow/data
```

Vì vậy:

| Trên máy host | Trong container Airflow |
|---|---|
| `ASM2/data/raw` | `/opt/airflow/data/raw` |
| `ASM2/data/output` | `/opt/airflow/data/output` |

---

## 5. Cấu hình môi trường

### 5.1. File `.env.example`

```env
AIRFLOW_UID=50000

AIRFLOW_ADMIN_USERNAME=admin
AIRFLOW_ADMIN_PASSWORD=admin
AIRFLOW_ADMIN_FIRSTNAME=Kai
AIRFLOW_ADMIN_LASTNAME=Dev
AIRFLOW_ADMIN_EMAIL=admin@example.com

POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow
POSTGRES_PORT=5433

MONGO_PORT=27017
MONGO_URI=mongodb://mongo:27017
MONGO_DATABASE=stackoverflow_asm2

DATA_RAW_PATH=/opt/airflow/data/raw
DATA_OUTPUT_PATH=/opt/airflow/data/output

QUESTIONS_FILE_ID=1kmrNUA8Uz9lKFcrkFyRCiReYuqFefxkO
ANSWERS_FILE_ID=1cJnA9LLfCgO-H7gvExj7xj7U-7rqSKZo
```

Tạo file `.env`:

```bash
cp .env.example .env
```

### 5.2. Google Drive File ID

Link Google Drive:

```text
Answers.csv:
https://drive.google.com/file/d/1cJnA9LLfCgO-H7gvExj7xj7U-7rqSKZo/view?usp=sharing

Questions.csv:
https://drive.google.com/file/d/1kmrNUA8Uz9lKFcrkFyRCiReYuqFefxkO/view?usp=sharing
```

File ID là phần nằm giữa `/d/` và `/view`:

```text
Answers.csv   -> 1cJnA9LLfCgO-H7gvExj7xj7U-7rqSKZo
Questions.csv -> 1kmrNUA8Uz9lKFcrkFyRCiReYuqFefxkO
```

Các file trên Google Drive cần để quyền:

```text
Anyone with the link can view
```

---

## 6. Cài đặt dependencies

### 6.1. File `requirements.txt`

```txt
apache-airflow-providers-apache-spark
pymongo
pandas
requests
python-dotenv
psycopg2-binary
```

Lưu ý: dự án sử dụng custom file:

```text
dags/utils/google_drive_downloader.py
```

nên không cần phụ thuộc vào package `googledrivedownloader` nữa.

### 6.2. File `environment.yml` cho local development

Conda không bắt buộc vì runtime chính là Docker. Tuy nhiên có thể dùng Conda để test script Python local.

```yaml
name: asm2-spark-pipeline
channels:
  - conda-forge
dependencies:
  - python=3.11
  - pandas
  - requests
  - pymongo
  - python-dotenv
  - pip
```

Tạo môi trường Conda:

```bash
conda env create -f environment.yml
conda activate asm2-spark-pipeline
```

---

## 7. Cách chạy project

### 7.1. Chạy nhanh bằng `run.sh`

File `run.sh` dùng cho quá trình development, không ép build lại Docker image.

```bash
chmod +x run.sh
./run.sh
```

Nội dung khuyến nghị của `run.sh`:

```bash
#!/bin/bash

set -e

echo "====================================="
echo " ASM2 Pipeline - Dev Start"
echo "====================================="

if [ ! -f ".env" ]; then
  echo ".env not found. Creating from .env.example..."
  cp .env.example .env
fi

mkdir -p dags spark/jobs scripts data/raw data/output logs plugins

docker compose up -d

echo "====================================="
echo " Airflow is starting..."
echo "====================================="
echo "Airflow UI : http://localhost:8080"
echo "Username   : admin"
echo "Password   : admin"
echo "MongoDB    : mongodb://localhost:27017"
echo "====================================="
```

Truy cập Airflow UI:

```text
http://localhost:8080
```

Tài khoản mặc định:

```text
Username: admin
Password: admin
```

### 7.2. Build lại Docker khi cần

Chỉ build lại khi sửa:

- `Dockerfile`
- `requirements.txt` và muốn đóng gói image chuẩn
- Java / Spark / MongoDB tools
- package hệ thống bằng `apt`
- base image Airflow

Lệnh build:

```bash
docker compose down
docker compose build
docker compose up -d
```

Nếu lỗi cache nặng mới dùng:

```bash
docker compose build --no-cache
```

### 7.3. File `build.sh`

```bash
#!/bin/bash

set -e

echo "====================================="
echo " ASM2 Pipeline - Rebuild Docker Image"
echo "====================================="

docker compose down
docker compose build
docker compose up -d

echo "Done."
```

Cấp quyền:

```bash
chmod +x build.sh
```

Chạy:

```bash
./build.sh
```

---

## 8. Workflow development nhanh

### 8.1. Sửa DAG

Khi sửa file:

```text
dags/asm2_stackoverflow_pipeline.py
```

Không cần build Docker.

Chỉ cần restart scheduler nếu muốn Airflow nhận DAG nhanh hơn:

```bash
docker compose restart airflow-scheduler
```

Check lỗi import DAG:

```bash
docker exec -it asm2-airflow-webserver airflow dags list-import-errors
```

Check DAG đã load chưa:

```bash
docker exec -it asm2-airflow-webserver airflow dags list | grep asm2
```

### 8.2. Sửa Spark job

Khi sửa file:

```text
spark/jobs/process_stackoverflow.py
```

Không cần build Docker vì folder `spark/` đã được mount vào container.

Chỉ cần trigger lại task `spark_process` hoặc trigger lại DAG.

### 8.3. Sửa downloader Google Drive

Khi sửa file:

```text
dags/utils/google_drive_downloader.py
```

Không cần build Docker.

Restart Airflow:

```bash
docker compose restart airflow-webserver airflow-scheduler
```

Test import downloader:

```bash
docker exec -it asm2-airflow-webserver python -c "from utils.google_drive_downloader import GoogleDriveDownloader as gdd; print('Downloader OK')"
```

### 8.4. Cài nóng package Python khi develop

Nếu thiếu package trong lúc dev, có thể cài nóng vào container:

```bash
docker exec -it asm2-airflow-webserver pip install <package_name>
docker exec -it asm2-airflow-scheduler pip install <package_name>
```

Sau đó nhớ thêm package vào `requirements.txt` để lần build sau không mất.

### 8.5. Restart Airflow nhanh

Tạo file `restart-airflow.sh`:

```bash
#!/bin/bash

docker compose restart airflow-webserver airflow-scheduler
docker exec -it asm2-airflow-webserver airflow dags list-import-errors
```

Cấp quyền:

```bash
chmod +x restart-airflow.sh
```

Chạy:

```bash
./restart-airflow.sh
```

---

## 9. Luồng chạy của DAG

DAG ID:

```text
asm2_stackoverflow_pipeline
```

Luồng tổng quát:

```text
start
  ↓
branching
  ├── end
  └── clear_file
        ↓
      download_answer_file_task      download_question_file_task
        ↓                            ↓
      import_answers_mongo           import_questions_mongo
        └──────────────┬─────────────┘
                       ↓
                 spark_process
                       ↓
               import_output_mongo
                       ↓
                      end
```

### 9.1. Ý nghĩa luồng rẽ nhánh

Task `branching` kiểm tra hai file:

```text
/opt/airflow/data/raw/Questions.csv
/opt/airflow/data/raw/Answers.csv
```

Nếu cả hai file đã tồn tại:

```text
start -> branching -> end
```

Nếu thiếu một trong hai file:

```text
start -> branching -> clear_file -> download -> import -> spark -> import output -> end
```

Muốn test full pipeline từ đầu, xoá file raw trước:

```bash
rm -f data/raw/Questions.csv
rm -f data/raw/Answers.csv
rm -rf data/output/question_answer_count
```

Sau đó trigger DAG trên Airflow UI.

---

## 10. Mô tả đầy đủ 9 yêu cầu

## Yêu cầu 1: Task `start` và `end`

Yêu cầu tạo hai task `start` và `end` bằng `DummyOperator` để biểu diễn điểm bắt đầu và kết thúc DAG.

Trong Airflow 2.x mới, `DummyOperator` đã được thay bằng `EmptyOperator`. Vì vậy code có xử lý tương thích:

```python
try:
    from airflow.operators.dummy import DummyOperator
except ImportError:
    from airflow.operators.empty import EmptyOperator as DummyOperator
```

Task:

```python
start = DummyOperator(task_id="start")

end = DummyOperator(
    task_id="end",
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
)
```

`trigger_rule=NONE_FAILED_MIN_ONE_SUCCESS` giúp task `end` chạy đúng khi DAG có nhiều nhánh và một số nhánh bị skip bởi `BranchPythonOperator`.

---

## Yêu cầu 2: Task `branching`

Task `branching` dùng `BranchPythonOperator` để kiểm tra dataset đã được tải xuống chưa.

Logic:

- Nếu có đủ `Questions.csv` và `Answers.csv` thì đi thẳng đến `end`.
- Nếu thiếu file thì chuyển đến `clear_file`.

```python
def check_dataset_files() -> str:
    questions_exists = QUESTIONS_FILE.exists()
    answers_exists = ANSWERS_FILE.exists()

    if questions_exists and answers_exists:
        return "end"

    return "clear_file"
```

Task:

```python
branching = BranchPythonOperator(
    task_id="branching",
    python_callable=check_dataset_files,
)
```

---

## Yêu cầu 3: Task `clear_file`

Task `clear_file` là task đầu tiên trong quá trình xử lý dữ liệu.

Mục đích:

- Xoá `Questions.csv` cũ.
- Xoá `Answers.csv` cũ.
- Xoá output Spark cũ.
- Tránh lỗi ghi đè hoặc dùng nhầm dữ liệu cũ.

Task sử dụng `BashOperator`:

```python
clear_file = BashOperator(
    task_id="clear_file",
    bash_command=f"""
    echo "Start clearing old dataset files..."

    mkdir -p "{DATA_RAW_PATH}"
    mkdir -p "{DATA_OUTPUT_PATH}"

    rm -f "{QUESTIONS_FILE}"
    rm -f "{ANSWERS_FILE}"

    rm -rf "{SPARK_OUTPUT_DIR}"

    echo "Old input files and Spark output folder have been removed."
    """,
)
```

---

## Yêu cầu 4: Task `download_question_file_task` và `download_answer_file_task`

Hai task này tải file CSV từ Google Drive về folder `data/raw`.

Dự án sử dụng custom downloader:

```text
dags/utils/google_drive_downloader.py
```

Lý do dùng custom downloader:

- Package `googledrivedownloader` có thể lỗi với Google Drive Virus Scan warning.
- File lớn trên Google Drive có thể trả về HTML confirmation page thay vì CSV thật.
- Custom downloader xử lý confirm token, warning page và validate file sau khi tải.

Task download dùng `PythonOperator`:

```python
download_question_file_task = PythonOperator(
    task_id="download_question_file_task",
    python_callable=download_question_file,
)

download_answer_file_task = PythonOperator(
    task_id="download_answer_file_task",
    python_callable=download_answer_file,
)
```

Function download:

```python
def download_question_file() -> None:
    DATA_RAW_PATH.mkdir(parents=True, exist_ok=True)

    gdd.download_file_from_google_drive(
        file_id=QUESTIONS_FILE_ID,
        dest_path=str(QUESTIONS_FILE),
        unzip=False,
        overwrite=True,
        showsize=True,
    )
```

```python
def download_answer_file() -> None:
    DATA_RAW_PATH.mkdir(parents=True, exist_ok=True)

    gdd.download_file_from_google_drive(
        file_id=ANSWERS_FILE_ID,
        dest_path=str(ANSWERS_FILE),
        unzip=False,
        overwrite=True,
        showsize=True,
    )
```

Sau khi tải xong, kiểm tra file:

```bash
head -n 3 data/raw/Questions.csv
head -n 3 data/raw/Answers.csv
```

File đúng phải có header CSV, không phải HTML.

---

## Yêu cầu 5: Task `import_questions_mongo` và `import_answers_mongo`

Sau khi tải file CSV, dữ liệu được import vào MongoDB bằng `mongoimport`.

Cú pháp yêu cầu:

```bash
mongoimport --type csv -d <database> -c <collection> --headerline --drop <file>
```

Task import Questions:

```python
import_questions_mongo = BashOperator(
    task_id="import_questions_mongo",
    bash_command=f"""
    mongoimport \
      --uri="{MONGO_URI}" \
      --type csv \
      -d "{MONGO_DATABASE}" \
      -c questions \
      --headerline \
      --drop \
      "{QUESTIONS_FILE}"
    """,
)
```

Task import Answers:

```python
import_answers_mongo = BashOperator(
    task_id="import_answers_mongo",
    bash_command=f"""
    mongoimport \
      --uri="{MONGO_URI}" \
      --type csv \
      -d "{MONGO_DATABASE}" \
      -c answers \
      --headerline \
      --drop \
      "{ANSWERS_FILE}"
    """,
)
```

Kiểm tra MongoDB:

```bash
docker exec -it asm2-mongo mongosh
```

Trong Mongo shell:

```javascript
use stackoverflow_asm2
show collections

db.questions.countDocuments()
db.answers.countDocuments()
```

---

## Yêu cầu 6: Task `spark_process`

Task `spark_process` dùng `SparkSubmitOperator` để submit Spark job.

Yêu cầu xử lý:

- Dựa vào tập `Answers.csv` hoặc collection `answers`.
- Mỗi câu trả lời có `ParentId` là ID của câu hỏi.
- Đếm mỗi câu hỏi có bao nhiêu câu trả lời.

Output:

```text
+----+-----------------+
| Id | Number of answers |
+----+-----------------+
```

Spark job nằm tại:

```text
spark/jobs/process_stackoverflow.py
```

Logic chính:

```python
result_df = (
    answers_df
    .filter(col("ParentId").isNotNull())
    .groupBy(col("ParentId").alias("Id"))
    .agg(count("*").alias("Number of answers"))
    .orderBy(col("Id").cast("int"))
)
```

Ghi output CSV:

```python
(
    result_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", True)
    .csv(str(OUTPUT_DIR))
)
```

Task Airflow:

```python
spark_process = SparkSubmitOperator(
    task_id="spark_process",
    application=SPARK_JOB_FILE,
    conn_id="spark_default",
    packages="org.mongodb.spark:mongo-spark-connector_2.12:10.3.0",
    conf={
        "spark.mongodb.read.connection.uri": MONGO_URI,
        "spark.mongodb.write.connection.uri": MONGO_URI,
    },
    env_vars={
        "JAVA_HOME": "/usr/lib/jvm/java-17-openjdk-arm64",
        "MONGO_URI": MONGO_URI,
        "MONGO_DATABASE": MONGO_DATABASE,
        "DATA_RAW_PATH": str(DATA_RAW_PATH),
        "DATA_OUTPUT_PATH": str(DATA_OUTPUT_PATH),
    },
    verbose=True,
)
```

### Test riêng yêu cầu 6

Tạo Spark connection:

```bash
docker exec -it asm2-airflow-webserver airflow connections delete spark_default

docker exec -it asm2-airflow-webserver airflow connections add spark_default \
  --conn-type spark \
  --conn-host 'local[*]' \
  --conn-extra '{"deploy-mode":"client"}'
```

Test `spark-submit`:

```bash
docker exec -it asm2-airflow-scheduler bash -c '
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
export PATH=$JAVA_HOME/bin:$PATH

spark-submit \
  --master local[*] \
  --packages org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 \
  --conf spark.mongodb.read.connection.uri=mongodb://mongo:27017 \
  --conf spark.mongodb.write.connection.uri=mongodb://mongo:27017 \
  /opt/airflow/spark/jobs/process_stackoverflow.py
'
```

Check output:

```bash
ls -lah data/output/question_answer_count
head -n 20 data/output/question_answer_count/part-*.csv
```

---

## Yêu cầu 7: Task `import_output_mongo`

Sau khi Spark ghi output ra CSV, task này import file output vào MongoDB collection `answer_counts`.

Spark output là một folder gồm:

```text
part-00000-xxxx.csv
_SUCCESS
```

Vì vậy cần tìm file `part-*.csv` trước khi import.

```python
import_output_mongo = BashOperator(
    task_id="import_output_mongo",
    bash_command=f"""
    OUTPUT_FILE=$(find "{SPARK_OUTPUT_DIR}" -name "part-*.csv" | head -n 1)

    if [ -z "$OUTPUT_FILE" ]; then
      echo "No Spark output CSV file found in {SPARK_OUTPUT_DIR}"
      exit 1
    fi

    mongoimport \
      --uri="{MONGO_URI}" \
      --type csv \
      -d "{MONGO_DATABASE}" \
      -c answer_counts \
      --headerline \
      --drop \
      "$OUTPUT_FILE"
    """,
)
```

Kiểm tra MongoDB:

```javascript
use stackoverflow_asm2

db.answer_counts.find().limit(20)
db.answer_counts.countDocuments()
```

---

## Yêu cầu 8: Sắp xếp thứ tự task và chạy song song bằng LocalExecutor

DAG được sắp xếp đúng theo flow đề bài:

```python
start >> branching

branching >> end
branching >> clear_file

clear_file >> [
    download_answer_file_task,
    download_question_file_task,
]

download_answer_file_task >> import_answers_mongo
download_question_file_task >> import_questions_mongo

[
    import_answers_mongo,
    import_questions_mongo,
] >> spark_process

spark_process >> import_output_mongo
import_output_mongo >> end
```

Các task chạy song song:

```text
download_question_file_task và download_answer_file_task
import_questions_mongo và import_answers_mongo
```

Airflow được cấu hình dùng `LocalExecutor`:

```yaml
environment:
  AIRFLOW__CORE__EXECUTOR: LocalExecutor
```

Check executor:

```bash
docker exec -it asm2-airflow-webserver airflow config get-value core executor
```

Kết quả đúng:

```text
LocalExecutor
```

---

## Yêu cầu 9: Cài đặt CeleryExecutor cho Airflow

Yêu cầu nâng cao cấu hình Airflow chạy với `CeleryExecutor`.

Kiến trúc CeleryExecutor:

```text
+-------------------+
| Airflow Webserver |
+-------------------+
          |
          v
+-------------------+       +----------------+
| Airflow Scheduler | ----> | Redis Broker   |
+-------------------+       +----------------+
                                  |
                                  v
                           +---------------+
                           | Airflow Worker|
                           +---------------+
                                  |
                                  v
                           +---------------+
                           | Execute Tasks |
                           +---------------+
```

Các service bổ sung:

| Service | Vai trò |
|---|---|
| `asm2-redis` | Message broker cho Celery |
| `asm2-airflow-worker` | Worker nhận task từ Redis và thực thi |

File cấu hình riêng:

```text
docker-compose.celery.yml
```

Các biến môi trường chính:

```yaml
AIRFLOW__CORE__EXECUTOR: CeleryExecutor
AIRFLOW__CELERY__BROKER_URL: redis://redis:6379/0
AIRFLOW__CELERY__RESULT_BACKEND: db+postgresql://airflow:airflow@postgres:5432/airflow
```

Chạy CeleryExecutor:

```bash
docker compose down

docker compose -f docker-compose.celery.yml up -d --build
```

Nếu image đã build rồi:

```bash
docker compose -f docker-compose.celery.yml up -d
```

Check executor:

```bash
docker exec -it asm2-airflow-webserver airflow config get-value core executor
```

Kết quả đúng:

```text
CeleryExecutor
```

Check worker:

```bash
docker compose -f docker-compose.celery.yml ps
```

Check log worker:

```bash
docker logs -f asm2-airflow-worker
```

Nếu thấy worker `ready` thì CeleryExecutor đã hoạt động.

---

## 11. Các lệnh kiểm tra nhanh

### Check container

```bash
docker compose ps
```

### Check log webserver

```bash
docker logs --tail=100 asm2-airflow-webserver
```

### Check log scheduler

```bash
docker logs --tail=100 asm2-airflow-scheduler
```

### Check import error DAG

```bash
docker exec -it asm2-airflow-webserver airflow dags list-import-errors
```

### Check DAG list

```bash
docker exec -it asm2-airflow-webserver airflow dags list | grep asm2
```

### Check MongoDB tools

```bash
docker exec -it asm2-airflow-webserver mongoimport --version
```

### Check Java

```bash
docker exec -it asm2-airflow-webserver java -version
```

### Check Spark

```bash
docker exec -it asm2-airflow-webserver spark-submit --version
```

### Check Spark connection

```bash
docker exec -it asm2-airflow-webserver airflow connections get spark_default
```

### Tạo lại Spark connection

```bash
docker exec -it asm2-airflow-webserver airflow connections delete spark_default

docker exec -it asm2-airflow-webserver airflow connections add spark_default \
  --conn-type spark \
  --conn-host 'local[*]' \
  --conn-extra '{"deploy-mode":"client"}'
```

---

## 12. Cách test full pipeline

Xoá file raw để ép DAG chạy toàn bộ pipeline:

```bash
rm -f data/raw/Questions.csv
rm -f data/raw/Answers.csv
rm -rf data/output/question_answer_count
```

Trigger DAG trong Airflow UI:

```text
DAGs -> asm2_stackoverflow_pipeline -> Trigger DAG
```

Kết quả mong muốn:

```text
start                      success
branching                  success
clear_file                 success
download_question_file_task success
download_answer_file_task  success
import_questions_mongo     success
import_answers_mongo       success
spark_process              success
import_output_mongo        success
end                        success
```

Check output file:

```bash
ls -lah data/output/question_answer_count
head -n 20 data/output/question_answer_count/part-*.csv
```

Check MongoDB:

```bash
docker exec -it asm2-mongo mongosh
```

Trong Mongo shell:

```javascript
use stackoverflow_asm2

show collections

db.questions.countDocuments()
db.answers.countDocuments()
db.answer_counts.countDocuments()
db.answer_counts.find().limit(20)
```

Kết quả mong muốn có 3 collection:

```text
questions
answers
answer_counts
```

---

## 13. Troubleshooting

### 13.1. DAG missing from DagBag

Check lỗi:

```bash
docker exec -it asm2-airflow-webserver airflow dags list-import-errors
```

Nguyên nhân thường gặp:

- Thiếu package Python.
- Sai import module.
- File DAG lỗi syntax.
- File chưa mount vào container.

Check file DAG trong container:

```bash
docker exec -it asm2-airflow-webserver ls -lah /opt/airflow/dags
```

### 13.2. Google Drive tải về HTML thay vì CSV

Nếu log báo:

```text
Downloaded file looks like an HTML page, not a real CSV file
```

Nguyên nhân:

- Google Drive trả warning page.
- File chưa share public.
- Quota exceeded.
- Need access.

Cách xử lý:

```bash
rm -f data/raw/Questions.csv
rm -f data/raw/Answers.csv
```

Check lại quyền Google Drive:

```text
Anyone with the link can view
```

Test downloader:

```bash
docker exec -it asm2-airflow-webserver python -c "from utils.google_drive_downloader import GoogleDriveDownloader as gdd; print('Downloader OK')"
```

### 13.3. SparkSubmitOperator fallback sang YARN

Nếu log có:

```text
Could not load connection string spark_default, defaulting to yarn
spark-submit --master yarn
```

Tạo Spark connection:

```bash
docker exec -it asm2-airflow-webserver airflow connections add spark_default \
  --conn-type spark \
  --conn-host 'local[*]' \
  --conn-extra '{"deploy-mode":"client"}'
```

Log đúng phải là:

```text
spark-submit --master local[*]
```

### 13.4. Spark lỗi JAVA_HOME amd64 trên Mac M1/M2/M3

Nếu log có:

```text
/usr/lib/jvm/java-17-openjdk-amd64/bin/java: No such file or directory
```

Set trong `docker-compose.yml`:

```yaml
environment:
  JAVA_HOME: /usr/lib/jvm/java-17-openjdk-arm64
```

Hoặc trong `SparkSubmitOperator`:

```python
env_vars={
    "JAVA_HOME": "/usr/lib/jvm/java-17-openjdk-arm64",
}
```

Recreate container, không cần build:

```bash
docker compose up -d --force-recreate airflow-webserver airflow-scheduler
```

### 13.5. Task bị skipped

Nếu DAG chỉ chạy:

```text
start -> branching -> end
```

và các task còn lại bị skipped, điều này đúng nếu hai file đã tồn tại:

```text
data/raw/Questions.csv
data/raw/Answers.csv
```

Muốn chạy full pipeline:

```bash
rm -f data/raw/Questions.csv
rm -f data/raw/Answers.csv
rm -rf data/output/question_answer_count
```

Sau đó trigger DAG lại.

---

## 14. Ghi chú về Dev, Staging, Production

### Development

Trong giai đoạn develop:

- Không build lại Docker khi chỉ sửa DAG/Spark job/script.
- Dùng volume mount để code cập nhật ngay vào container.
- Có thể cài nóng package để test nhanh.
- Restart scheduler/webserver khi cần.

### Staging / Production

Trong staging hoặc production:

- Không cài nóng package trong container.
- Build image từ `Dockerfile` + `requirements.txt`.
- Tag image theo version.
- Deploy image cố định.

Ví dụ:

```bash
docker build -t asm2-airflow:1.0.0 -f docker/airflow/Dockerfile .
```

---

## 15. Kết luận

Dự án đã triển khai đầy đủ 9 yêu cầu của ASM2:

| Yêu cầu | Nội dung | Trạng thái |
|---|---|---|
| 1 | Tạo task `start` và `end` | Hoàn thành |
| 2 | Tạo task `branching` bằng `BranchPythonOperator` | Hoàn thành |
| 3 | Tạo task `clear_file` bằng `BashOperator` | Hoàn thành |
| 4 | Tải `Questions.csv` và `Answers.csv` từ Google Drive | Hoàn thành |
| 5 | Import CSV vào MongoDB bằng `mongoimport` | Hoàn thành |
| 6 | Xử lý dữ liệu bằng Spark | Hoàn thành |
| 7 | Import output Spark vào MongoDB | Hoàn thành |
| 8 | Sắp xếp task và chạy song song bằng `LocalExecutor` | Hoàn thành |
| 9 | Cấu hình nâng cao `CeleryExecutor` | Hoàn thành |

Pipeline đáp ứng đầy đủ yêu cầu xử lý dữ liệu lớn từ Cloud bằng Airflow, MongoDB và Spark.
