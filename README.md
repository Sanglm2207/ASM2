# ASM2 - Data Pipeline cho dữ liệu lớn từ Cloud

**Môn học:** Dữ liệu lớn với Spark - DEP303x  
**Bài tập:** Assignment 02  
**Tên dự án:** Thiết lập Data Pipeline cho dữ liệu lớn từ Cloud  

Project này xây dựng một Data Pipeline sử dụng **Apache Airflow**, **Apache Spark** và **MongoDB** để tải dữ liệu StackOverflow từ Google Drive, import vào MongoDB, xử lý bằng Spark và lưu kết quả xử lý trở lại MongoDB.

---

## 1. Kiến trúc tổng quan

Pipeline gồm các thành phần chính:

| Thành phần | Vai trò |
|---|---|
| Airflow Webserver | Giao diện quản lý DAG tại `http://localhost:8080` |
| Airflow Scheduler | Điều phối và thực thi task |
| PostgreSQL | Metadata database cho Airflow |
| MongoDB | Lưu dữ liệu raw và dữ liệu sau xử lý |
| Spark | Xử lý dữ liệu lớn, tính số câu trả lời của từng câu hỏi |
| Redis | Message broker cho CeleryExecutor ở yêu cầu nâng cao |
| Airflow Worker | Worker xử lý task khi dùng CeleryExecutor |
| Flower | UI theo dõi worker Celery tại `http://localhost:5555` |

Luồng xử lý chính:

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

Hai nhóm task được thiết lập chạy song song:

```text
download_question_file_task  ||  download_answer_file_task
import_questions_mongo       ||  import_answers_mongo
```

---

## 2. Cấu trúc thư mục

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
├── config/
│   └── airflow.cfg
├── docker/
│   └── airflow/
│       └── Dockerfile
├── data/
│   ├── raw/
│   └── output/
├── logs/
├── plugins/
├── scripts/
│   ├── build.sh
│   ├── run-dev.sh
│   ├── stop.sh
│   ├── restart-airflow.sh
│   ├── check.sh
│   ├── setup-spark-connection.sh
│   ├── reset-data.sh
│   ├── test-spark.sh
│   ├── logs.sh
│   ├── run-celery.sh
│   ├── rebuild-clean.sh
│   └── export-output.sh
├── submission/
├── .env.example
├── .env
├── .gitignore
├── docker-compose.yml
├── docker-compose.celery.yml
├── requirements.txt
└── README.md
```

---

## 3. Dataset

Project sử dụng 2 file CSV:

### `Questions.csv`

| Cột | Ý nghĩa |
|---|---|
| Id | Id của câu hỏi |
| OwnerUserId | Id người tạo câu hỏi |
| CreationDate | Ngày câu hỏi được tạo |
| ClosedDate | Ngày câu hỏi kết thúc |
| Score | Điểm số câu hỏi |
| Title | Tiêu đề câu hỏi |
| Body | Nội dung câu hỏi |

### `Answers.csv`

| Cột | Ý nghĩa |
|---|---|
| Id | Id của câu trả lời |
| OwnerUserId | Id người tạo câu trả lời |
| CreationDate | Ngày câu trả lời được tạo |
| ParentId | Id câu hỏi mà câu trả lời thuộc về |
| Score | Điểm số câu trả lời |
| Body | Nội dung câu trả lời |

Google Drive file IDs:

```env
QUESTIONS_FILE_ID=1kmrNUA8Uz9lKFcrkFyRCiReYuqFefxkO
ANSWERS_FILE_ID=1cJnA9LLfCgO-H7gvExj7xj7U-7rqSKZo
```

---

## 4. File cấu hình môi trường

Tạo `.env` từ `.env.example`:

```bash
cp .env.example .env
```

Nội dung `.env` mẫu:

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

JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
```

Lưu ý với Mac M1/M2/M3:

```env
JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
```

Nếu chạy trên Intel/AMD64 Linux thì có thể dùng:

```env
JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
```

---

## 5. File `airflow.cfg`

Đề bài yêu cầu có file `airflow.cfg` để thiết lập LocalExecutor.

File nên đặt tại:

```text
config/airflow.cfg
```

Nội dung chính:

```ini
[core]
executor = LocalExecutor
load_examples = False
dags_folder = /opt/airflow/dags
dags_are_paused_at_creation = True
default_timezone = utc

[database]
sql_alchemy_conn = postgresql+psycopg2://airflow:airflow@postgres:5432/airflow

[webserver]
secret_key = asm2_secret_key
web_server_host = 0.0.0.0
web_server_port = 8080

[scheduler]
dag_dir_list_interval = 30
max_threads = 2

[logging]
base_log_folder = /opt/airflow/logs
remote_logging = False
```

Trong `docker-compose.yml`, file này được mount vào container:

```yaml
- ./config/airflow.cfg:/opt/airflow/airflow.cfg
```

Ngoài ra vẫn giữ biến môi trường:

```yaml
AIRFLOW__CORE__EXECUTOR: LocalExecutor
```

---

## 6. Các script hỗ trợ

Tất cả lệnh vận hành được gom trong thư mục:

```text
scripts/
```

Cấp quyền chạy:

```bash
chmod +x scripts/*.sh
```

### 6.1. `scripts/build.sh`

Dùng khi sửa `Dockerfile`, cài thêm system package, MongoDB tools, Java, Spark hoặc muốn build lại image chuẩn.

```bash
./scripts/build.sh
```

Nội dung khuyến nghị:

```bash
#!/bin/bash

set -e

echo "====================================="
echo " ASM2 - Build Docker Images"
echo "====================================="

if [ ! -f ".env" ]; then
  echo ".env not found. Creating from .env.example..."
  cp .env.example .env
fi

mkdir -p dags spark/jobs scripts data/raw data/output logs plugins config submission

docker compose down
docker compose build

echo "Build completed."
echo "Run project with: ./scripts/run-dev.sh"
```

### 6.2. `scripts/run-dev.sh`

Dùng hằng ngày để chạy môi trường dev. Không ép build lại Docker.

```bash
./scripts/run-dev.sh
```

Nội dung khuyến nghị:

```bash
#!/bin/bash

set -e

echo "====================================="
echo " ASM2 - Run Dev Environment"
echo "====================================="

if [ ! -f ".env" ]; then
  echo ".env not found. Creating from .env.example..."
  cp .env.example .env
fi

mkdir -p dags spark/jobs scripts data/raw data/output logs plugins config submission

docker compose up -d

echo "Airflow UI : http://localhost:8080"
echo "Username   : admin"
echo "Password   : admin"
echo "MongoDB    : mongodb://localhost:27017"
echo ""
echo "Next steps:"
echo "./scripts/check.sh"
echo "./scripts/setup-spark-connection.sh"
```

### 6.3. `scripts/stop.sh`

Dừng container, không xoá volume.

```bash
./scripts/stop.sh
```

Nội dung khuyến nghị:

```bash
#!/bin/bash

set -e

echo "Stopping ASM2 containers..."
docker compose down
echo "Stopped."
```

### 6.4. `scripts/restart-airflow.sh`

Dùng khi sửa DAG, Spark job hoặc downloader. Không build Docker.

```bash
./scripts/restart-airflow.sh
```

Nội dung khuyến nghị, có xử lý lỗi stale PID webserver:

```bash
#!/bin/bash

set -e

echo "====================================="
echo " ASM2 - Restart Airflow Safely"
echo "====================================="

echo "Stopping Airflow webserver and scheduler..."
docker compose stop airflow-webserver airflow-scheduler || true

echo "Removing old webserver container to avoid stale PID..."
docker rm -f asm2-airflow-webserver || true

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
echo "Checking DAG import errors:"
docker exec -it asm2-airflow-webserver airflow dags list-import-errors || true

echo ""
echo "Reserializing DAGs:"
docker exec -it asm2-airflow-webserver airflow dags reserialize || true

echo ""
echo "Checking ASM2 DAG:"
docker exec -it asm2-airflow-webserver airflow dags list | grep asm2 || true

echo "Done."
```

### 6.5. `scripts/check.sh`

Check nhanh toàn bộ môi trường.

```bash
./scripts/check.sh
```

Nội dung khuyến nghị:

```bash
#!/bin/bash

set -e

echo "====================================="
echo " ASM2 - Check Environment"
echo "====================================="

echo ""
echo "1. Docker Compose status:"
docker compose ps

echo ""
echo "2. Airflow executor:"
docker exec -it asm2-airflow-webserver airflow config get-value core executor || true

echo ""
echo "3. DAG import errors:"
docker exec -it asm2-airflow-webserver airflow dags list-import-errors || true

echo ""
echo "4. DAG list:"
docker exec -it asm2-airflow-webserver airflow dags list | grep asm2 || true

echo ""
echo "5. Java version:"
docker exec -it asm2-airflow-scheduler java -version || true

echo ""
echo "6. Spark version:"
docker exec -it asm2-airflow-scheduler spark-submit --version || true

echo ""
echo "7. MongoImport version:"
docker exec -it asm2-airflow-scheduler mongoimport --version || true

echo ""
echo "8. Spark connection:"
docker exec -it asm2-airflow-webserver airflow connections get spark_default || true

echo ""
echo "Check completed."
```

### 6.6. `scripts/setup-spark-connection.sh`

Tạo connection `spark_default` để `SparkSubmitOperator` chạy local thay vì fallback sang YARN.

```bash
./scripts/setup-spark-connection.sh
```

Nội dung khuyến nghị:

```bash
#!/bin/bash

set -e

echo "====================================="
echo " ASM2 - Setup Spark Connection"
echo "====================================="

echo "Deleting old spark_default connection if exists..."
docker exec -it asm2-airflow-webserver airflow connections delete spark_default || true

echo "Creating spark_default connection..."
docker exec -it asm2-airflow-webserver airflow connections add spark_default \
  --conn-type spark \
  --conn-host 'local[*]' \
  --conn-extra '{"deploy-mode":"client"}'

echo ""
echo "Current spark_default connection:"
docker exec -it asm2-airflow-webserver airflow connections get spark_default

echo "Done."
```

### 6.7. `scripts/reset-data.sh`

Xoá input/output local để ép DAG chạy full từ `clear_file`.

```bash
./scripts/reset-data.sh
```

Nội dung khuyến nghị:

```bash
#!/bin/bash

set -e

echo "====================================="
echo " ASM2 - Reset Data"
echo "====================================="

rm -f data/raw/Questions.csv
rm -f data/raw/Answers.csv
rm -rf data/output/question_answer_count

echo "Local data cleaned:"
echo "- data/raw/Questions.csv"
echo "- data/raw/Answers.csv"
echo "- data/output/question_answer_count"

echo ""
echo "If you want to also reset MongoDB collections, run:"
echo "docker exec -it asm2-mongo mongosh"
echo ""
echo "Then inside mongosh:"
echo "use stackoverflow_asm2"
echo "db.questions.drop()"
echo "db.answers.drop()"
echo "db.answer_counts.drop()"
```

### 6.8. `scripts/test-spark.sh`

Test riêng yêu cầu 6 bằng `spark-submit`.

```bash
./scripts/test-spark.sh
```

Nội dung khuyến nghị:

```bash
#!/bin/bash

set -e

echo "====================================="
echo " ASM2 - Test Spark Job"
echo "====================================="

docker exec -it asm2-airflow-scheduler bash -c '
export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-17-openjdk-arm64}
export PATH=$JAVA_HOME/bin:$PATH

echo "JAVA_HOME=$JAVA_HOME"
echo "Testing spark-submit..."
spark-submit --version

echo "Running Spark job..."
spark-submit \
  --master local[*] \
  --packages org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 \
  --conf spark.mongodb.read.connection.uri=mongodb://mongo:27017 \
  --conf spark.mongodb.write.connection.uri=mongodb://mongo:27017 \
  /opt/airflow/spark/jobs/process_stackoverflow.py
'

echo ""
echo "Checking Spark output on host..."
ls -lah data/output/question_answer_count || true

echo ""
echo "Preview output:"
head -n 20 data/output/question_answer_count/part-*.csv || true
```

### 6.9. `scripts/logs.sh`

Xem log nhanh theo service.

```bash
./scripts/logs.sh scheduler
./scripts/logs.sh webserver
./scripts/logs.sh mongo
./scripts/logs.sh postgres
./scripts/logs.sh worker
./scripts/logs.sh redis
./scripts/logs.sh flower
```

Nội dung khuyến nghị:

```bash
#!/bin/bash

SERVICE=${1:-scheduler}

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
  flower)
    docker logs -f asm2-airflow-flower
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
    echo "./scripts/logs.sh flower"
    exit 1
    ;;
esac
```

### 6.10. `scripts/run-celery.sh`

Chạy bản CeleryExecutor cho yêu cầu 9.

```bash
./scripts/run-celery.sh
```

Nội dung khuyến nghị:

```bash
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
echo "Flower UI  : http://localhost:5555"
echo "Username   : admin"
echo "Password   : admin"
echo ""
echo "Check executor:"
echo "docker exec -it asm2-airflow-webserver airflow config get-value core executor"
echo ""
echo "Check worker:"
echo "docker logs -f asm2-airflow-worker"
```

### 6.11. `scripts/rebuild-clean.sh`

Chỉ dùng khi đổi base image hoặc lỗi cache nặng.

```bash
./scripts/rebuild-clean.sh
```

Nội dung khuyến nghị:

```bash
#!/bin/bash

set -e

echo "====================================="
echo " ASM2 - Clean Rebuild"
echo "====================================="

docker compose down
docker compose build --no-cache
docker compose up -d

echo "Clean rebuild completed."
```

### 6.12. `scripts/export-output.sh`

Copy CSV Spark output ra folder `submission`.

```bash
./scripts/export-output.sh
```

Nội dung khuyến nghị:

```bash
#!/bin/bash

set -e

echo "====================================="
echo " ASM2 - Export Processed CSV"
echo "====================================="

mkdir -p submission

OUTPUT_FILE=$(find "data/output/question_answer_count" -name "part-*.csv" | head -n 1)

if [ -z "$OUTPUT_FILE" ]; then
  echo "No Spark output CSV found."
  echo "Please run DAG or ./scripts/test-spark.sh first."
  exit 1
fi

cp "$OUTPUT_FILE" submission/question_answer_count.csv

echo "Exported to submission/question_answer_count.csv"
echo ""
head -n 20 submission/question_answer_count.csv
```

---

## 7. Workflow chạy project

### Lần đầu setup

```bash
cp .env.example .env
chmod +x scripts/*.sh

./scripts/build.sh
./scripts/run-dev.sh
./scripts/setup-spark-connection.sh
./scripts/check.sh
```

Mở Airflow:

```text
http://localhost:8080
```

Login:

```text
admin / admin
```

### Khi sửa code DAG

Các file liên quan:

```text
dags/asm2_stackoverflow_pipeline.py
dags/utils/google_drive_downloader.py
```

Chạy:

```bash
./scripts/restart-airflow.sh
```

Không build Docker.

### Khi sửa Spark job

File liên quan:

```text
spark/jobs/process_stackoverflow.py
```

Test nhanh:

```bash
./scripts/test-spark.sh
```

Hoặc restart scheduler rồi clear task `spark_process` trên Airflow UI:

```bash
./scripts/restart-airflow.sh
```

### Khi sửa Dockerfile hoặc thêm system package

Các file liên quan:

```text
docker/airflow/Dockerfile
requirements.txt
```

Chạy:

```bash
./scripts/build.sh
./scripts/run-dev.sh
```

Nếu lỗi cache nặng:

```bash
./scripts/rebuild-clean.sh
```

### Khi muốn chạy full pipeline từ đầu

```bash
./scripts/reset-data.sh
```

Sau đó vào Airflow UI:

```text
DAG asm2_stackoverflow_pipeline
→ Trigger DAG
```

### Khi muốn xuất file CSV đã xử lý để nộp

```bash
./scripts/export-output.sh
```

File output:

```text
submission/question_answer_count.csv
```

---

## 8. Kiểm thử theo từng yêu cầu

### Yêu cầu 1 - Task `start` và `end`

Vào Airflow Graph View, kiểm tra có 2 task:

```text
start
end
```

Cả hai dùng `DummyOperator` hoặc fallback `EmptyOperator`.

### Yêu cầu 2 - Task `branching`

Task `branching` dùng `BranchPythonOperator`.

Logic:

| Điều kiện | Nhánh |
|---|---|
| Có đủ `Questions.csv` và `Answers.csv` | `branching -> end` |
| Thiếu một trong hai file | `branching -> clear_file` |

Test case đã có file:

```bash
touch data/raw/Questions.csv
touch data/raw/Answers.csv
```

Trigger DAG. Kết quả:

```text
start -> branching -> end
```

Test case chưa có file:

```bash
./scripts/reset-data.sh
```

Trigger DAG. Kết quả:

```text
start -> branching -> clear_file -> ...
```

### Yêu cầu 3 - Task `clear_file`

Task `clear_file` dùng `BashOperator`.

Nhiệm vụ:

```text
Xoá Questions.csv
Xoá Answers.csv
Xoá output Spark cũ
```

Test:

```bash
./scripts/reset-data.sh
```

Sau khi trigger DAG, xem log task `clear_file` trong Airflow UI.

### Yêu cầu 4 - Download CSV từ Google Drive

Task:

```text
download_question_file_task
download_answer_file_task
```

Dùng `PythonOperator` và custom downloader:

```text
dags/utils/google_drive_downloader.py
```

Test output:

```bash
ls -lah data/raw
head -n 3 data/raw/Questions.csv
head -n 3 data/raw/Answers.csv
```

Kỳ vọng có:

```text
Questions.csv
Answers.csv
```

### Yêu cầu 5 - Import CSV vào MongoDB

Task:

```text
import_questions_mongo
import_answers_mongo
```

Dùng `BashOperator` với `mongoimport`.

Test:

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

Kỳ vọng có:

```text
questions
answers
```

và count > 0.

### Yêu cầu 6 - Spark process

Task:

```text
spark_process
```

Dùng `SparkSubmitOperator`.

Spark job:

```text
spark/jobs/process_stackoverflow.py
```

Logic xử lý:

```text
Đọc collection answers
Group by ParentId
Đếm số answer của mỗi question
Đổi ParentId thành Id
Ghi output CSV
```

Test riêng:

```bash
./scripts/test-spark.sh
```

Check output:

```bash
ls -lah data/output/question_answer_count
head -n 20 data/output/question_answer_count/part-*.csv
```

Kỳ vọng:

```csv
Id,Number of answers
80,3
90,3
120,1
180,9
260,9
```

### Yêu cầu 7 - Import Spark output vào MongoDB

Task:

```text
import_output_mongo
```

Dùng `BashOperator` với `mongoimport`.

Test:

```bash
docker exec -it asm2-mongo mongosh
```

Trong Mongo shell:

```javascript
use stackoverflow_asm2

db.answer_counts.countDocuments()
db.answer_counts.find().limit(20)
```

Kỳ vọng collection:

```text
answer_counts
```

và document có dạng:

```javascript
{
  Id: 80,
  "Number of answers": 3
}
```

### Yêu cầu 8 - Sắp xếp task và chạy song song bằng LocalExecutor

Check executor:

```bash
docker exec -it asm2-airflow-webserver airflow config get-value core executor
```

Kỳ vọng:

```text
LocalExecutor
```

Check Graph View trong Airflow:

```text
download_question_file_task và download_answer_file_task chạy song song
import_questions_mongo và import_answers_mongo chạy song song
```

Có thể chụp màn hình Graph View DAG xanh hết để nộp.

### Yêu cầu 9 - CeleryExecutor và Flower

Chạy bản Celery:

```bash
./scripts/run-celery.sh
```

Check executor:

```bash
docker exec -it asm2-airflow-webserver airflow config get-value core executor
```

Kỳ vọng:

```text
CeleryExecutor
```

Check container:

```bash
docker compose -f docker-compose.celery.yml ps
```

Kỳ vọng có:

```text
asm2-redis
asm2-airflow-worker
asm2-airflow-flower
asm2-airflow-webserver
asm2-airflow-scheduler
```

Mở Flower:

```text
http://localhost:5555
```

Chụp màn hình Flower thấy worker online:

```text
celery@...
Status: online
```

Đây là ảnh nộp cho phần:

```text
Ảnh chụp màn hình của Flower để thiết lập các Worker
```

---

## 9. Các lỗi thường gặp và cách xử lý

### 9.1. DAG missing from DagBag

Check lỗi import:

```bash
docker exec -it asm2-airflow-webserver airflow dags list-import-errors
```

Sau khi sửa DAG:

```bash
./scripts/restart-airflow.sh
```

### 9.2. Spark fallback sang YARN

Log lỗi:

```text
Could not load connection string spark_default, defaulting to yarn
spark-submit --master yarn
```

Fix:

```bash
./scripts/setup-spark-connection.sh
docker compose restart airflow-scheduler
```

Log đúng phải là:

```text
spark-submit --master local[*]
```

### 9.3. Spark lỗi JAVA_HOME

Log lỗi:

```text
/usr/lib/jvm/java-17-openjdk-amd64/bin/java: No such file or directory
```

Fix trên Mac M1/M2/M3:

```env
JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
```

Recreate container, không cần build:

```bash
docker compose up -d --force-recreate airflow-webserver airflow-scheduler
```

Check:

```bash
docker exec -it asm2-airflow-scheduler bash -c 'echo $JAVA_HOME'
docker exec -it asm2-airflow-scheduler spark-submit --version
```

### 9.4. Google Drive tải về HTML thay vì CSV

Log lỗi:

```text
Downloaded file is HTML, not CSV
```

Nguyên nhân thường là Google Drive trả về warning page hoặc file chưa public.

Cần check:

```text
Anyone with the link can view
```

Sau đó xoá file tải lỗi:

```bash
rm -f data/raw/Questions.csv
rm -f data/raw/Answers.csv
```

Trigger DAG lại.

### 9.5. Webserver lỗi stale PID

Log lỗi:

```text
Error: Already running on PID ... or pid file '/opt/airflow/airflow-webserver.pid' is stale
```

Fix nhanh:

```bash
docker compose stop airflow-webserver
docker rm -f asm2-airflow-webserver
docker compose up -d airflow-webserver
```

Hoặc dùng:

```bash
./scripts/restart-airflow.sh
```

---

## 10. Các file cần nộp

Nên chuẩn bị folder:

```text
submission/
├── question_answer_count.csv
└── screenshots/
    ├── 01_airflow_graph_success.png
    ├── 02_local_executor.png
    ├── 03_mongodb_answer_counts.png
    ├── 04_spark_output_csv.png
    ├── 05_celery_executor.png
    ├── 06_flower_workers.png
    └── 07_flower_tasks.png
```

Tạo file CSV output:

```bash
./scripts/export-output.sh
```

Các ảnh nên chụp:

| Ảnh | Nội dung |
|---|---|
| `01_airflow_graph_success.png` | Graph View DAG xanh hết |
| `02_local_executor.png` | Terminal hiển thị `LocalExecutor` |
| `03_mongodb_answer_counts.png` | MongoDB `db.answer_counts.find().limit(20)` |
| `04_spark_output_csv.png` | Terminal `head -n 20 submission/question_answer_count.csv` |
| `05_celery_executor.png` | Terminal hiển thị `CeleryExecutor` |
| `06_flower_workers.png` | Flower UI worker online |
| `07_flower_tasks.png` | Flower UI task đã chạy qua worker |

---

## 11. Tóm tắt trạng thái 9 yêu cầu

| Yêu cầu | Nội dung | File / Task |
|---|---|---|
| 1 | Tạo task start và end | `start`, `end` |
| 2 | Branch kiểm tra file đã tải chưa | `branching` |
| 3 | Xoá file cũ trước khi tải | `clear_file` |
| 4 | Download Questions.csv và Answers.csv | `download_question_file_task`, `download_answer_file_task` |
| 5 | Import raw CSV vào MongoDB | `import_questions_mongo`, `import_answers_mongo` |
| 6 | Spark xử lý số answer mỗi question | `spark_process`, `process_stackoverflow.py` |
| 7 | Import output Spark vào MongoDB | `import_output_mongo` |
| 8 | Sắp xếp task, chạy song song, LocalExecutor | `docker-compose.yml`, `airflow.cfg` |
| 9 | CeleryExecutor, Redis, Worker, Flower | `docker-compose.celery.yml`, `airflow-worker`, `airflow-flower` |

---

## 12. Quick commands

Chạy dev:

```bash
./scripts/run-dev.sh
./scripts/setup-spark-connection.sh
```

Check:

```bash
./scripts/check.sh
```

Reset data và chạy full:

```bash
./scripts/reset-data.sh
```

Sau đó trigger DAG trên Airflow UI.

Test Spark:

```bash
./scripts/test-spark.sh
```

Export output:

```bash
./scripts/export-output.sh
```

Chạy CeleryExecutor:

```bash
./scripts/run-celery.sh
```

Mở UI:

```text
Airflow: http://localhost:8080
Flower : http://localhost:5555
MongoDB: mongodb://localhost:27017
```
