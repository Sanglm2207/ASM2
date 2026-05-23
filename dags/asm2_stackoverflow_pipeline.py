from __future__ import annotations

import os
from pathlib import Path
from datetime import datetime

from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.trigger_rule import TriggerRule

# ---------------------------------------------------------------------
# DummyOperator Compatibility
# ---------------------------------------------------------------------
# Đề bài yêu cầu sử dụng DummyOperator cho task start và end.
#
# Tuy nhiên, trong các version Airflow mới, DummyOperator đã được thay thế
# bởi EmptyOperator.
#
# Đoạn try/except này giúp code:
# - Vẫn đúng yêu cầu đề bài: dùng DummyOperator về mặt ý nghĩa.
# - Vẫn chạy được trên các version Airflow mới.
try:
    from airflow.operators.dummy import DummyOperator
except ImportError:
    from airflow.operators.empty import EmptyOperator as DummyOperator

# Thư viện dùng để tải file từ Google Drive theo file_id.
from utils.google_drive_downloader import GoogleDriveDownloader as gdd


# =====================================================================
# 1. CONFIGURATION
# =====================================================================

# ---------------------------------------------------------------------
# Data path configuration
# ---------------------------------------------------------------------
# Lưu ý:
# - Trên máy thật, data nằm trong folder project:
#       ASM2/data/raw
#       ASM2/data/output
#
# - Nhưng DAG chạy bên trong Docker container Airflow.
# - Trong docker-compose.yml, folder ./data được mount vào:
#       /opt/airflow/data
#
# Vì vậy trong code Airflow phải dùng path bên trong container:
#       /opt/airflow/data/raw
#       /opt/airflow/data/output
DATA_RAW_PATH = Path(os.getenv("DATA_RAW_PATH", "/opt/airflow/data/raw"))
DATA_OUTPUT_PATH = Path(os.getenv("DATA_OUTPUT_PATH", "/opt/airflow/data/output"))

# File input sau khi download từ Google Drive.
QUESTIONS_FILE = DATA_RAW_PATH / "Questions.csv"
ANSWERS_FILE = DATA_RAW_PATH / "Answers.csv"

# Thư mục output mà Spark sẽ ghi kết quả ra dạng CSV.
SPARK_OUTPUT_DIR = DATA_OUTPUT_PATH / "question_answer_count"

# File Spark job sẽ được SparkSubmitOperator submit.
SPARK_JOB_FILE = "/opt/airflow/spark/jobs/process_stackoverflow.py"

# ---------------------------------------------------------------------
# MongoDB configuration
# ---------------------------------------------------------------------
# MongoDB service chạy trong Docker Compose với service name là mongo.
# Vì Airflow container giao tiếp với MongoDB container trong cùng network,
# nên host sẽ là "mongo", không phải localhost.
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017")
MONGO_DATABASE = os.getenv("MONGO_DATABASE", "stackoverflow_asm2")

# ---------------------------------------------------------------------
# Google Drive file IDs
# ---------------------------------------------------------------------
# Link gốc:
# Answers.csv:
# https://drive.google.com/file/d/1cJnA9LLfCgO-H7gvExj7xj7U-7rqSKZo/view?usp=sharing
#
# Questions.csv:
# https://drive.google.com/file/d/1kmrNUA8Uz9lKFcrkFyRCiReYuqFefxkO/view?usp=sharing
#
# File ID là phần nằm giữa /d/ và /view.
QUESTIONS_FILE_ID = os.getenv(
    "QUESTIONS_FILE_ID",
    "1kmrNUA8Uz9lKFcrkFyRCiReYuqFefxkO",
)

ANSWERS_FILE_ID = os.getenv(
    "ANSWERS_FILE_ID",
    "1cJnA9LLfCgO-H7gvExj7xj7U-7rqSKZo",
)


# =====================================================================
# 2. PYTHON CALLABLE FUNCTIONS
# =====================================================================

def check_dataset_files() -> str:
    """
    Task branching sử dụng function này để kiểm tra xem dataset đã có
    đầy đủ hai file Questions.csv và Answers.csv hay chưa.

    Logic theo yêu cầu đề bài:

    Case 1:
        Nếu cả Questions.csv và Answers.csv đã tồn tại:
        -> Dataset đã được tải xuống.
        -> Không cần chạy lại pipeline xử lý.
        -> Chuyển thẳng đến task end.

    Case 2:
        Nếu thiếu một trong hai file:
        -> Dataset chưa sẵn sàng.
        -> Bắt đầu quá trình xử lý dữ liệu.
        -> Chuyển đến task clear_file.

    Returns:
        str:
            task_id mà BranchPythonOperator sẽ chọn để chạy tiếp.
    """

    questions_exists = QUESTIONS_FILE.exists()
    answers_exists = ANSWERS_FILE.exists()

    print(f"Checking dataset files in: {DATA_RAW_PATH}")
    print(f"Questions.csv exists: {questions_exists}")
    print(f"Answers.csv exists: {answers_exists}")

    if questions_exists and answers_exists:
        print("Dataset already exists. Pipeline will go to end.")
        return "end"

    print("Dataset is not ready. Pipeline will go to clear_file.")
    return "clear_file"


def download_question_file() -> None:
    """
    Download Questions.csv từ Google Drive về data/raw.
    """

    DATA_RAW_PATH.mkdir(parents=True, exist_ok=True)

    gdd.download_file_from_google_drive(
        file_id=QUESTIONS_FILE_ID,
        dest_path=str(QUESTIONS_FILE),
        unzip=False,
        overwrite=True,
        showsize=True,
    )

    if not QUESTIONS_FILE.exists():
        raise FileNotFoundError(f"Download failed: {QUESTIONS_FILE}")

    print(f"Downloaded Questions.csv to {QUESTIONS_FILE}")


def download_answer_file() -> None:
    """
    Download Answers.csv từ Google Drive về data/raw.
    """

    DATA_RAW_PATH.mkdir(parents=True, exist_ok=True)

    gdd.download_file_from_google_drive(
        file_id=ANSWERS_FILE_ID,
        dest_path=str(ANSWERS_FILE),
        unzip=False,
        overwrite=True,
        showsize=True,
    )

    if not ANSWERS_FILE.exists():
        raise FileNotFoundError(f"Download failed: {ANSWERS_FILE}")

    print(f"Downloaded Answers.csv to {ANSWERS_FILE}")

# =====================================================================
# 3. DAG DEFAULT ARGUMENTS
# =====================================================================

default_args = {
    # Người sở hữu DAG.
    "owner": "kai",

    # Không phụ thuộc vào kết quả lần chạy trước.
    "depends_on_past": False,

    # Không retry để dễ debug khi làm bài ASM.
    # Nếu production thật có thể set retries > 0.
    "retries": 0,
}


# =====================================================================
# 4. DAG DEFINITION
# =====================================================================

with DAG(
    dag_id="asm2_stackoverflow_pipeline",
    description="ASM2 - Data Pipeline for StackOverflow Questions and Answers using Airflow, MongoDB and Spark",
    default_args=default_args,

    # start_date chỉ là mốc để Airflow nhận DAG.
    # Vì schedule_interval=None nên DAG chỉ chạy khi trigger thủ công.
    start_date=datetime(2026, 1, 1),

    # Không chạy tự động theo lịch.
    schedule_interval=None,

    # Không backfill các ngày trong quá khứ.
    catchup=False,

    tags=["ASM2", "DEP303x", "Spark", "MongoDB"],
) as dag:

    # =================================================================
    # REQUIREMENT 1: TASK start
    # =================================================================
    # DummyOperator dùng để biểu diễn điểm bắt đầu của DAG.
    # Task này không xử lý dữ liệu.
    start = DummyOperator(
        task_id="start",
    )

    # =================================================================
    # REQUIREMENT 2: TASK branching
    # =================================================================
    # BranchPythonOperator dùng để rẽ nhánh pipeline dựa trên điều kiện.
    #
    # Nếu đã có đủ:
    #     Questions.csv
    #     Answers.csv
    # thì task này trả về:
    #     "end"
    #
    # Nếu thiếu một trong hai file thì task này trả về:
    #     "clear_file"
    branching = BranchPythonOperator(
        task_id="branching",
        python_callable=check_dataset_files,
    )

    # =================================================================
    # REQUIREMENT 3: TASK clear_file
    # =================================================================
    # Đây là task đầu tiên trong quá trình xử lý dữ liệu.
    #
    # Trước khi download Questions.csv và Answers.csv từ Google Drive,
    # ta cần xoá các file cũ nếu đang tồn tại.
    #
    # Mục đích:
    # - Tránh lỗi ghi đè file.
    # - Tránh dùng nhầm dữ liệu cũ.
    # - Đảm bảo pipeline chạy lại từ đầu một cách sạch sẽ.
    #
    # Theo yêu cầu đề bài, task này sử dụng BashOperator.
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
        echo "Questions file path: {QUESTIONS_FILE}"
        echo "Answers file path  : {ANSWERS_FILE}"
        echo "Spark output path  : {SPARK_OUTPUT_DIR}"
        """,
    )

    # =================================================================
    # REQUIREMENT 4: TASK download_question_file_task
    # =================================================================
    # Task này tải Questions.csv từ Google Drive về data/raw.
    #
    # Theo yêu cầu đề bài:
    # - Sử dụng PythonOperator.
    # - Sử dụng thư viện google_drive_downloader.
    download_question_file_task = PythonOperator(
        task_id="download_question_file_task",
        python_callable=download_question_file,
    )

    # =================================================================
    # REQUIREMENT 4: TASK download_answer_file_task
    # =================================================================
    # Task này tải Answers.csv từ Google Drive về data/raw.
    #
    # Theo yêu cầu đề bài:
    # - Sử dụng PythonOperator.
    # - Sử dụng thư viện google_drive_downloader.
    download_answer_file_task = PythonOperator(
        task_id="download_answer_file_task",
        python_callable=download_answer_file,
    )

    # =================================================================
    # REQUIREMENT 5: TASK import_questions_mongo
    # =================================================================
    # Sau khi Questions.csv đã được tải xuống, task này import file CSV
    # vào MongoDB.
    #
    # Theo yêu cầu đề bài, sử dụng BashOperator với mongoimport:
    #
    # mongoimport --type csv -d <database> -c <collection> --headerline --drop <file>
    #
    # Giải thích option:
    # - --uri       : URI kết nối tới MongoDB.
    # - --type csv  : Dữ liệu đầu vào là file CSV.
    # - -d          : Tên database MongoDB.
    # - -c          : Tên collection.
    # - --headerline: Dòng đầu tiên của CSV là tên cột.
    # - --drop      : Xoá collection cũ trước khi import lại.
    import_questions_mongo = BashOperator(
        task_id="import_questions_mongo",
        bash_command=f"""
        echo "Start importing Questions.csv into MongoDB..."

        mongoimport \
          --uri="{MONGO_URI}" \
          --type csv \
          -d "{MONGO_DATABASE}" \
          -c questions \
          --headerline \
          --drop \
          "{QUESTIONS_FILE}"

        echo "Questions.csv has been imported into MongoDB collection: questions"
        """,
    )

    # =================================================================
    # REQUIREMENT 5: TASK import_answers_mongo
    # =================================================================
    # Sau khi Answers.csv đã được tải xuống, task này import file CSV
    # vào MongoDB collection answers.
    #
    # Collection answers sẽ được Spark dùng để tính toán số lượng câu trả lời
    # của từng câu hỏi.
    import_answers_mongo = BashOperator(
        task_id="import_answers_mongo",
        bash_command=f"""
        echo "Start importing Answers.csv into MongoDB..."

        mongoimport \
          --uri="{MONGO_URI}" \
          --type csv \
          -d "{MONGO_DATABASE}" \
          -c answers \
          --headerline \
          --drop \
          "{ANSWERS_FILE}"

        echo "Answers.csv has been imported into MongoDB collection: answers"
        """,
    )

    # =================================================================
    # REQUIREMENT 6: TASK spark_process
    # =================================================================
    # Task này submit Spark job để xử lý dữ liệu.
    #
    # Yêu cầu xử lý:
    # - Dựa vào tập Answers.csv.
    # - Mỗi answer có ParentId là ID của câu hỏi.
    # - Group theo ParentId để đếm số answer của mỗi question.
    #
    # Output cần có dạng:
    # +----+-----------------+
    # | Id | Number of answers |
    # +----+-----------------+
    #
    # Spark job sẽ:
    # - Đọc dữ liệu từ MongoDB collection answers.
    # - Group by ParentId.
    # - Count số lượng answer.
    # - Ghi kết quả ra CSV tại DATA_OUTPUT_PATH.
    #
    # SparkSubmitOperator được dùng đúng theo yêu cầu đề bài.
    spark_process = SparkSubmitOperator(
        task_id="spark_process",

        # File Python chứa Spark job.
        application=SPARK_JOB_FILE,

        # Connection spark_default cần được tạo trong Airflow.
        # Lệnh tạo:
        # docker exec -it asm2-airflow-webserver airflow connections add spark_default \
        #   --conn-type spark \
        #   --conn-host local \
        #   --conn-extra '{"deploy-mode":"client"}'
        conn_id="spark_default",

        # MongoDB Spark Connector.
        # Package này giúp Spark có thể đọc/ghi MongoDB.
        packages="org.mongodb.spark:mongo-spark-connector_2.12:10.3.0",

        # Config truyền cho Spark.
        conf={
            "spark.mongodb.read.connection.uri": MONGO_URI,
            "spark.mongodb.write.connection.uri": MONGO_URI,
        },

        # Biến môi trường truyền vào Spark job.
        # Spark job process_stackoverflow.py sẽ đọc các biến này bằng os.getenv().
        env_vars={
            "JAVA_HOME": "/usr/lib/jvm/java-17-openjdk-arm64",
            "MONGO_URI": MONGO_URI,
            "MONGO_DATABASE": MONGO_DATABASE,
            "DATA_RAW_PATH": str(DATA_RAW_PATH),
            "DATA_OUTPUT_PATH": str(DATA_OUTPUT_PATH),
        },

        # In log chi tiết để dễ debug khi chạy trên Airflow.
        verbose=True,
    )

    # =================================================================
    # REQUIREMENT 7: TASK import_output_mongo
    # =================================================================
    # Sau khi Spark xử lý xong, kết quả được ghi ra CSV.
    #
    # Spark không ghi ra một file CSV đơn lẻ, mà ghi ra một folder gồm:
    # - part-00000-xxxx.csv
    # - _SUCCESS
    #
    # Vì vậy task này cần tìm file part-*.csv trước,
    # sau đó dùng mongoimport để import kết quả vào MongoDB.
    #
    # Collection output:
    #     answer_counts
    import_output_mongo = BashOperator(
        task_id="import_output_mongo",
        bash_command=f"""
        echo "Start importing Spark output CSV into MongoDB..."

        OUTPUT_FILE=$(find "{SPARK_OUTPUT_DIR}" -name "part-*.csv" | head -n 1)

        if [ -z "$OUTPUT_FILE" ]; then
          echo "No Spark output CSV file found in {SPARK_OUTPUT_DIR}"
          exit 1
        fi

        echo "Found Spark output file: $OUTPUT_FILE"

        mongoimport \
          --uri="{MONGO_URI}" \
          --type csv \
          -d "{MONGO_DATABASE}" \
          -c answer_counts \
          --headerline \
          --drop \
          "$OUTPUT_FILE"

        echo "Spark output has been imported into MongoDB collection: answer_counts"
        """,
    )

    # =================================================================
    # REQUIREMENT 1: TASK end
    # =================================================================
    # DummyOperator dùng để biểu diễn điểm kết thúc DAG.
    #
    # Vì task end nhận flow từ nhiều nhánh:
    # - branching -> end
    # - import_output_mongo -> end
    #
    # BranchPythonOperator sẽ skip nhánh không được chọn.
    # Nếu không set trigger_rule, task end có thể bị skip theo.
    #
    # NONE_FAILED_MIN_ONE_SUCCESS nghĩa là:
    # - Chỉ cần ít nhất một upstream task success.
    # - Không có upstream task nào failed.
    # thì end sẽ được chạy.
    end = DummyOperator(
        task_id="end",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # =================================================================
    # REQUIREMENT 8: TASK DEPENDENCIES
    # =================================================================
    # Sắp xếp thứ tự các task theo đúng flow của đề bài:
    #
    # start
    #   ↓
    # branching
    #   ├── end
    #   └── clear_file
    #         ↓
    #       download_answer_file_task      download_question_file_task
    #         ↓                            ↓
    #       import_answers_mongo           import_questions_mongo
    #         └──────────────┬─────────────┘
    #                        ↓
    #                  spark_process
    #                        ↓
    #                import_output_mongo
    #                        ↓
    #                       end
    #
    # Các task chạy song song:
    # - download_question_file_task và download_answer_file_task
    # - import_questions_mongo và import_answers_mongo

    start >> branching

    # Nếu dataset đã tồn tại thì kết thúc pipeline theo yêu cầu branching.
    branching >> end

    # Nếu dataset chưa tồn tại thì bắt đầu xử lý dữ liệu.
    branching >> clear_file

    # Download 2 file song song để tiết kiệm thời gian.
    clear_file >> [
        download_answer_file_task,
        download_question_file_task,
    ]

    # Sau khi tải Answers.csv thì import vào MongoDB collection answers.
    download_answer_file_task >> import_answers_mongo

    # Sau khi tải Questions.csv thì import vào MongoDB collection questions.
    download_question_file_task >> import_questions_mongo

    # Chỉ chạy Spark sau khi cả 2 collection đã import xong.
    [
        import_answers_mongo,
        import_questions_mongo,
    ] >> spark_process

    # Sau khi Spark xử lý xong thì import output vào MongoDB.
    spark_process >> import_output_mongo

    # Kết thúc pipeline.
    import_output_mongo >> end