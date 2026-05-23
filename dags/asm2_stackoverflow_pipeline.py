from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.trigger_rule import TriggerRule

try:
    from airflow.operators.dummy import DummyOperator
except ImportError:
    from airflow.operators.empty import EmptyOperator as DummyOperator

from utils.google_drive_downloader import GoogleDriveDownloader as gdd


# =====================================================================
# CONFIGURATION
# =====================================================================

DATA_RAW_PATH = Path(os.getenv("DATA_RAW_PATH", "/opt/airflow/data/raw"))
DATA_OUTPUT_PATH = Path(os.getenv("DATA_OUTPUT_PATH", "/opt/airflow/data/output"))

QUESTIONS_FILE = DATA_RAW_PATH / "Questions.csv"
ANSWERS_FILE = DATA_RAW_PATH / "Answers.csv"

SPARK_OUTPUT_DIR = DATA_OUTPUT_PATH / "question_answer_count"
SPARK_JOB_FILE = "/opt/airflow/spark/jobs/process_stackoverflow.py"

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017")
MONGO_DATABASE = os.getenv("MONGO_DATABASE", "stackoverflow_asm2")

QUESTIONS_FILE_ID = os.getenv(
    "QUESTIONS_FILE_ID",
    "1kmrNUA8Uz9lKFcrkFyRCiReYuqFefxkO",
)

ANSWERS_FILE_ID = os.getenv(
    "ANSWERS_FILE_ID",
    "1cJnA9LLfCgO-H7gvExj7xj7U-7rqSKZo",
)

JAVA_HOME = os.getenv("JAVA_HOME", "/usr/lib/jvm/java-17-openjdk-arm64")


# =====================================================================
# PYTHON CALLABLES
# =====================================================================

def check_dataset_files() -> str:
    """
    Kiểm tra hai file Questions.csv và Answers.csv đã có sẵn hay chưa.

    Nếu cả hai file đã tồn tại:
        -> đi thẳng tới task end.

    Nếu thiếu một trong hai file:
        -> đi tới task clear_file để bắt đầu xử lý lại pipeline.
    """

    questions_exists = QUESTIONS_FILE.exists()
    answers_exists = ANSWERS_FILE.exists()

    print(f"Questions.csv exists: {questions_exists}")
    print(f"Answers.csv exists  : {answers_exists}")

    if questions_exists and answers_exists:
        return "end"

    return "clear_file"


def download_question_file() -> None:
    """
    Tải Questions.csv từ Google Drive về thư mục data/raw.
    """

    DATA_RAW_PATH.mkdir(parents=True, exist_ok=True)

    gdd.download_file_from_google_drive(
        file_id=QUESTIONS_FILE_ID,
        dest_path=str(QUESTIONS_FILE),
        overwrite=True,
        unzip=False,
        showsize=True,
    )

    if not QUESTIONS_FILE.exists():
        raise FileNotFoundError(f"Questions.csv download failed: {QUESTIONS_FILE}")

    print(f"Questions.csv downloaded: {QUESTIONS_FILE}")


def download_answer_file() -> None:
    """
    Tải Answers.csv từ Google Drive về thư mục data/raw.
    """

    DATA_RAW_PATH.mkdir(parents=True, exist_ok=True)

    gdd.download_file_from_google_drive(
        file_id=ANSWERS_FILE_ID,
        dest_path=str(ANSWERS_FILE),
        overwrite=True,
        unzip=False,
        showsize=True,
    )

    if not ANSWERS_FILE.exists():
        raise FileNotFoundError(f"Answers.csv download failed: {ANSWERS_FILE}")

    print(f"Answers.csv downloaded: {ANSWERS_FILE}")


# =====================================================================
# DAG DEFINITION
# =====================================================================

default_args = {
    "owner": "kai",
    "depends_on_past": False,
    "retries": 0,
}


with DAG(
    dag_id="asm2_stackoverflow_pipeline",
    description="ASM2 - Data Pipeline with Airflow, MongoDB and Spark",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
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
        echo "Cleaning old files..."

        mkdir -p "{DATA_RAW_PATH}"
        mkdir -p "{DATA_OUTPUT_PATH}"

        rm -f "{QUESTIONS_FILE}"
        rm -f "{ANSWERS_FILE}"
        rm -rf "{SPARK_OUTPUT_DIR}"

        echo "Clean completed."
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
        echo "Importing Questions.csv into MongoDB..."

        mongoimport \
          --uri="{MONGO_URI}" \
          --type csv \
          -d "{MONGO_DATABASE}" \
          -c questions \
          --headerline \
          --drop \
          "{QUESTIONS_FILE}"

        echo "Questions.csv imported."
        """,
    )

    # =================================================================
    # REQUIREMENT 5: TASK import_answers_mongo
    # =================================================================
    # Sau khi Answers.csv đã được tải xuống, task này import file CSV
    # vào MongoDB.
    #
    # Collection answers sẽ được Spark sử dụng để tính toán số lượng
    # câu trả lời của từng câu hỏi.
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
    import_answers_mongo = BashOperator(
        task_id="import_answers_mongo",
        bash_command=f"""
        echo "Importing Answers.csv into MongoDB..."

        mongoimport \
          --uri="{MONGO_URI}" \
          --type csv \
          -d "{MONGO_DATABASE}" \
          -c answers \
          --headerline \
          --drop \
          "{ANSWERS_FILE}"

        echo "Answers.csv imported."
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
        application=SPARK_JOB_FILE,
        conn_id="spark_default",
        packages="org.mongodb.spark:mongo-spark-connector_2.12:10.3.0",
        conf={
            "spark.mongodb.read.connection.uri": MONGO_URI,
            "spark.mongodb.write.connection.uri": MONGO_URI,
        },
        env_vars={
            "JAVA_HOME": JAVA_HOME,
            "MONGO_URI": MONGO_URI,
            "MONGO_DATABASE": MONGO_DATABASE,
            "DATA_RAW_PATH": str(DATA_RAW_PATH),
            "DATA_OUTPUT_PATH": str(DATA_OUTPUT_PATH),
        },
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
        echo "Importing Spark output into MongoDB..."

        OUTPUT_FILE=$(find "{SPARK_OUTPUT_DIR}" -name "part-*.csv" | head -n 1)

        if [ -z "$OUTPUT_FILE" ]; then
          echo "No Spark output file found in {SPARK_OUTPUT_DIR}"
          exit 1
        fi

        echo "Found output file: $OUTPUT_FILE"

        mongoimport \
          --uri="{MONGO_URI}" \
          --type csv \
          -d "{MONGO_DATABASE}" \
          -c answer_counts \
          --headerline \
          --drop \
          "$OUTPUT_FILE"

        echo "Spark output imported."
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
    # Requirement 8: task order and parallel execution
    # =================================================================

    start >> branching

    # Nếu đã có đủ Questions.csv và Answers.csv thì kết thúc.
    branching >> end

    # Nếu thiếu file thì chạy pipeline xử lý dữ liệu.
    branching >> clear_file

    # Hai task download chạy song song.
    clear_file >> [
        download_answer_file_task,
        download_question_file_task,
    ]

    # Hai task import MongoDB chạy song song sau khi từng file được tải xong.
    download_answer_file_task >> import_answers_mongo
    download_question_file_task >> import_questions_mongo

    # Spark chỉ chạy sau khi cả questions và answers đã import xong.
    [
        import_answers_mongo,
        import_questions_mongo,
    ] >> spark_process

    spark_process >> import_output_mongo >> end