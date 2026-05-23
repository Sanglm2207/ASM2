from __future__ import annotations

import os
import shutil
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count


# =====================================================================
# CONFIGURATION
# =====================================================================

# MongoDB URI.
# Trong Docker Compose, MongoDB service name là "mongo",
# nên URI mặc định là mongodb://mongo:27017.
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017")

# Tên MongoDB database dùng cho bài ASM2.
MONGO_DATABASE = os.getenv("MONGO_DATABASE", "stackoverflow_asm2")

# Path dữ liệu trong container Airflow.
DATA_RAW_PATH = Path(os.getenv("DATA_RAW_PATH", "/opt/airflow/data/raw"))
DATA_OUTPUT_PATH = Path(os.getenv("DATA_OUTPUT_PATH", "/opt/airflow/data/output"))

# File Answers.csv dùng làm fallback nếu đọc MongoDB lỗi.
ANSWERS_FILE = DATA_RAW_PATH / "Answers.csv"

# Folder output để Spark ghi kết quả.
OUTPUT_DIR = DATA_OUTPUT_PATH / "question_answer_count"


# =====================================================================
# MAIN SPARK JOB
# =====================================================================

def main() -> None:
    """
    Spark job xử lý dữ liệu StackOverflow Answers.

    Mục tiêu:
        Tính toán mỗi câu hỏi có bao nhiêu câu trả lời.

    Input chính:
        MongoDB collection:
            database: stackoverflow_asm2
            collection: answers

    Input fallback:
        /opt/airflow/data/raw/Answers.csv

    Output:
        CSV folder:
            /opt/airflow/data/output/question_answer_count

    Output schema:
        Id, Number of answers

    Trong đó:
        Id:
            ID của câu hỏi.

        Number of answers:
            Số lượng câu trả lời của câu hỏi đó.
    """

    # -----------------------------------------------------------------
    # Create SparkSession
    # -----------------------------------------------------------------
    # SparkSession là entry point chính để làm việc với Spark DataFrame.
    #
    # Các config spark.mongodb.* được truyền để Spark biết cách kết nối MongoDB.
    spark = (
        SparkSession.builder
        .appName("ASM2 StackOverflow Answer Count")
        .config("spark.mongodb.read.connection.uri", MONGO_URI)
        .config("spark.mongodb.write.connection.uri", MONGO_URI)
        .getOrCreate()
    )

    print("==================================================")
    print("ASM2 Spark Processing Started")
    print("==================================================")
    print(f"Mongo URI       : {MONGO_URI}")
    print(f"Mongo Database  : {MONGO_DATABASE}")
    print(f"Answers CSV     : {ANSWERS_FILE}")
    print(f"Output directory: {OUTPUT_DIR}")

    # -----------------------------------------------------------------
    # Clear old Spark output
    # -----------------------------------------------------------------
    # Spark mặc định không cho ghi đè vào một folder đã tồn tại nếu không
    # set mode overwrite.
    #
    # Dù phía dưới đã dùng mode("overwrite"), việc xoá trước giúp output
    # sạch hơn và tránh lỗi khi folder bị lỗi quyền hoặc còn file rác.
    if OUTPUT_DIR.exists():
        print(f"Removing old output directory: {OUTPUT_DIR}")
        shutil.rmtree(OUTPUT_DIR)

    # -----------------------------------------------------------------
    # Read answers data
    # -----------------------------------------------------------------
    # Theo flow pipeline:
    # 1. Download Answers.csv.
    # 2. Import Answers.csv vào MongoDB collection answers.
    # 3. Spark đọc collection answers để xử lý.
    #
    # Vì bài yêu cầu import vào MongoDB trước khi Spark xử lý,
    # nên hướng chính là đọc từ MongoDB.
    #
    # Tuy nhiên, trong quá trình làm bài, Mongo Spark Connector có thể lỗi
    # do network/package/version. Vì vậy code có fallback đọc từ CSV để
    # dễ debug hơn.
    try:
        print("Reading answers data from MongoDB collection: answers")

        answers_df = (
            spark.read
            .format("mongodb")
            .option("database", MONGO_DATABASE)
            .option("collection", "answers")
            .load()
        )

        print("Read answers data from MongoDB successfully.")

    except Exception as exc:
        print("Cannot read answers data from MongoDB.")
        print("Fallback to reading Answers.csv directly.")
        print(f"MongoDB read error: {exc}")

        answers_df = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(str(ANSWERS_FILE))
        )

        print("Read answers data from CSV successfully.")

    # -----------------------------------------------------------------
    # Validate input columns
    # -----------------------------------------------------------------
    # Answers.csv có cấu trúc:
    # - Id
    # - OwnerUserId
    # - CreationDate
    # - ParentId
    # - Score
    # - Body
    #
    # Trong đó ParentId là ID của câu hỏi mà answer đang thuộc về.
    if "ParentId" not in answers_df.columns:
        raise ValueError(
            "Input data does not contain required column: ParentId"
        )

    print("Input schema:")
    answers_df.printSchema()

    print("Input preview:")
    answers_df.show(5, truncate=False)

    # -----------------------------------------------------------------
    # Transform data
    # -----------------------------------------------------------------
    # Yêu cầu:
    # Tính toán xem mỗi câu hỏi có bao nhiêu câu trả lời.
    #
    # Mỗi dòng trong Answers.csv là một câu trả lời.
    # ParentId chính là ID của câu hỏi.
    #
    # Vì vậy:
    # - groupBy ParentId
    # - count số dòng answer thuộc về ParentId đó
    # - đổi tên ParentId thành Id
    # - đổi tên count thành Number of answers
    result_df = (
        answers_df
        .filter(col("ParentId").isNotNull())
        .groupBy(col("ParentId").alias("Id"))
        .agg(count("*").alias("Number of answers"))
        .orderBy(col("Id").cast("int"))
    )

    print("Result schema:")
    result_df.printSchema()

    print("Result preview:")
    result_df.show(20, truncate=False)

    # -----------------------------------------------------------------
    # Write output to CSV
    # -----------------------------------------------------------------
    # Theo yêu cầu đề bài:
    # Sau khi tính toán xong, sử dụng DataFrameWriter để lưu kết quả
    # dưới dạng CSV.
    #
    # Spark thường ghi ra nhiều file part-*.csv.
    # Ở đây dùng coalesce(1) để gom output về 1 file CSV,
    # giúp task import_output_mongo dễ tìm và import hơn.
    (
        result_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(str(OUTPUT_DIR))
    )

    print(f"Spark output saved successfully to: {OUTPUT_DIR}")

    # -----------------------------------------------------------------
    # Stop SparkSession
    # -----------------------------------------------------------------
    # Giải phóng tài nguyên Spark sau khi xử lý xong.
    spark.stop()

    print("==================================================")
    print("ASM2 Spark Processing Finished")
    print("==================================================")


if __name__ == "__main__":
    main()