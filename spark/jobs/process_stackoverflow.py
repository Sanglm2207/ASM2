from __future__ import annotations

import os
import shutil
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count


MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017")
MONGO_DATABASE = os.getenv("MONGO_DATABASE", "stackoverflow_asm2")

DATA_RAW_PATH = Path(os.getenv("DATA_RAW_PATH", "/opt/airflow/data/raw"))
DATA_OUTPUT_PATH = Path(os.getenv("DATA_OUTPUT_PATH", "/opt/airflow/data/output"))

ANSWERS_FILE = DATA_RAW_PATH / "Answers.csv"
OUTPUT_DIR = DATA_OUTPUT_PATH / "question_answer_count"


def main() -> None:
    """
    Spark job dùng để tính số lượng câu trả lời của từng câu hỏi.

    Input:
        MongoDB collection answers.

    Fallback input:
        Answers.csv nếu Spark không đọc được MongoDB.

    Output:
        CSV file tại data/output/question_answer_count.
    """

    spark = (
        SparkSession.builder
        .appName("ASM2 StackOverflow Answer Count")
        .config("spark.mongodb.read.connection.uri", MONGO_URI)
        .config("spark.mongodb.write.connection.uri", MONGO_URI)
        .getOrCreate()
    )

    print("ASM2 Spark job started.")
    print(f"Mongo URI      : {MONGO_URI}")
    print(f"Mongo database : {MONGO_DATABASE}")
    print(f"Answers file   : {ANSWERS_FILE}")
    print(f"Output dir     : {OUTPUT_DIR}")

    if OUTPUT_DIR.exists():
        shutil.rmtree(OUTPUT_DIR)

    answers_df = read_answers_data(spark)

    if "ParentId" not in answers_df.columns:
        raise ValueError("Required column ParentId does not exist in answers data.")

    result_df = (
        answers_df
        .filter(col("ParentId").isNotNull())
        .groupBy(col("ParentId").alias("Id"))
        .agg(count("*").alias("Number of answers"))
        .orderBy(col("Id").cast("int"))
    )

    print("Result preview:")
    result_df.show(20, truncate=False)

    (
        result_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(str(OUTPUT_DIR))
    )

    print(f"Spark output saved to: {OUTPUT_DIR}")

    spark.stop()

    print("ASM2 Spark job finished.")


def read_answers_data(spark: SparkSession):
    """
    Đọc dữ liệu answers từ MongoDB.

    Nếu Mongo Spark Connector lỗi, fallback sang đọc trực tiếp Answers.csv
    để vẫn test được logic Spark transformation.
    """

    try:
        print("Reading answers from MongoDB...")

        return (
            spark.read
            .format("mongodb")
            .option("database", MONGO_DATABASE)
            .option("collection", "answers")
            .load()
        )

    except Exception as exc:
        print("Cannot read from MongoDB. Fallback to CSV.")
        print(f"MongoDB error: {exc}")

        return (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(str(ANSWERS_FILE))
        )


if __name__ == "__main__":
    main()