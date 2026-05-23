FROM apache/airflow:2.10.5-python3.11

USER root

# Cài các package hệ thống cần thiết:
# - Java 17 để chạy Spark
# - wget/curl để tải Spark và MongoDB Database Tools
# - procps để Spark có thể dùng một số command hệ thống
RUN apt-get update && apt-get install -y \
    openjdk-17-jdk \
    curl \
    wget \
    gnupg \
    procps \
    ca-certificates \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# ---------------------------------------------------------------------
# Java configuration
# ---------------------------------------------------------------------
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# ---------------------------------------------------------------------
# Spark configuration
# ---------------------------------------------------------------------
ENV SPARK_VERSION=3.5.3
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark

# Tải và cài Spark vào /opt/spark
RUN wget -q "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    && tar -xzf "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" -C /opt \
    && mv "/opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}" "${SPARK_HOME}" \
    && rm "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz"

# ---------------------------------------------------------------------
# MongoDB Database Tools
# ---------------------------------------------------------------------
# Cài mongoimport bằng file tar.gz thay vì .deb.
# Lý do:
# - Mac M1/M2/M3 thường build Docker theo arm64.
# - MongoDB không phải lúc nào cũng có package debian12-arm64 .deb.
# - Bản ubuntu2204-arm64 tar.gz chạy ổn trên Debian 12 vì cùng nền glibc Linux.
RUN ARCH="$(dpkg --print-architecture)" \
    && if [ "$ARCH" = "amd64" ]; then TOOLS_ARCH="x86_64"; \
       elif [ "$ARCH" = "arm64" ]; then TOOLS_ARCH="arm64"; \
       else echo "Unsupported architecture: $ARCH" && exit 1; \
       fi \
    && echo "Installing MongoDB Database Tools tar.gz for architecture: $TOOLS_ARCH" \
    && wget -q "https://fastdl.mongodb.org/tools/db/mongodb-database-tools-ubuntu2204-${TOOLS_ARCH}-100.9.4.tgz" \
    && tar -xzf "mongodb-database-tools-ubuntu2204-${TOOLS_ARCH}-100.9.4.tgz" \
    && cp "mongodb-database-tools-ubuntu2204-${TOOLS_ARCH}-100.9.4/bin/"* /usr/local/bin/ \
    && rm -rf "mongodb-database-tools-ubuntu2204-${TOOLS_ARCH}-100.9.4" \
    && rm "mongodb-database-tools-ubuntu2204-${TOOLS_ARCH}-100.9.4.tgz" \
    && mongoimport --version

# ---------------------------------------------------------------------
# PATH configuration
# ---------------------------------------------------------------------
# airflow binary nằm trong /home/airflow/.local/bin.
# Nếu thiếu path này có thể gặp lỗi: airflow: command not found
ENV PATH="/home/airflow/.local/bin:${JAVA_HOME}/bin:${SPARK_HOME}/bin:${PATH}"

USER airflow

COPY requirements.txt /requirements.txt

# Cài Python dependencies theo constraint của đúng Airflow version
# để tránh pip phá dependency nội bộ của Airflow.
RUN pip install --no-cache-dir \
    -r /requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.11.txt"