FROM apache/spark-py:v3.3.2

USER root

RUN apt-get update && apt-get install -y curl 



WORKDIR /app

# =========================
# 1. Detect Spark home (важно для переносимости)
# =========================
ENV SPARK_HOME=/opt/spark

# =========================
# 2. Kafka connector для Spark 3.3.2 (Scala 2.12)
# =========================
RUN curl -L -o $SPARK_HOME/jars/spark-sql-kafka-0-10_2.12-3.3.2.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.3.2/spark-sql-kafka-0-10_2.12-3.3.2.jar

# =========================
# 3. Kafka clients
# =========================
RUN curl -L -o $SPARK_HOME/jars/kafka-clients-3.3.2.jar \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.3.2/kafka-clients-3.3.2.jar

# =========================
# 4. S3 support (Hadoop AWS)
# =========================
RUN curl -L -o $SPARK_HOME/jars/hadoop-aws-3.3.2.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.2/hadoop-aws-3.3.2.jar && \
    curl -L -o $SPARK_HOME/jars/aws-java-sdk-bundle-1.12.262.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# =========================
# 5. Yandex Cloud CA certificate (Kafka SSL / HTTPS)
# =========================
RUN mkdir -p /usr/local/share/ca-certificates/yandex && \
    curl -L https://storage.yandexcloud.net/cloud-certs/CA.pem \
    -o /usr/local/share/ca-certificates/yandex/yandex-ca.crt && \
    chmod 644 /usr/local/share/ca-certificates/yandex/yandex-ca.crt && \
    update-ca-certificates

# =========================
# 6. Python dependencies
# =========================
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# =========================
# 7. Application code
# =========================
COPY ./scripts/clean_and_prepare.py /app/clean_and_prepare.py

# =========================
# 8. Runtime configuration
# =========================
ENV KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
ENV KAFKA_TOPIC_IN="raw_data"
ENV KAFKA_TOPIC_OUT="for_ml_data"
