"""
Application
"""

import os
import numpy as np
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType, TimestampType, BooleanType, DateType
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.storagelevel import StorageLevel



KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_IN = os.getenv("KAFKA_TOPIC_IN", "raw_data")
KAFKA_TOPIC_OUT = os.getenv("KAFKA_TOPIC_OUT", "predictions")

print(f'KAFKA_BOOTSTRAP_SERVERS: {KAFKA_BOOTSTRAP_SERVERS}')

KAFKA_USER = os.getenv("KAFKA_USER")
KAFKA_PASS = os.getenv("KAFKA_PASS")

# Путь к сертификату в образе (после update-ca-certificates он доступен в системе)
CERT_PATH = "/usr/local/share/ca-certificates/yandex/yandex-ca.crt"
PERSIST_BUCKET_NAME = os.getenv("PERSIST_BUCKET_NAME")


import os
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *


CHECKPOINT_PATH = f"s3a://{PERSIST_BUCKET_NAME}/checkpoints/spark-job"

# Константы для обработки (Special IDs)
SPECIAL_CUSTOMER_ID = -1
SPECIAL_TERMINAL_ID = -1
SPECIAL_SCENARIO_ID = -1
SPECIAL_TIME_SECONDS = -1
SPECIAL_TIME_DAYS = -1

# =====================================================
# Инициализация Spark
# =====================================================
spark = (
    SparkSession.builder
    .appName("SparkKafkaStreamingJob")
    .getOrCreate()
)

# Схема входящего JSON
# schema = StructType([
#     StructField("transaction_id", LongType(), True),
#     StructField("tx_datetime", StringType(), True),
#     StructField("customer_id", IntegerType(), True),
#     StructField("terminal_id", IntegerType(), True),
#     StructField("tx_amount", DoubleType(), True),
#     StructField("tx_time_seconds", IntegerType(), True),
#     StructField("tx_time_days", IntegerType(), True),
#     StructField("tx_fraud", IntegerType(), True),
#     StructField("tx_fraud_scenario", IntegerType(), True)
# ])

schema = StructType([StructField('transaction_id', LongType(), True), 
                                 StructField('tx_amount', DoubleType(), True), 
                                 StructField('tx_fraud', IntegerType(), True), 
                                 StructField('unix_time', LongType(), True), 
                                 StructField('tx_amount_isnew_ct_30d_hist', IntegerType(), True), 
                                 StructField('tx_amount_isnew_ct_7d_hist', IntegerType(), True), 
                                 StructField('tx_amount_cn_cust_7d_full', LongType(), True), 
                                 StructField('tx_amount_avg_cust_7d_full', DoubleType(), True), 
                                 StructField('tx_amount_cn_cust_1d_full', LongType(), True), 
                                 StructField('tx_amount_avg_cust_1d_full', DoubleType(), True), 
                                 StructField('tx_amount_cn_cust_1h_full', LongType(), True), 
                                 StructField('tx_amount_avg_cust_1h_full', DoubleType(), True), 
                                 StructField('tx_amount_cn_cust_30d_hist', LongType(), True), 
                                 StructField('tx_amount_avg_cust_30d_hist', DoubleType(), True), 
                                 StructField('tx_amount_std_cust_30d_hist', DoubleType(), True), 
                                 StructField('tx_amount_cn_cust_7d_hist', LongType(), True), 
                                 StructField('tx_amount_avg_cust_7d_hist', DoubleType(), True), 
                                 StructField('tx_amount_std_cust_7d_hist', DoubleType(), True), 
                                 StructField('tx_amount_cn_cust_1d_hist', LongType(), True), 
                                 StructField('tx_amount_avg_cust_1d_hist', DoubleType(), True), 
                                 StructField('tx_amount_std_cust_1d_hist', DoubleType(), True), 
                                 StructField('term_tx_amount_cn_7d_full', LongType(), True), 
                                 StructField('term_tx_amount_avg_7d_full', DoubleType(), True), 
                                 StructField('term_tx_amount_cn_1d_full', LongType(), True), 
                                 StructField('term_tx_amount_avg_1d_full', DoubleType(), True), 
                                 StructField('term_tx_amount_cn_1h_full', LongType(), True), 
                                 StructField('term_tx_amount_avg_1h_full', DoubleType(), True), 
                                 StructField('term_tx_amount_cn_30d_hist', LongType(), True), 
                                 StructField('term_tx_amount_avg_30d_hist', DoubleType(), True), 
                                 StructField('term_tx_amount_std_30d_hist', DoubleType(), True), 
                                 StructField('term_tx_amount_cn_7d_hist', LongType(), True), 
                                 StructField('term_tx_amount_avg_7d_hist', DoubleType(), True), 
                                 StructField('term_tx_amount_std_7d_hist', DoubleType(), True), 
                                 StructField('term_tx_amount_cn_1d_hist', LongType(), True), 
                                 StructField('term_tx_amount_avg_1d_hist', DoubleType(), True), 
                                 StructField('term_tx_amount_std_1d_hist', DoubleType(), True), 
                                 StructField('term_tx_amount_isnew_ct_30d_hist_sum_7d_full', LongType(), True), 
                                 StructField('term_tx_amount_isnew_ct_30d_hist_sum_1d_full', LongType(), True), 
                                 StructField('term_tx_amount_isnew_ct_30d_hist_sum_1h_full', LongType(), True), 
                                 StructField('term_tx_amount_isnew_ct_7d_hist_sum_7d_full', LongType(), True), 
                                 StructField('term_tx_amount_isnew_ct_7d_hist_sum_1d_full', LongType(), True), 
                                 StructField('term_tx_amount_isnew_ct_7d_hist_sum_1h_full', LongType(), True), 
                                 StructField('fraud_tx_fraud_cn_1d_full_delay', LongType(), True), 
                                 StructField('fraud_tx_fraud_risk_1d_full_delay', DoubleType(), True), 
                                 StructField('fraud_tx_fraud_cn_7d_full_delay', LongType(), True), 
                                 StructField('fraud_tx_fraud_risk_7d_full_delay', DoubleType(), True), 
                                 StructField('fraud_tx_fraud_cn_21d_full_delay', LongType(), True), 
                                 StructField('fraud_tx_fraud_risk_21d_full_delay', DoubleType(), True), 
                                 StructField('is_night', IntegerType(), True), 
                                 StructField('is_weekend', IntegerType(), True), 
                                 StructField('is_rovn_sum', IntegerType(), True), 
                                 StructField('is_unknonw_terminal', IntegerType(), True), 
                                 StructField('is_unknonw_customer', IntegerType(), True), 
                                 StructField('hour_sin', DoubleType(), True), 
                                 StructField('hour_cos', DoubleType(), True), 
                                 StructField('day_sin', DoubleType(), True), 
                                 StructField('day_cos', DoubleType(), True), 
                                 StructField('ratio_term_tx_amount_avg_30d_hist', DoubleType(), True), 
                                 StructField('ratio_term_tx_amount_avg_7d_hist', DoubleType(), True), 
                                 StructField('ratio_term_tx_amount_std_30d_hist', DoubleType(), True), 
                                 StructField('ratio_term_tx_amount_std_7d_hist', DoubleType(), True), 
                                 StructField('ratio_tx_amount_avg_cust_30d_hist', DoubleType(), True), 
                                 StructField('ratio_tx_amount_std_cust_30d_hist', DoubleType(), True), 
                                 StructField('ratio_tx_amount_avg_cust_7d_hist', DoubleType(), True), 
                                 StructField('ratio_tx_amount_std_cust_7d_hist', DoubleType(), True), 
                                 StructField('date', DateType(), True)])

fillna_value = 0.0 

# Настройки безопасности Kafka (Yandex Cloud использует SASL_SSL)
kafka_security_options = {
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "SCRAM-SHA-512",
    "kafka.sasl.jaas.config": f"org.apache.kafka.common.security.scram.ScramLoginModule required username='{KAFKA_USER}' password='{KAFKA_PASS}';",
    "kafka.ssl.endpoint.identification.algorithm": "", # Для работы в приватной сети
    "kafka.ssl.truststore.location": CERT_PATH,
    "kafka.ssl.truststore.type": "PEM"
}

# =====================================================
# 1. Чтение из Kafka (JSON)
# =====================================================
raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC_IN)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false") 
    .options(**kafka_security_options)
    .load()
)

# Парсим JSON из бинарного поля value
parsed_stream = raw_stream.select(
    F.from_json(F.col("value").cast("string"), schema).alias("data")
).select("data.*")

# =====================================================
# 2. Очистка и Трансформация
# =====================================================
clean_stream = (
    parsed_stream
    # 1. Сначала приводим строку к типу Timestamp (обязательно для Watermark)
    #.withColumn("unix_time", F.regexp_replace("tx_datetime", "24:00:00", "23:59:59"))
    .withColumn("tx_datetime", F.col("unix_time").cast(TimestampType()))
    
    # 2. Устанавливаем "водную отметку" в 1 день.
    # Spark будет хранить в памяти transaction_id только для событий не старше 24 часов.
    .withWatermark("tx_datetime", "1 day")
    
    # 3. Удаляем дубликаты. В стриминге с Watermark в dropDuplicates 
    # рекомендуется включать колонку времени.
    .dropDuplicates(["transaction_id", "tx_datetime"])
    # 4. Остальная валидация полей
    #.withColumn("customer_id", F.when(F.col("customer_id") >= 0, F.col("customer_id")).otherwise(SPECIAL_CUSTOMER_ID))
    #.withColumn("terminal_id", F.when(F.col("terminal_id") >= 0, F.col("terminal_id")).otherwise(SPECIAL_TERMINAL_ID))
    #.withColumn("tx_fraud_scenario", F.when(F.col("tx_fraud_scenario") >= 0, F.col("tx_fraud_scenario")).otherwise(SPECIAL_SCENARIO_ID))
    #.withColumn("tx_time_seconds", F.when(F.col("tx_time_seconds") >= 0, F.col("tx_time_seconds")).otherwise(SPECIAL_TIME_SECONDS))
    #.withColumn("tx_time_days", F.when(F.col("tx_time_days") >= 0, F.col("tx_time_days")).otherwise(SPECIAL_TIME_DAYS))
)

clean_stream = clean_stream.drop("tx_datetime","date")

clean_stream = clean_stream.fillna(0)
# =====================================================
# 3. Подготовка к записи в Kafka
# =====================================================
# Kafka ожидает колонки 'key' и 'value' (в формате string/binary)

output_stream = clean_stream.select(
    F.col("transaction_id").cast("string").alias("key"),
    F.to_json(F.struct("*")).alias("value")
)

# =====================================================
# 4. Запись в Kafka
# =====================================================
query = (
    output_stream.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("topic", KAFKA_TOPIC_OUT)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .outputMode("append")
    .options(**kafka_security_options)
    .start()
)

query.awaitTermination()