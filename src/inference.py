import sys
import argparse
import math
import logging
import re
from datetime import datetime

#import findspark
#findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col
)

from pyspark.ml import Pipeline
from pyspark.ml.feature import  VectorAssembler
from pyspark.ml.classification import GBTClassifier
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType, TimestampType, BooleanType, DateType
from pyspark.sql.functions import from_json, struct
from pyspark.sql.functions import get_json_object

import mlflow
from mlflow.tracking import MlflowClient
import requests

import warnings
warnings.filterwarnings("ignore", category=UserWarning, module="mlflow")

import os

import boto3
import json

def parse_arguments() -> argparse.Namespace:

    parser = argparse.ArgumentParser(
        description='Скрипт инференса'
    )

    parser.add_argument(
        '--in-topic',
        type=str,
        required=True,
        help='Входящий топик в Kafka'
    )

    parser.add_argument(
        '--out-topic',
        type=str,
        required=True,
        help='Исходящий топик в Kafka'
    )

    parser.add_argument(
        '--master-conn',
        type=str,
        required=False,
        default="",
        help='подключение к мастер ноде'
    )

    parser.add_argument(
        '--mlflow-conn',
        type=str,
        required=False,
        default="http://127.0.0.1:5005",
        help='подключение к mlflow'
    )
    
    parser.add_argument(
        '--kafka-conn',
        type=str,
        required=False,
        default="localhost:9092",
        help='подключение к kafka'
    )
        
    parser.add_argument(
        '--ca-path',
        type=str,
        required=False,
        default="",
        help='cert_ca_path'
    )

    parser.add_argument(
        '--pbn',
        type=str,
        required=False,
        default="",
        help='Имя S3 бакета для работы с временными метками'
    )

    parser.add_argument(
        '--pbk',
        type=str,
        required=False,
        default="",
        help='Ключ S3 бакета для работы с временными метками'
    )

    parser.add_argument(
        '--pbs',
        type=str,
        required=False,
        default="",
        help='Секрет S3 бакета для работы с временными метками'
    )

    parser.add_argument(
        '--local',
        action='store_true',
        default=False,
        help='признак локального запуска'
    )

    return parser.parse_args()

class dates_json():
    '''класс для чтения и записи временных меток в постоянный бакет'''
    def __init__(self,
                 bucket,
                 aws_access_key_id,
                 aws_secret_access_key,
                 file_path,
                 endpoint_url='https://storage.yandexcloud.net',
                 region_name='ru-central1-a'
                ):
        
        self.bucket = bucket
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.endpoint_url = endpoint_url
        self.region_name = region_name
        self.file_path = file_path
        
        self.s3 = self.get_session()

    def get_session(self):
        session = boto3.session.Session()
        s3 = session.client(
            service_name='s3',
            endpoint_url=self.endpoint_url,
            region_name=self.region_name,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key
        )
        return s3

    def read_json(self):
        
        if self.s3_file_exists(self.bucket,self.file_path):
            
            response = self.s3.get_object(Bucket=self.bucket, Key=self.file_path)
            content = response['Body'].read().decode('utf-8')
            result_dict = json.loads(content)
            
            return result_dict
        
        else :
            return {}

   
    def s3_file_exists(self,bucket, key):
        try:
            self.s3.head_object(Bucket=bucket, Key=key)
            return True
        except Exception as e:
            return False

    def write_json(self,data_json):
        json_string = json.dumps(data_json, ensure_ascii=False)
        self.s3.put_object(
            Bucket= self.bucket,
            Key=self.file_path, 
            Body=json_string.encode('utf-8'),
            ContentType='application/json' 
        )
    
    def write_img(self,img_data):
        self.s3.put_object(
            Bucket= self.bucket,
            Key=self.file_path, 
            Body=img_data,
            ContentType='image/png' 
        )

    def get_lists(self,prefix):
        objects = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
        files = [obj['Key'] for obj in objects.get('Contents', []) if obj['Key'].endswith('.parquet')]
        return files


def get_model(model_name, flag="last"):
    client = MlflowClient()
    try:
        if flag == "best":
            # Получаем конкретную версию, на которую указывает алиас
            model_v = client.get_model_version_by_alias(model_name, "best")
            version_num = model_v.version
            model = mlflow.spark.load_model(f"models:/{model_name}@best")
            return model, version_num
        
        else: # last
            # Получаем все версии и берем самую новую
            all_versions = client.get_registered_model(model_name).latest_versions
            latest_v = max(int(v.version) for v in all_versions)
            model = mlflow.spark.load_model(f"models:/{model_name}/{latest_v}")
            return model, str(latest_v)
            
    except Exception as e:
        print(f"Ошибка при получении модели ({flag}): {e}")
        return None, None
    
def inference(
        in_topic: str,
        out_topic: str,
        master_conn: str = "", #IP:PORT
        mlflow_conn: str = "", #IP:PORT
        kafka_conn: str = "", #IP:PORT[:user:pwd]
        ca_path: bool="",
        local: bool = False,
        pbn="",
        pbk="",
        pbs=""
        ):

    # =====================================================
    # Конфигурация
    # =====================================================

    INPUT_TOPIC = in_topic
    OUTPUT_TOPIC = out_topic
    MASTER_CONN = master_conn
    MLFLOW_CONN = mlflow_conn
    CA_CERT_PATH = ca_path 
    LOCAL_RUN = local

    kafka_conn_c = kafka_conn.split(':')
    if len(kafka_conn_c) == 2:
        KAFKA_CONN = kafka_conn
        KAFKA_USER = None
        KAFKA_PWD = None
    elif len(kafka_conn_c) == 4:
        KAFKA_CONN = f'{kafka_conn_c[0]}:{kafka_conn_c[1]}'
        KAFKA_USER = kafka_conn_c[2]
        KAFKA_PWD = kafka_conn_c[3]
    else:
        raise Exception(f"Kafka conn not correct: {kafka_conn}") 
    

    PERSIST_BUCKET_NAME = pbn
    PERSIST_BUCKET_KEY = pbk
    PERSIST_BUCKET_SECRET = pbs

    CHECKPOINTS_PATH = f"s3a://{PERSIST_BUCKET_NAME}/spark/checkpoints_ml"



    # --------------------------------------------------
    # MLFLOW SETUP
    # --------------------------------------------------
    
    #os.environ["MLFLOW_S3_ENDPOINT_URL"] = "https://storage.yandexcloud.net"
    os.environ["MLFLOW_TRACKING_URI"] = MLFLOW_CONN
    mlflow.set_tracking_uri(MLFLOW_CONN)
    
    dates_json_obj = dates_json(
         bucket = PERSIST_BUCKET_NAME,
         aws_access_key_id=PERSIST_BUCKET_KEY,
         aws_secret_access_key=PERSIST_BUCKET_SECRET,
         file_path = 'inference/data.json',
         endpoint_url='https://storage.yandexcloud.net',
         region_name='ru-central1-a',
         )
  
    data_dict = {
         "KAFKA_CONN": KAFKA_CONN,
         }
    
    dates_json_obj.write_json(data_dict)

    def check_mlflow_availability(uri):
        print(f"Checking MLflow connection: {uri}")
        try:
            # Пытаемся достучаться до эндпоинта health или version
            response = requests.get(f"{uri}/health", timeout=10)
            if response.status_code != 200:
                raise Exception(f"MLflow returned status code {response.status_code}")
            print("MLflow is reachable. Proceeding...")
        except Exception as e:
            # Печатаем ошибку в stderr, чтобы она была видна в логах Airflow
            print(f"CRITICAL ERROR: MLflow server is unreachable at {uri}.", file=sys.stderr)
            print(f"Details: {e}", file=sys.stderr)
            # Завершаем процесс с ненулевым кодом выхода. 
            # Это заставит Airflow пометить задачу как Failed.
            sys.exit(1)

    
    data_dict.update({
         "check_mlflow_availability": True,
         })
    
    dates_json_obj.write_json(data_dict)

    # =====================================================
    # Создание сессии
    # =====================================================
    if 'spark' in locals() or 'spark' in globals():
        spark.stop()

    spark  = ""
    logger = ""

    if LOCAL_RUN:
        print("LOCAL RUN !!!")
        spark = (
            SparkSession.builder
                .appName("MLflow-Kafka-Inference")
                .master("local[10]")
                .config("spark.driver.memory", "16g")
                .config("spark.driver.maxResultSize", "4g")
                .config("spark.sql.execution.arrow.pyspark.enabled", "true")
                .config("spark.local.dir", "/media/rk/2TB/spark_tmp")
                # в стриминге нужно ставить столько, сколько у вас ядер.
                .config("spark.sql.shuffle.partitions", "10") \
                # настройки заставляют spark снижать скорость чтения из Kafka, если он не успевает.
                .config("spark.streaming.backpressure.enabled", "true") \
                .config("spark.streaming.kafka.maxRatePerPartition", "50") \
                # пакеты
                .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.11.1026")
                \
                # настройка доступа к s3
                .config("spark.hadoop.fs.s3a.access.key", PERSIST_BUCKET_KEY)
                .config("spark.hadoop.fs.s3a.secret.key", PERSIST_BUCKET_SECRET)
                .config("spark.hadoop.fs.s3a.endpoint", "storage.yandexcloud.net") \
                .config("spark.hadoop.fs.s3a.endpoint.region", "ru-central1-a") \
                \
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
                .getOrCreate()
        )
        #spark.conf.set('spark.sql.repl.eagerEval.enabled', True)  # to pretty print pyspark.DataFrame in jupyter

    else:
        spark = (
            SparkSession.builder
                .appName("Spark ML Learning")
                # ДРАЙВЕР (Мастер-нода: 8 CPU / 32 GB)
                # Отдаем драйверу 12 ГБ, так как он координирует Optuna и собирает метрики
                .config("spark.driver.memory", "16g")
                .config("spark.driver.maxResultSize", "4g")

                .config("spark.executor.cores", "7") # Оставляем 1 ядро системе/YARN
                .config("spark.executor.memory", "14g") # Оставляем запас для системы и Overhead
                .config("spark.executor.memoryOverhead", "4g") # Важно для GBT (внекучевая память)

                .config("spark.streaming.backpressure.enabled", "true") \
                .config("spark.streaming.kafka.maxRatePerPartition", "50") \
                # пакеты
                .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.11.1026")
                \
                # настройка доступа к s3
                .config("spark.hadoop.fs.s3a.access.key", PERSIST_BUCKET_KEY)
                .config("spark.hadoop.fs.s3a.secret.key", PERSIST_BUCKET_SECRET)
                .config("spark.hadoop.fs.s3a.endpoint", "storage.yandexcloud.net") \
                .config("spark.hadoop.fs.s3a.endpoint.region", "ru-central1") \
                .config("com.amazonaws.services.s3.enableV4", "true") \
                \
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

                .getOrCreate()
        )
        
    spark.conf.set("spark.sql.ansi.enabled", "false")
    
    logger = logging.getLogger("ModelLearning")
    logger.setLevel(logging.WARN)

    data_dict.update({
    "spark_conn": True,
    })
    dates_json_obj.write_json(data_dict)
    #sc = spark.sparkContext
    #sc.setLogLevel("WARN")

    # =====================================================
    # Параметры
    # =====================================================
  
    #минимум записей для запуска
    parquet_schema = StructType([StructField('transaction_id', LongType(), True), 
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
   
    # =====================================================
    # Чтение данных
    # =====================================================
    data_dict.update({
        "kafka_conn": f'{KAFKA_CONN}--{KAFKA_USER}--{KAFKA_PWD}',
        })
    dates_json_obj.write_json(data_dict)

    # Настройки безопасности
    kafka_options = {}
    if KAFKA_USER:
        kafka_options = {
            "kafka.bootstrap.servers": KAFKA_CONN,
            "kafka.security.protocol": "SASL_SSL",
            "kafka.sasl.mechanism": "SCRAM-SHA-512",
            # Настройка JAAS для логина и пароля
            "kafka.sasl.jaas.config": f'org.apache.kafka.common.security.scram.ScramLoginModule required username="{KAFKA_USER}" password="{KAFKA_PWD}";',
            # Настройка SSL (Truststore)
            #"kafka.ssl.truststore.location": "/абсолютный/путь/к/secrets/kafka.truststore.jks",
            #"kafka.ssl.truststore.password": "password123"
            "kafka.ssl.truststore.type": "PEM",
            #"kafka.ssl.truststore.location": CA_CERT_PATH
            "kafka.ssl.truststore.certificates": open(CA_CERT_PATH).read()
        }
        
        data_dict.update({
            "CA_CERT_PATH": CA_CERT_PATH,
            })
        dates_json_obj.write_json(data_dict)

    else:
        kafka_options = {"kafka.bootstrap.servers":KAFKA_CONN}
    
    # Получаем модель
    logger.warning("Get model")
    experiment_name = "Anti-Fraud-System_Optuna"
    model_name = f'{experiment_name}_model'

    model_best, v_best = get_model(model_name, "best")
    logger.warning(model_best)

    data_dict.update({
        "get model": True,
        })
    dates_json_obj.write_json(data_dict)
    
    # читаем поток
    raw_stream = spark.readStream \
        .format("kafka") \
        .options(**kafka_options) \
        .option("subscribe", INPUT_TOPIC) \
        .load()


    # парсим
    parsed_df = raw_stream.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), parquet_schema).alias("data")) \
        .select("data.*")

    df_for_model = parsed_df.fillna(fillna_value)
    
    predictions_df = model_best.transform(df_for_model)

    # выбираем нужные колонки
    final_df = predictions_df.select('unix_time','transaction_id','prediction','probability','tx_fraud')

    data_dict.update({
            "OUTPUT_TOPIC": OUTPUT_TOPIC,
            "CHECKPOINTS_PATH":CHECKPOINTS_PATH
            })
    dates_json_obj.write_json(data_dict)
    
    #display(final_df.printSchema())
    query = final_df.selectExpr("to_json(struct(*)) AS value") \
        .writeStream \
        .format("kafka") \
        .options(**kafka_options) \
        .option("topic", OUTPUT_TOPIC) \
        .option("checkpointLocation", "/tmp/checkpoints_ml") \
        .start()
    

    query.awaitTermination()

    spark.stop()

    return

def main() -> None:
    try:
        # Парсинг аргументов
        args = parse_arguments()

        # Логирование параметров запуска
        print("=" * 50)
        print("Запуск инференса")
        print(f"in topic: {args.in_topic}")
        print(f"out topic: {args.out_topic}")
        print(f"MLFLOW подключение: {args.mlflow_conn}")
        print(f"KAFKA подключение: {args.kafka_conn}")
        print("=" * 50)

        dates_json_obj = dates_json(
         bucket = args.pbn,
         aws_access_key_id=args.pbk,
         aws_secret_access_key=args.pbs,
         file_path = 'inference/main_data.json',
         endpoint_url='https://storage.yandexcloud.net',
         region_name='ru-central1-a',
         )
  
        data_dict = {
            "KAFKA_CONN": args.kafka_conn,
            }
        
        dates_json_obj.write_json(data_dict)

        # Запуск обработки данных
        inference(
            in_topic=args.in_topic,
            out_topic=args.out_topic,
            master_conn=args.master_conn,
            mlflow_conn=args.mlflow_conn,
            kafka_conn=args.kafka_conn,
            ca_path=args.ca_path,
            pbn=args.pbn,
            pbk=args.pbk,
            pbs=args.pbs,
            local=args.local
        )


    except Exception as e:
        
        print(f"Ошибка: {e}")
        data_dict.update({
            "ERROR": str(e),
            })
        dates_json_obj.write_json(data_dict)

        sys.exit(1)


if __name__ == "__main__":
    main()
    

#python ./scripts/inference.py --in-topic=fraud_test --out-topic=predictions --ca-path '/home/rk/projects/forHW6/kafka/secrets/ca-cert' --local