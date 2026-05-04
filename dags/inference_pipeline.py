"""
DAG: data_pipeline
Description: DAG for processing data with Dataproc and PySpark.
"""

import uuid
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.settings import Session
from airflow.models import Connection, Variable
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.yandex.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocCreatePysparkJobOperator,
    DataprocDeleteClusterOperator,
    InitializationAction
)

# Общие переменные для вашего облака
YC_ZONE = Variable.get("YC_ZONE")
YC_FOLDER_ID = Variable.get("YC_FOLDER_ID")
YC_SUBNET_ID = Variable.get("YC_SUBNET_ID")
YC_SSH_PUBLIC_KEY = Variable.get("YC_SSH_PUBLIC_KEY")

# Переменные для подключения к Object Storage
S3_ENDPOINT_URL = Variable.get("S3_ENDPOINT_URL")
S3_ACCESS_KEY = Variable.get("S3_ACCESS_KEY")
S3_SECRET_KEY = Variable.get("S3_SECRET_KEY")
S3_BUCKET_NAME = Variable.get("S3_BUCKET_NAME")
#S3_INPUT_BUCKET_NAME = Variable.get("S3_INPUT_BUCKET_NAME")
S3_INPUT_DATA_BUCKET = S3_BUCKET_NAME + "/airflow/"     # YC S3 bucket for input data
S3_SRC_BUCKET = S3_BUCKET_NAME[:]                       # YC S3 bucket for pyspark source files
S3_DP_LOGS_BUCKET = S3_BUCKET_NAME + "/airflow_logs/"   # YC S3 bucket for Data Proc logs

# Переменные необходимые для создания Dataproc кластера
DP_SA_AUTH_KEY_PUBLIC_KEY = Variable.get("DP_SA_AUTH_KEY_PUBLIC_KEY")
DP_SA_JSON = Variable.get("DP_SA_JSON")
DP_SA_ID = Variable.get("DP_SA_ID")
DP_SECURITY_GROUP_ID = Variable.get("DP_SECURITY_GROUP_ID")

MLFLOW_IP = Variable.get("MLFLOW_IP")
MLFLOW_PORT = Variable.get("MLFLOW_PORT")

# для сохранения и чтения точек отсчёта следующей партии
PERSIST_BUCKET_NAME = Variable.get("PERSIST_BUCKET_NAME")
PERSIST_BUCKET_KEY = Variable.get("PERSIST_BUCKET_KEY")
PERSIST_BUCKET_SECRET = Variable.get("PERSIST_BUCKET_SECRET")

KAFKA_FQDN = Variable.get("KAFKA_FQDN")
KAFKA_USER = Variable.get("KAFKA_USER")
KAFKA_PWD = Variable.get("KAFKA_PWD")
KAFKA_FOR_ML_FRAUD_TOPIC = Variable.get("KAFKA_FOR_ML_FRAUD_TOPIC")
KAFKA_PREDICTION_TOPIC = Variable.get("KAFKA_PREDICTION_TOPIC")

KAFKA_PORT = "9091"

KAFKA_CONN = ""
if KAFKA_PORT == '9092':
    KAFKA_CONN = f'{KAFKA_FQDN}:{KAFKA_PORT}'
    KAFKA_USER = None
    KAFKA_PWD = None
else :
    KAFKA_CONN = f'{KAFKA_FQDN}:{KAFKA_PORT}:{KAFKA_USER}:{KAFKA_PWD}'

CA_CERT_PATH = '/etc/ssl/certs/yandex_ca.pem'

# Создание подключения для Object Storage
YC_S3_CONNECTION = Connection(
    conn_id="yc-s3",
    conn_type="s3",
    host=S3_ENDPOINT_URL,
    extra={
        "aws_access_key_id": S3_ACCESS_KEY,
        "aws_secret_access_key": S3_SECRET_KEY,
        "host": S3_ENDPOINT_URL,
    },
)

# Создание подключения для Dataproc
YC_SA_CONNECTION = Connection(
    conn_id="yc-sa",
    conn_type="yandexcloud",
    extra={
        "extra__yandexcloud__public_ssh_key": DP_SA_AUTH_KEY_PUBLIC_KEY,
        "extra__yandexcloud__service_account_json": DP_SA_JSON,
    },
)

# Проверка наличия подключений в Airflow
# Если подключения отсутствуют, то они добавляются
# и сохраняются в базе данных Airflow
# Подключения используются для доступа к Object Storage и Dataproc
def setup_airflow_connections(*connections: Connection) -> None:
    """
    Check and add missing connections to Airflow.

    Parameters
    ----------
    *connections : Connection
        Variable number of Airflow Connection objects to verify and add

    Returns
    -------
    None
    """
    session = Session()
    try:
        for conn in connections:
            print("Checking connection:", conn.conn_id)
            if not session.query(Connection).filter(Connection.conn_id == conn.conn_id).first():
                session.add(conn)
                print("Added connection:", conn.conn_id)
        session.commit()
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()


# Функция для выполнения setup_airflow_connections в рамках оператора
def run_setup_connections(**kwargs): # pylint: disable=unused-argument
    """Создает подключения внутри оператора"""
    setup_airflow_connections(YC_S3_CONNECTION, YC_SA_CONNECTION)
    return True


# Настройки DAG
with DAG(
    dag_id="inference",
    start_date=datetime(year=2026, month=3, day=16),
    schedule_interval=timedelta(minutes=60*24),
    catchup=False,
    max_active_runs=1
) as dag:
    # Задача для создания подключений
    setup_connections = PythonOperator(
        task_id="setup_connections",
        python_callable=run_setup_connections,
    )

    # 1 этап: создание Dataproc клаcтера
    create_spark_cluster = DataprocCreateClusterOperator(
        task_id="dp-cluster-create-task",
        folder_id=YC_FOLDER_ID,
        cluster_name=f"tmp-proc-dp-{uuid.uuid4()}",
        cluster_description="YC Spark Cluster",
        subnet_id=YC_SUBNET_ID,
        s3_bucket=S3_DP_LOGS_BUCKET,
        service_account_id=DP_SA_ID,
        ssh_public_keys=YC_SSH_PUBLIC_KEY,
        zone=YC_ZONE,
        cluster_image_version="2.1",

        # masternode
        masternode_resource_preset="s3-c8-m32",
        masternode_disk_type="network-ssd",
        masternode_disk_size=20,

        # datanodes
        datanode_resource_preset="s3-c8-m32",
        datanode_disk_type="network-ssd",
        datanode_disk_size=50,
        datanode_count=2,

        # computenodes
        computenode_count=0,

        # software
        services=["YARN", "SPARK", "HDFS", "MAPREDUCE"],
        connection_id=YC_SA_CONNECTION.conn_id,
        
        initialization_actions=[
            InitializationAction(
                uri=f"s3a://{S3_SRC_BUCKET}/src/install_ml_libs_for_inference.sh",
                args=[],         # Список аргументов командной строки для скрипта
                timeout=600      # Тайм-аут выполнения в секундах (например, 10 минут)
            )
        ],
        dag=dag,
    )
    # 2 этап: запуск задания PySpark
    poke_spark_processing = DataprocCreatePysparkJobOperator(
        task_id="dp-cluster-pyspark-task",
        main_python_file_uri=f"s3a://{S3_SRC_BUCKET}/src/inference.py",
        connection_id=YC_SA_CONNECTION.conn_id,
        args=["--in-topic", KAFKA_FOR_ML_FRAUD_TOPIC,
            "--out-topic",KAFKA_PREDICTION_TOPIC,
            "--mlflow-conn", f"http://{MLFLOW_IP}:{MLFLOW_PORT}",
            "--kafka-conn", KAFKA_CONN,
            "--ca-path",CA_CERT_PATH,
            "--pbn", PERSIST_BUCKET_NAME,
            "--pbk", PERSIST_BUCKET_KEY,
            "--pbs", PERSIST_BUCKET_SECRET
            ],
        dag=dag,
    )
    # 3 этап: удаление Dataproc кластера
    delete_spark_cluster = DataprocDeleteClusterOperator(
        task_id="dp-cluster-delete-task",
        trigger_rule=TriggerRule.ALL_DONE,
        dag=dag,
    )

    # Формирование DAG из указанных выше этапов
    # pylint: disable=pointless-statement
    setup_connections >> create_spark_cluster >> poke_spark_processing >> delete_spark_cluster
