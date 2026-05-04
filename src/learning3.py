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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType, TimestampType, BooleanType

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
#from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.classification import GBTClassifier
#from pyspark.ml.feature import StandardScaler, PCA

import mlflow
from mlflow.tracking import MlflowClient
from mlflow.models import infer_signature
import json

import matplotlib
matplotlib.use('Agg') # Переключает matplotlib в режим записи в файл, без GUI
import matplotlib.pyplot as plt
import seaborn as sns

import warnings
warnings.filterwarnings("ignore", category=UserWarning, module="mlflow")

import optuna 
import gc
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.functions import vector_to_array
import boto3
import os
import requests
import sys
from pyspark.storagelevel import StorageLevel

#from common_functions import dates_json, split_train_test_by_time, get_waighted_data, generate_time_windows_by_ratio, get_fold_data

#from ml_functions import get_metrics_and_log, get_confusion_matrix

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


def parse_arguments() -> argparse.Namespace:

    parser = argparse.ArgumentParser(
        description='Скрипт для очистки данных из HDFS и сохранения в S3'
    )

    parser.add_argument(
        '--in-path',
        type=str,
        required=True,
        help='Путь к папке в HDFS с исходными данными'
    )

    parser.add_argument(
        '--out-path',
        type=str,
        required=False,
        help='Имя S3 бакета для сохранения результатов'
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
        '--log-stats',
        action='store_true',
        default=False,
        help='Включить логирование статистики по отфильтрованным данным'
    )

    parser.add_argument(
        '--local',
        action='store_true',
        default=False,
        help='признак локального запуска'
    )

    return parser.parse_args()

# ФУНКЦИИ РАЗДЕЛЕНИЯ ПРИЗНАКОВ И ВЫВОДА МЕТРИК
    
def split_train_test_by_time(df, train_ratio=0.8, time_col='tx_datetime'):
    """
    Разделяет датасет на train/test по времени без перемешивания.
    """
    # Находим минимальное и максимальное время (в секундах для точности)
    #stats = df.select(
    #    F.min(time_col).alias('min_t'),
    #    F.max(time_col).alias('max_t')
    #).collect()[0]
    
    #start_t = stats['min_t']
    #end_t = stats['max_t']
    
    # Вычисляем точку отсечения (threshold) через перцентиль
    threshold = df.stat.approxQuantile(time_col, [train_ratio], 0.01)[0]
    
    # Разделяем данные
    train_df = df.filter(F.col(time_col) <= threshold)
    test_df = df.filter(F.col(time_col) > threshold)
    
    return train_df, test_df
    
def generate_time_windows_by_ratio(df, time_col, n_folds, val_ratio,gap=1):

    # сколько "долей"
    total_parts = int((1 / val_ratio - 1) + n_folds) + gap

    # квантили для разбиения
    quantiles = [i / total_parts for i in range(1, total_parts)]
    split_points = df.approxQuantile(time_col, quantiles, 0.001)

    # добавляем границы
    stats = df.select(
        F.min(time_col).alias('min_t'),
        F.max(time_col).alias('max_t')
    ).collect()[0]

    min_time = stats['min_t']
    max_time = stats['max_t']

    points = [min_time] + split_points + [max_time]

    windows = []

    # размер train в "долях"
    train_parts = int(1 / val_ratio - 1)

    for i in range(n_folds):
        
        train_start = points[i]
        train_end = points[i + train_parts]

        val_start = points[i + train_parts + gap]
        val_end = points[i + train_parts + 1 + gap]

        windows.append((
            int(train_start),
            int(train_end),
            int(val_start),
            int(val_end)
        ))

    return windows

def get_fold_data(df, window, time_col="unix_time"):
    train_start, train_end, val_start, val_end = window

    train = df.filter(
        (F.col(time_col) >= train_start) &
        (F.col(time_col) < train_end)
    )

    val = df.filter(
        (F.col(time_col) >= val_start) &
        (F.col(time_col) < val_end)
    )

    return train, val


def get_waighted_data(df,eval_col="tx_fraud"):
    
    # Считаем количество строк каждого класса
    counts = df.groupBy(eval_col).count().collect()
    dict_counts = {row[eval_col]: row["count"] for row in counts}

    # Рассчитываем коэффициент (Negative / Positive)
    ratio = round(dict_counts[False] / dict_counts[True])
    #print(ratio)
    # Создаем колонку с весами: 
    # Если фрод — вес равен ratio, если нет — вес равен 1
    train_weighted = df.withColumn("weight", F.when(F.col(eval_col) == True, ratio).otherwise(1.0))

    return train_weighted


def get_metrics_and_log(predictions,prefix=''):
    
    metrics_raw = predictions.select(
        F.sum(F.when((F.col("prediction") == 1) & (F.col("tx_fraud") == 1), 1).otherwise(0)).alias("TP"),
        F.sum(F.when((F.col("prediction") == 1) & (F.col("tx_fraud") == 0), 1).otherwise(0)).alias("FP"),
        F.sum(F.when((F.col("prediction") == 0) & (F.col("tx_fraud") == 1), 1).otherwise(0)).alias("FN"),
        F.min("unix_time").alias("first_tx"),
        F.max("unix_time").alias("last_tx") 
    ).collect()[0]
    
    tp, fp, fn, first_tx, last_tx = metrics_raw["TP"], metrics_raw["FP"], metrics_raw["FN"], metrics_raw['first_tx'], metrics_raw['last_tx']
    
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0
    f1 = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0
    
    total_weeks = (last_tx - first_tx) / (86400*7)
    print(f'total_weeks: {total_weeks}, first_tx:, {datetime.fromtimestamp(first_tx)}, : {datetime.fromtimestamp(last_tx)}')
    avg_fp_per_week = fp / total_weeks if total_weeks > 0 else 0

    # Логируем метрики в MLflow
    mlflow.log_metrics({
        f"{prefix}precision": precision,
        f"{prefix}recall": recall,
        f"{prefix}f1_score": f1,
        f"{prefix}avg_fp_per_week": avg_fp_per_week
    })
    
    #print(f"Metrics logged: F1={f1:.4f}, FP/week={avg_fp_per_week:.2f}")
    return precision, recall, f1, avg_fp_per_week

def get_confusion_matrix(predictions,file_name="confusion_matrix.png"):
    
    #Логируем Confusion Matrix как рисунок
    conf_matrix = predictions.groupBy("tx_fraud", "prediction").count().toPandas()
    matrix_df = conf_matrix.pivot(index='tx_fraud', columns='prediction', values='count').fillna(0)

    plt.figure(figsize=(8, 6))
    sns.heatmap(matrix_df, annot=True, fmt='g', cmap='Blues')
    plt.xlabel('Предсказание')
    plt.ylabel('Реальность (tx_fraud)')
    plt.title('Confusion Matrix')
    
    # Сохраняем во временный файл и отправляем в MLflow
    temp_plot_path = file_name
    plt.savefig(temp_plot_path)
    mlflow.log_artifact(temp_plot_path)
    plt.close() # закрываем фигуру, чтобы не дублировать в notebook


def learning(
        in_path: str,
        out_path: str,
        master_conn: str = "", #IP:PORT
        mlflow_conn: str = "", #IP:PORT
        log_stats: bool = False,
        local: bool = False,
        pbn="",
        pbk="",
        pbs=""):

    # =====================================================
    # Конфигурация
    # =====================================================
    INPUT_PATH = in_path
    OUTPUT_PATH = out_path
    MASTER_CONN = master_conn
    MLFLOW_CONN = mlflow_conn
    LOG = log_stats
    LOCAL_RUN = local

    # для сохранения и чтения точек отсчёта следующей партии
    PERSIST_BUCKET_NAME = pbn
    PERSIST_BUCKET_KEY = pbk
    PERSIST_BUCKET_SECRET = pbs

    SPECIAL_TERMINAL_ID = -1
    SPECIAL_CUSTOMER_ID = -1
    SPECIAL_SCENARIO_ID = -1
    SPECIAL_TIME_SECONDS = -1
    SPECIAL_TIME_DAYS = -1
    

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
        file_path = 'learning/data.json',
        endpoint_url='https://storage.yandexcloud.net',
        region_name='ru-central1-a',
        )
  
    data_dict = {
        "mlflow_conn": MLFLOW_CONN,
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

    # Вызов проверки
    check_mlflow_availability(MLFLOW_CONN)
    
    #os.environ["MLFLOW_S3_ENDPOINT_URL"] = "https://storage.yandexcloud.net"
    #mlflow.set_tracking_uri(MLFLOW_CONN)

    #инициализация клиента, подключаемся к уже существующему эксперименту, если он есть
    client = MlflowClient()
    experiment_name = f"Anti-Fraud-System_Optuna"
    experiment = client.get_experiment_by_name(experiment_name)
    if experiment and not isinstance(experiment,str):
        experiment_id = experiment.experiment_id
    else:
        experiment = client.create_experiment(experiment_name)
        experiment = client.get_experiment_by_name(experiment_name)
        experiment_id = experiment.experiment_id
    
    if not experiment_id:
        raise RuntimeError("Experiment not initialized")
    
    #mlflow.set_experiment(experiment_name)

    data_dict.update({
        "experiment_id": experiment_id,
        "experiment_name": experiment_name
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
                .appName("Spark ML Learning")
                .master("local[14]")
                .config("spark.driver.memory", "24g")
                .config("spark.driver.maxResultSize", "4g")
                .config("spark.memory.fraction", "0.6") # 60% памяти под вычисления
                .config("spark.memory.storageFraction", "0.5")
                #.config("spark.memory.offHeap.enabled", "true")
                #.config("spark.memory.offHeap.size", "10g") # Явно выделяем место под нативную память
                #.config("spark.executor.memory", "16g")
                #.config("spark.default.parallelism", "6")
                #.config("spark.sql.shuffle.partitions", "16")
                #.config("spark.driver.maxResultSize", "4g")
                .config("spark.local.dir", "/media/rk/2TB/spark_tmp")
                # Адаптивность
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") # Склеивать мелкие части
                .config("spark.sql.adaptive.skewJoin.enabled", "true")           # Обработка перекосов данных
                .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128mb") # целевой размер раздела при склейке
                #.config("spark.jars.packages", "ml.dmlc:xgboost4j-spark_2.12:1.4.1")
                #.config("spark.driver.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true")
                #.config("spark.jars.packages","com.microsoft.azure:synapseml_2.12:0.11.1")
                #.config("spark.sql.execution.arrow.pyspark.enabled", "true")
                #.config("spark.jars.excludes", "io.netty:netty-transport-native-kqueue,io.netty:netty-resolver-dns-native-macos")
                #.config("spark.driver.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true") 
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

                # ИСПОЛНИТЕЛИ (2 воркера по 8 CPU / 32 GB)
                # На каждом узле запускаем по одному "толстому" исполнителю
                #.config("spark.executor.instances", "2") 
                .config("spark.executor.cores", "7") # Оставляем 1 ядро системе/YARN
                .config("spark.executor.memory", "18g") # Оставляем запас для системы и Overhead
                .config("spark.executor.memoryOverhead", "4g") # Важно для GBT (внекучевая память)

                # ПАРАЛЛЕЛИЗМ
                # Оптимально: кол-во ядер * (2 или 3) -> 14 ядер * 3 = 42
                #.config("spark.sql.shuffle.partitions", "42")
                #.config("spark.default.parallelism", "42")

                # ПАМЯТЬ И КЭШИРОВАНИЕ
                # Увеличиваем долю памяти под кэш данных (Storage), чтобы 12 ГБ влезли в RAM
                #.config("spark.memory.fraction", "0.8") 
                #.config("spark.storage.memoryFraction", "0.5")

                # АДАПТИВНОСТЬ (AQE) - оставляем включенной
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128mb")

                .getOrCreate()
        )
        
    spark.conf.set("spark.sql.ansi.enabled", "false")

    logger = logging.getLogger("ModelLearning")
    logger.setLevel(logging.WARN)

    #sc = spark.sparkContext
    #sc.setLogLevel("WARN")

    # =====================================================
    # Параметры
    # =====================================================
  
    #минимум записей для запуска
    learn_period = 3600*24*7
    # сколько данных брать от последнего сохранённого timestamp (для проекта, чтобы каждый запуск добавлял день данных)
    min_predict_period = 3600*24
    max_predict_period = 3600*24*2
    train_ratio = 0.8
    fillna_value = 0 


    # =====================================================
    # Чтение данных
    # =====================================================

    df = (
        spark.read
        .parquet(INPUT_PATH)
        .sample(fraction=0.1, seed=42)
        .persist(StorageLevel.MEMORY_AND_DISK)
        )

    #=====================================================
    # Смотрим с какого времени начать
    #=====================================================

    #min_time = df.agg(F.min(F.col("unix_time"))).collect()[0][0]
    #max_time = df.agg(F.max(F.col("unix_time"))).collect()[0][0]
    stats = df.select(F.min("unix_time"), F.max("unix_time")).collect()[0]
    min_time, max_time = stats[0], stats[1]

    logger.warning(datetime.fromtimestamp(min_time))
    logger.warning(datetime.fromtimestamp(max_time))

    
    # если данных недостаточно для обучения (меньше периода максимального окна)
    if max_time - min_time < learn_period + min_predict_period:
        logger.warning(f"Not enough data to start processing: {max_time - min_time} seconds with a min limit of {learn_period + min_predict_period}")
        spark.stop()
        return
    else:
        if max_time - min_time < learn_period + max_predict_period:
            predict_period = min_predict_period
        else:
            predict_period = max_predict_period
            
        start_time = max_time - learn_period - predict_period
        end_time = max_time

    logger.warning(f'start_time: {datetime.fromtimestamp(start_time)}, end_time: {datetime.fromtimestamp(end_time)}, learn_period: {learn_period}, predict_period: {predict_period}, \
    train_ratio: {(train_ratio)}, fillna_value: {fillna_value}')

    df = df.filter(
        (F.col("unix_time") >= start_time) 
        )
    
      
    #заполняем null значения
    df = df.fillna(fillna_value)

   
    # =====================================================
    # Разделение даных  train/test, добавление весов
    # =====================================================

    # разделяем на тренировочный и тестовый датасеты
    train, test = split_train_test_by_time(df, train_ratio=train_ratio, time_col="unix_time")
    if LOG:
        logger.warning(f"Train count: {train.count()}")
        logger.warning(f"Test count: {test.count()}")

    #считаем и добавляем в датасет веса
    train_weighted = get_waighted_data(train,eval_col="tx_fraud")
    #train_weighted = train
    #train_weighted = train_weighted.repartition(140)

    #a = train_weighted.limit(1)
    #logger.warning(str(a))

    # =====================================================
    # Подготовка вектора признаков и инициализация модели
    # =====================================================
    
    #определяем признаки на удаление:
    to_remove_cols = [  'terminal_id',
                        'transaction_id',
                        'tx_datetime',
                        'customer_id',
                        'tx_time_seconds',
                        'tx_time_days',
                        'tx_fraud',
                        'tx_fraud_scenario',
                        'unix_time',
                        'hour',
                        'day_of_week',
                        'date'
                    ]
    
    num_cols = [column for column in df.columns if column not in to_remove_cols]

    # Собираем всё в один вектор (формат для Spark ML)
    assembler = VectorAssembler(
        inputCols=num_cols ,#+ [f"{c}_vec" for c in cat_cols],
        outputCol="features"
    )
    
    # --------------------------------------------------
    # WINDOWS FOR CV
    # --------------------------------------------------

    windows = generate_time_windows_by_ratio(
        train_weighted,
        time_col="unix_time",
        n_folds=3,
        val_ratio = 0.2,
        gap=0
    )


    # --------------------------------------------------
    # OPTUNA LEARNING
    # --------------------------------------------------
    
    mlflow.pyspark.ml.autolog(log_models=False)
    
    # Формируем имя запуска
    runs = client.search_runs(experiment_ids=[experiment_id],order_by=["metrics.test_f1_score DESC"])
    run_number = len(runs) + 1  # Номер текущего запуска
    current_run_name = f"Fraud_Detection_v_{run_number}"

    data_dict.update({
        "run_number": run_number,
        "current_run_name": current_run_name
        })
    
    dates_json_obj.write_json(data_dict)

    train_assembled = assembler.transform(train_weighted).select("features", "tx_fraud", "weight", "unix_time")
    train_assembled.persist(StorageLevel.MEMORY_AND_DISK)
    train_assembled.count() 

    with mlflow.start_run(
        experiment_id=experiment_id,
        run_name=current_run_name + "_Optuna_TimeCV"
    ) as parent_run:

        def objective(trial):

            with mlflow.start_run(experiment_id=experiment_id,
                                run_name=f"trial_{trial.number}", 
                                nested=True, 
                                parent_run_id=parent_run.info.run_id) as trial_run:
        
                params = {
                    "maxDepth": trial.suggest_int("maxDepth", 3, 8),
                    "maxIter": trial.suggest_int("maxIter", 30, 80),
                    "stepSize": trial.suggest_float("stepSize", 0.05, 0.2),
                }
        
                mlflow.log_params(params)
                
                evaluator = MulticlassClassificationEvaluator(
                    labelCol="tx_fraud",
                    predictionCol="prediction",
                    metricName="f1"
                )
                
                f1_scores = []
        
                for window in windows:
        
                    train_fold, val_fold = get_fold_data(train_assembled, window)
                    #train_fold = train_fold.fillna(fillna_value)
                    #val_fold = val_fold.fillna(fillna_value)
        
                    gbt = GBTClassifier(
                        labelCol="tx_fraud",
                        featuresCol="features",
                        weightCol="weight",
                        seed=42,
                        **params
                    )
                    
                    model = gbt.fit(train_fold)
        
                    preds = model.transform(val_fold).select("prediction", "tx_fraud")
        
                    f1 = evaluator.evaluate(preds)
        
                    f1_scores.append(f1)
        
                mean_f1 = sum(f1_scores) / len(f1_scores)
        
                mlflow.log_metric("mean_f1", mean_f1)

                _ = gc.collect()
                # очистка Spark
                spark.sparkContext.cancelAllJobs() # отменяет висячие jobs, если есть
                
            return mean_f1
            
        study = optuna.create_study(direction="maximize")

        study.optimize(objective, n_trials=5)

        best_params = study.best_params
        best_score = study.best_value

        mlflow.log_params(best_params)
        mlflow.log_metric("best_f1", best_score)
    
    # =====================================================
    # Финальное обучение, подсчёт метрик и сохранение модели
    # =====================================================
    
    # best_params = {'maxDepth':5, 
    #                'maxIter':75,
    #                'stepSize': 0.12935632392466073}

    best_gbt = GBTClassifier(
        labelCol="tx_fraud",
        featuresCol="features",
        weightCol="weight",
        seed=42,
        **best_params
        )

    with mlflow.start_run(
        experiment_id=experiment_id,
        run_name=current_run_name + "_final"
    ) as run:
        
        # Логируем список признаков (как артефакт)

        features_config = {"num_cols": num_cols}
        with open("features.json", "w") as f:
            json.dump(features_config, f)
        
        mlflow.log_artifact("features.json")
        mlflow.log_param("fillna_value", fillna_value)

        # Обучение
        pipeline = Pipeline(stages=[assembler, best_gbt])
        model = pipeline.fit(train_weighted)

        # Оценка на тренировочном наборе
        # Получение предсказаний 
        predictions = model.transform(train)
        #подсчёт метрик
        precision, recall, f1, avg_fp_per_week = get_metrics_and_log(predictions,prefix='train_')

        if LOG:
            logger.info(f'''precision: {precision}, 
                        recall: {recall}, 
                        f1: {f1}, 
                        avg_fp_per_week: {avg_fp_per_week}''') 
        
        # Оценка на тестовом наборе
        # Получение предсказаний 
        predictions = model.transform(test)
        #подсчёт метрик
        precision, recall, f1, avg_fp_per_week = get_metrics_and_log(predictions,prefix='test_')

        if LOG:
            logger.info(f'''precision: {precision}, 
                        recall: {recall}, 
                        f1: {f1}, 
                        avg_fp_per_week: {avg_fp_per_week}''') 
            
        get_confusion_matrix(predictions,file_name="confusion_matrix.png")
        get_confusion_matrix(predictions.filter(vector_to_array("probability")[1]>=0.9),file_name="confusion_matrix_09.png")
        
        # Создаем сигнатуру
        input_example = train_weighted.select(num_cols).limit(5).toPandas()
        prediction_sample = model.transform(train_weighted.limit(5)).select("prediction").toPandas()
        signature = infer_signature(input_example, prediction_sample)

        # Записываем модель
        model_name = f'{experiment_name}_model'
        
        model_info = mlflow.spark.log_model(
            spark_model=model, 
            artifact_path="gb_model",
            signature=signature,
            pip_requirements=["pandas==2.3.3", "pyspark==3.3.2"],
            registered_model_name = model_name ) 
        
        new_version = model_info.registered_model_version
        
        print(f"Зарегистрирована версия №: {new_version}")

        # проверяем алиас и назначаем, если его нет
        try:
            client.get_model_version_by_alias(model_name, "best")
        except mlflow.exceptions.RestException:
            client.set_registered_model_alias(model_name, "best", new_version)
            print(f"Алиас 'best' назначен версии {new_version}")
            
        if LOG:
            logger.info(f"Run finished. ID: {run.info.run_id}")


    spark.stop()

    return

def main() -> None:
    try:
        # Парсинг аргументов
        args = parse_arguments()

        # Логирование параметров запуска
        print("=" * 50)
        print("Запуск скрипта обучения")
        print(f"in path: {args.in_path}")
        print(f"MLFLOW подключение: {args.mlflow_conn}")
        print(f"Логирование: {'Включено' if args.log_stats else 'Выключено'}")
        print(f"Локальный запуск: {'Включено' if args.local else 'Выключено'}")
        print("=" * 50)

        # Запуск обработки данных
        learning(
            in_path=args.in_path,
            out_path=args.out_path,
            master_conn=args.master_conn,
            mlflow_conn=args.mlflow_conn,
            log_stats=args.log_stats,
            local=args.local,
            pbn=args.pbn,
            pbk=args.pbk,
            pbs=args.pbs
        )


    except Exception as e:
        print(f"Ошибка: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

#python ./scripts/learning3.py --in-path=./out_folder_for_ml --log-stats --local &> output.log