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

import mlflow
from mlflow.tracking import MlflowClient
import json

import matplotlib
matplotlib.use('Agg') # Переключает matplotlib в режим записи в файл, без GUI
import matplotlib.pyplot as plt
import seaborn as sns

import warnings
warnings.filterwarnings("ignore", category=UserWarning, module="mlflow")

from common_functions import dates_json, split_train_test_by_time
from ml_functions import  evaluate_model, bootstrap_metrics_spark, statistical_comparison
import os
from pyspark.storagelevel import StorageLevel
import requests
import numpy as np

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

def get_model(model_name, client, flag="last",logger=None):

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
        logger.error(f"Ошибка при получении модели ({flag}): {e}")
        return None, None
    
def change_aliaces(model_name,new_version,old_version,client,logger=None):
    client.set_registered_model_alias(model_name, "old_best", old_version)
    client.set_registered_model_alias(model_name, "best", new_version)
    logger.warning(f"Алиасы обновлены: v{old_version} -> old_best, v{new_version} -> best")

def ab_test_models(
    production_model,
    candidate_model,
    test_df,
    bootstrap_iterations=100,
    alpha=0.01,
    logger=None
):
    """
    A/B тестирование двух моделей

    Parameters
    ----------
    production_model : spark estimator
        Текущая производственная модель
    candidate_model : spark estimator
        Модель-кандидат
    test_df : df
        Тестовые метки
    bootstrap_iterations : int, default=100
        Количество итераций bootstrap
    alpha : float, default=0.01
        Уровень значимости

    Returns
    -------
    results : dict
        Словарь с результатами сравнения
    """
    logger.warning("\nA/B ТЕСТИРОВАНИЕ МОДЕЛЕЙ")
    logger.warning("=" * 50)

   # Оценка производственной модели
    logger.warning("Оценка производственной модели...")
    prod_metrics, prod_predictions = evaluate_model(production_model,test_df,target_col='tx_fraud')
    
    logger.warning("Метрики производственной модели:")
    for metric_name, value in prod_metrics.items():
        logger.warning(f"  {metric_name}: {value:.4f}")
    
    # Оценка модели-кандидата
    logger.warning("\nОценка модели-кандидата...")
    cand_metrics, cand_predictions = evaluate_model(candidate_model,test_df,target_col='tx_fraud')
    
    logger.warning("Метрики модели-кандидата:")
    for metric_name, value in cand_metrics.items():
        logger.warning(f"  {metric_name}: {value:.4f}")

    # Bootstrap анализ
    logger.warning(f"\nBootstrap анализ ({100} итераций)...")
    
    logger.warning("Bootstrap для производственной модели...")
    prod_bootstrap = bootstrap_metrics_spark(prod_predictions,n_iterations=bootstrap_iterations, seed=42)
    
    logger.warning("Bootstrap для модели-кандидата...")
    cand_bootstrap = bootstrap_metrics_spark(cand_predictions,n_iterations=bootstrap_iterations, seed=42)
    
    # Статистическое сравнение
    logger.warning(f"\nСтатистическое сравнение (α = {alpha})...")
    comparison_results = statistical_comparison(prod_bootstrap, cand_bootstrap, alpha)
    
    logger.warning("\nРезультаты t-теста:")
    for metric, results in comparison_results.items():
        logger.warning(f"\n{metric}-score:")
        logger.warning(f"  Production: {results['base_mean']:.4f}")
        logger.warning(f"  Candidate:  {results['candidate_mean']:.4f}")
        logger.warning(f"  Улучшение:  {results['improvement']:+.4f}")
        logger.warning(f"  p-value:    {results['p_value']:.6f}")
        logger.warning(f"  Cohen's d:  {results['effect_size']:.4f}")
    
        if results["is_significant"]:
            logger.warning(f"  ЗНАЧИМОЕ улучшение при α={alpha}")
        else:
            logger.warning(f"  Незначимое различие при α={alpha}")

    # Общее решение
    f1_significant = comparison_results["F1"]["is_significant"]
    f1_improvement = comparison_results["F1"]["improvement"] > 0
    
    should_deploy = f1_significant and f1_improvement
    
    logger.warning(f"\n{'='*50}")
    logger.warning(f"ИТОГОВОЕ РЕШЕНИЕ:")
    if should_deploy:
        logger.warning("РАЗВЕРНУТЬ новую модель в Production")
        logger.warning("   Модель-кандидат показала статистически значимое улучшение")
    else:
        logger.warning("ОСТАВИТЬ текущую модель в Production")
        if not f1_improvement:
            logger.warning("   Модель-кандидат не показала улучшения")
        else:
            logger.warning("   Улучшение статистически незначимо")
    logger.warning(f"{'='*50}")

    return {
        "should_deploy": should_deploy,
        "production_metrics": prod_metrics,
        "candidate_metrics": cand_metrics,
        "comparison_results": comparison_results,
        "production_bootstrap": prod_bootstrap,
        "candidate_bootstrap": cand_bootstrap,
    }

def ab_test(
        in_path: str,
        out_path: str,
        master_conn: str = "", #IP:PORT
        mlflow_conn: str = "", #IP:PORT
        log_stats: bool = False,
        local: bool = False):

    # =====================================================
    # Конфигурация
    # =====================================================
    INPUT_PATH = in_path
    OUTPUT_PATH = out_path
    MASTER_CONN = master_conn
    MLFLOW_CONN = mlflow_conn
    LOG = log_stats
    LOCAL_RUN = local

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

    #инициализация клиента для сохранения результата
    client = MlflowClient()
    experiment_name = f"Anti-Fraud-System_Optuna_AB_test"
    experiment = client.get_experiment_by_name(experiment_name)
    if experiment and not isinstance(experiment,str):
        experiment_id = experiment.experiment_id
    else:
        experiment = client.create_experiment(experiment_name)
        experiment = client.get_experiment_by_name(experiment_name)
        experiment_id = experiment.experiment_id
    
    if not experiment_id:
        raise RuntimeError("Experiment not initialized")
    

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
        #test_start_time = max_time - min_predict_period
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


   
    # --------------------------------------------------
    # GET MODELS
    # --------------------------------------------------
    
    experiment_name = "Anti-Fraud-System_Optuna"

    model_name = f'{experiment_name}_model'
   
    model_best, v_best = get_model(model_name, client,"best",logger=logger)
    model_last, v_last = get_model(model_name, client,"last",logger=logger)

    if model_best and model_last:
        if v_best == v_last:
            logger.warning(f"Сравнение отменено: последняя версия ({v_last}) уже помечена как 'best'.")
            spark.stop()
            return
        else:
            logger.warning(f"Сравниваются модели версий: 'best' (v{v_best}) vs 'last' (v{v_last})")
            # Здесь ваш код Bootstrap
    else:
        logger.warning("Недостаточно моделей для сравнения (одна из них отсутствует).")
        spark.stop()
        return


    # --------------------------------------------------
    # AB TEST
    # --------------------------------------------------
    test.cache()
    results = ab_test_models(
        model_best,
        model_last,
        test,
        bootstrap_iterations=100,
        alpha=0.01,
        logger = logger
    )
    test.unpersist()

    save_results = {k:results[k] for k in ['should_deploy', 'production_metrics', 'candidate_metrics', 'comparison_results']}
    
    def log_dict_to_metrics(d, prefix="",run_id=None):
        """Рекурсивно проходит по словарю и логирует числовые значения как метрики"""
        for key, value in d.items():
            # Формируем имя метрики (например, candidate_metrics_f1_score)
            metric_name = f"{prefix}_{key}" if prefix else key
            
            # Преобразуем numpy-типы в стандартные
            if isinstance(value, (np.float64, np.float32, float, int)):
                if run_id:
                    mlflow.log_metric(metric_name, float(value),run_id=run_id)
                else:
                    mlflow.log_metric(metric_name, float(value))

            elif isinstance(value, (np.bool_, bool)):
                # Метрики в MLflow только числовые, поэтому bool пишем как 1 или 0
                if run_id:
                    mlflow.log_metric(metric_name, 1.0 if value else 0.0,run_id=run_id)
                else:
                    mlflow.log_metric(metric_name, 1.0 if value else 0.0)

            elif isinstance(value, dict):
                # Если внутри словарь, идем глубже
                log_dict_to_metrics(value, prefix=metric_name,run_id=run_id)

    # Формируем имя запуска для сохраненния результатов
    runs = client.search_runs(experiment_ids=[experiment_id])
    run_number = len(runs) + 1  # Номер текущего запуска
    current_run_name = f"AB_Test_Run_{run_number}"

    with mlflow.start_run(
        experiment_id=experiment_id,
        run_name=current_run_name
    ) as run:
        
        run_id = run.info.run_id

        log_dict_to_metrics(save_results,run_id=run_id)

        logger.info(f"Run finished. ID: {run.info.run_id}")

    # меняем алиас если новая модель лучше
    if results['should_deploy']:
        change_aliaces(model_name,v_last,v_best,client,logger=logger)

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
        ab_test(
            in_path=args.in_path,
            out_path=args.out_path,
            master_conn=args.master_conn,
            mlflow_conn=args.mlflow_conn,
            log_stats=args.log_stats,
            local=args.local
        )


    except Exception as e:
        print(f"Ошибка: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

#python ./scripts/ab_test.py --in-path=./out_folder_for_ml --log-stats --local &> output.log