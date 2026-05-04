import sys
import argparse
import math
import logging

#import findspark
#findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType, TimestampType, BooleanType
from pyspark.sql import Window
from pyspark.storagelevel import StorageLevel

import boto3
import json
from datetime import datetime
from common_functions import get_coalesce_number, dates_json

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
        required=True,
        help='Имя S3 бакета для сохранения результатов'
    )

    parser.add_argument(
        '--master-conn',
        type=str,
        required=False,
        default="",
        help='Имя S3 бакета для сохранения результатов'
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
        help='Включить логирование статистики по отфильтрованным данным'
    )

    return parser.parse_args()

def clear_data(
        in_path: str,
        out_path: str,
        master_conn: str = "", #IP:PORT
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


    # =====================================================
    # Создание сессии
    # =====================================================
    if 'spark' in locals() or 'spark' in globals():
        spark.stop()

    spark  = ""
    logger = ""

    if LOCAL_RUN:
        spark = (
            SparkSession.builder
                .appName("Spark ML Clean Data")
                .master("local[14]") \
                .config("spark.driver.memory", "16g") \
                .config("spark.sql.shuffle.partitions", "28") \
                .config("spark.driver.maxResultSize", "4g") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.local.dir", "/media/rk/2TB/spark_tmp") \
                .getOrCreate()
        )

    #spark.conf.set('spark.sql.repl.eagerEval.enabled', True)  # to pretty print pyspark.DataFrame in jupyter

    else:
        spark = (
            SparkSession.builder
                .appName("Spark ML Clean Data")
                #.master(f"spark://{MASTER_CONN}")
                #.config("spark.executor.instances", "3")
                #.config("spark.executor.cores", "3")
                #.config("spark.executor.memory", "10g")
                #.config("spark.executor.memoryOverhead", "2g")
                .config("spark.driver.memory", "12g")
                .config("spark.driver.cores", "3")
                #.config("spark.sql.shuffle.partitions", "72")
                #.config("spark.sql.files.maxPartitionBytes", "256m")
                .config("spark.sql.adaptive.enabled", "true")
                .getOrCreate()
        )

    spark.conf.set("spark.sql.ansi.enabled", "false")

    logger = logging.getLogger("DataQuality")
    logger.setLevel(logging.WARN)


    #вычисляем максимальное количество выходных файлов
    #n_out = get_coalesce_number(spark,logger,INPUT_PATH, target_size_mb=512,zip_coeff=3)
    
    # =====================================================
    # Чтение файлов для временных отсчётов
    # =====================================================

    # размер пересечения с предыдущей порцией данных для предотвращения дублей
    overlap = 1e6
    #минимум записей для запуска
    record_limit = 1e6
    # сколько данных брать от последнего сохранённого (для проекта, чтобы каждый запуск добалял день данных)
    batch_size = 0
    # первая порция данных
    first_batch = None

    dates_json_obj = dates_json(
        bucket = PERSIST_BUCKET_NAME,
        aws_access_key_id=PERSIST_BUCKET_KEY,
        aws_secret_access_key=PERSIST_BUCKET_SECRET,
        file_path = 'clean_data_dates/data.json',
        endpoint_url='https://storage.yandexcloud.net',
        region_name='ru-central1-a',
    )

    #считываем последний обработанный id транзакции
    dates_dict = dates_json_obj.read_json()
    
    old_end_datetime_str = 'old_end_datetime'

    if len(dates_dict) > 0:
        old_end_id = dates_json_obj.read_json()[old_end_datetime_str]      
    else:
        old_end_id = 0
        overlap = 0
    
    # =====================================================
    # Чтение данных
    # =====================================================
    schema = StructType([
        StructField("transaction_id", LongType(), True),
        StructField("tx_datetime", StringType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("terminal_id", IntegerType(), True),
        StructField("tx_amount", DoubleType(), True),
        StructField("tx_time_seconds", IntegerType(), True),
        StructField("tx_time_days", IntegerType(), True),
        StructField("tx_fraud", IntegerType(), True),
        StructField("tx_fraud_scenario", IntegerType(), True),
        StructField("_corrupt", StringType(), True)
    ])

    df_raw = (
        spark.read
        .option("header", False)
        .option("comment", "#")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt")
        .schema(schema)
        .csv(INPUT_PATH)
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    


    #=====================================================
    # Смотрим с какого времени начать
    #=====================================================

    min_id = df_raw.agg(F.min("transaction_id")).collect()[0][0]
    max_id = df_raw.agg(F.max(F.col("transaction_id"))).collect()[0][0]

    if old_end_id > 0:
        if max_id - old_end_id >= record_limit:
            start_id = old_end_id - overlap
            if batch_size > 0:
                end_id = min(old_end_id + batch_size,
                             max_id)
            else:
                end_id = max_id
        else:
            # дложно быть по крайней record_limit данных, чтобы процесс запустился
            logger.warning(f"Not enough data to start processing: {max_id - old_end_id} seconds with a limit of {record_limit}")
            spark.stop()
            return 
    else:
        start_id = min_id
        if first_batch:
            end_id = min_id + first_batch
        else:
            end_id = max_id

    logger.warning(f'''start_id: {start_id}, end_id: {end_id}, record_limit: {record_limit}, batch_size: {batch_size}, first_batch: {(not first_batch==None)}, overlap: {overlap}''')
    

    df_raw = df_raw.filter(
        (F.col("transaction_id")>= start_id) 
        & (F.col("transaction_id") < end_id)
        )

    # =====================================================
    #  Считаем corrupt строки
    # =====================================================
    if LOG:
        dq_corrupt = df_raw.select(
            F.count("*").alias("total_rows"),
            F.sum(F.when(F.col("_corrupt").isNotNull(), 1).otherwise(0)).alias("corrupt_rows")
        )
    #df.limit(5)

    # =====================================================
    # Предобработка данных
    # =====================================================

    # Удаляем битые строки
    df = df_raw.filter(F.col("_corrupt").isNull()).drop("_corrupt")

    # Дубликаты
    window = Window.partitionBy("transaction_id").orderBy("transaction_id")
    df = df.withColumn("rn", F.row_number().over(window))

    if overlap > 0:
        df = df_raw = df_raw.filter(
            (F.col("transaction_id") >= start_id + overlap) 
            )

    # tx_datetime
    df = (
        df.withColumn(
            "tx_datetime",
            F.regexp_replace("tx_datetime", "24:00:00", "23:59:59")
        )
        .withColumn("tx_datetime", F.col("tx_datetime").cast(TimestampType()))
    )

    # customer_id
    df = df.withColumn(
        "customer_id",
        F.when(
            (F.col("customer_id") >= 0) & F.col("customer_id").isNotNull(),
            F.col("customer_id")
        ).otherwise(SPECIAL_CUSTOMER_ID)
    )

    # terminal_id
    df = df.withColumn(
        "terminal_id",
        F.when(
            (F.col("terminal_id") >= 0) & F.col("terminal_id").isNotNull(),
            F.col("terminal_id")
        ).otherwise(SPECIAL_TERMINAL_ID)
    )

    # tx_fraud_scenario
    df = df.withColumn(
        "tx_fraud_scenario",
        F.when(
            (F.col("tx_fraud_scenario") >= 0) & F.col("tx_fraud_scenario").isNotNull(),
            F.col("tx_fraud_scenario")
        ).otherwise(SPECIAL_SCENARIO_ID)
    )

    # tx_time_seconds
    df = df.withColumn(
        "tx_time_seconds",
        F.when(
            (F.col("tx_time_seconds") >= 0) & F.col("tx_time_seconds").isNotNull(),
            F.col("tx_time_seconds")
        ).otherwise(SPECIAL_TIME_SECONDS)
    )

    # bad_tx_time_days
    df = df.withColumn(
        "tx_time_days",
        F.when(
            (F.col("tx_time_days") >= 0) & F.col("tx_time_days").isNotNull(),
            F.col("tx_time_days")
        ).otherwise(SPECIAL_TIME_DAYS)
    )


    #!!!
    #df.persist(StorageLevel.MEMORY_AND_DISK)

# =====================================================
# Логирование, если задано
# =====================================================

    if LOG:

        dq_stats = df.select(
            F.count("*").alias("rows_after_clean"),
            #F.countDistinct("transaction_id").alias("unique_transactions"),
            F.sum(F.when(F.col("rn") > 1, 1).otherwise(0)).alias("duplicates"),
            F.sum(F.when(~((F.col("transaction_id") >= 0) & (F.col("transaction_id").isNotNull())), 1).otherwise(0)).alias("bad_transaction_id"),
            F.sum(F.when(F.col("tx_datetime").isNull(), 1).otherwise(0)).alias("bad_tx_datetime"),
            F.sum(F.when(F.col("customer_id") == SPECIAL_CUSTOMER_ID, 1).otherwise(0)).alias("bad_customer_id"),
            F.sum(F.when(F.col("terminal_id") == SPECIAL_TERMINAL_ID, 1).otherwise(0)).alias("bad_terminal_id"),
            F.sum(F.when(~((F.col("tx_amount") >= 0) & (F.col("tx_amount").isNotNull()) ), 1).otherwise(0)).alias("bad_tx_amount"),
            F.sum(F.when(F.col("tx_time_seconds") == SPECIAL_TIME_SECONDS, 1).otherwise(0)).alias("bad_tx_time_seconds"),
            F.sum(F.when(F.col("tx_time_days") == SPECIAL_TIME_DAYS, 1).otherwise(0)).alias("bad_tx_time_days"),
            F.sum(F.when(F.col("tx_fraud_scenario") == SPECIAL_SCENARIO_ID, 1).otherwise(0)).alias("bad_scenario_id"),
            F.sum(F.when(~((F.col("tx_fraud").isin([0,1])) & (F.col("tx_fraud").isNotNull())), 1).otherwise(0)).alias("bad_tx_fraud")
        )

        dq1 = dq_corrupt.collect()[0]
        dq2 = dq_stats.collect()[0]

        logger.warning(
            f"""
            DATA QUALITY REPORT
            -------------------
            total_rows:            {dq1['total_rows']}
            corrupt_rows:          {dq1['corrupt_rows']}

            rows_after_clean:      {dq2['rows_after_clean']}
            duplicates:            {dq2['duplicates']}

            bad_transaction_id:    {dq2['bad_transaction_id']}
            bad_tx_datetime:       {dq2['bad_tx_datetime']}
            bad_customer_id:       {dq2['bad_customer_id']}
            bad_terminal_id:       {dq2['bad_terminal_id']}
            bad_scenario_id:       {dq2['bad_scenario_id']}
            bad_tx_amount:         {dq2['bad_tx_amount']}
            bad_tx_time_seconds:   {dq2['bad_tx_time_seconds']}
            bad_tx_time_days:      {dq2['bad_tx_time_days']}
            bad_tx_fraud:          {dq2['bad_tx_fraud']}
            """
            )

    # =====================================================
    # Фильтрация и финальное приведение типов
    # =====================================================

    df_clean = (
        df.filter(
            (F.col("rn") == 1) &
            (F.col("transaction_id") >= 0) &
            F.col("transaction_id").isNotNull() &
            F.col("tx_datetime").isNotNull() &
            (F.col("tx_amount") >= 0) &
            F.col("tx_amount").isNotNull() &
            F.col("tx_fraud").isin([0, 1]) &
            F.col("tx_fraud").isNotNull()
        )
        .withColumn("tx_fraud", F.col("tx_fraud").cast(BooleanType()))
        .drop("rn")
    )


    # df.filter(df["customer_id"]==-1).limit(5)

    # =====================================================
    # Для сортировки и сохранения
    # =====================================================
    df_clean = df_clean.withColumn("unix_time", F.col("tx_datetime").cast("long"))
    #df_clean = df_clean.withColumn("date", F.to_date("tx_datetime"))
    df_clean = df_clean.withColumn("week", F.date_trunc("week", F.from_unixtime("unix_time")))

    # =====================================================
    # Запись результата
    # =====================================================

    (   df_clean
        .repartition("week") # Группируем данные одной даты в один раздел Spark
        .sortWithinPartitions("unix_time") # Сортируем внутри каждой даты по времени
        .write
        .partitionBy("week") # Создаем структуру папок /date=YYYY-MM-DD/
        .mode("append")      # Добавляем новую порцию к существующим данным
        .parquet(OUTPUT_PATH)
    )
    
    
    data_dict = {
        old_end_datetime_str: end_id,
        }
    
    dates_json_obj.write_json(data_dict)

    spark.stop()
    
   

def main() -> None:
    try:
        # Парсинг аргументов
        args = parse_arguments()

        # Логирование параметров запуска
        print("=" * 50)
        print("Запуск скрипта обработки данных")
        print(f"in path: {args.in_path}")
        print(f"out path: {args.out_path}")
        print(f"HDFS хост:порт: {args.master_conn}")
        print(f"Логирование фильтраций: {'Включено' if args.log_stats else 'Выключено'}")
        print(f"Локальный запуск: {'Включено' if args.local else 'Выключено'}")
        print("=" * 50)

        # Запуск обработки данных
        clear_data(
            in_path=args.in_path,
            out_path=args.out_path,
            master_conn=args.master_conn,
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


#python infra/scripts/clean_data.py --hdfs-path=/user/ubuntu/data/2022-11-04.txt \
#--s3-bucket-path=s3a://otus-bucket3-b1gukkncvsp3tvci7gp3/data \
#--log-stats \
#--local

# yc dataproc job submit spark \
#   --cluster-name otus-dataproc-cluster \
#   --name clean-fraud-data \
#   --main-python-file infra\scripts\clean_data.py \
#   --args "--hdfs-path=/user/ubuntu/data/2022-11-04.txt --s3-bucket-path=s3a://otus-bucket3-b1gukkncvsp3tvci7gp3/data "


#spark-submit s3a://otus-bucket3-b1gukkncvsp3tvci7gp3/scpts/clean_data.py \
#--hdfs-path /user/ubuntu/data/2022-11-04.txt \
#--s3-bucket-path s3a://otus-bucket3-b1gukkncvsp3tvci7gp3/data


#python scripts/clean_data.py \
#--in-path="in_folder" \
#--out-path="out_folder" \
#--log-stats \
#--local

#python ./scripts/clean_data3.py --in-path="in_folder" --out-path="out_folder" --log-stats --local