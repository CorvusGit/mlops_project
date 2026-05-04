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
from pyspark.sql import Window


from datetime import datetime
from common_functions import dates_json

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
        help='подключение к мастер ноде'
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


# ФУНКЦИИ ПОДГОТОВКИ ПРИЗНАКОВ

def create_windows(partition_col, time_col, window_definitions, prefix_name='tx'):
    """
    window_definitions: dict вида {"название": (start_sec, end_sec)}
    Пример: {"1d_hist": (-86400, -1), "1d_full": (-86399, 0)}
    """
    windows = {}
    for name, (start, end) in window_definitions.items():
        windows[f'{prefix_name}_{name}'] = Window.partitionBy(*partition_col) \
                              .orderBy(time_col) \
                              .rangeBetween(start, end)
    #print(',\n'.join(windows.keys()))
    return windows


def add_aggregated_features(df, target_col, windows, agg_types=["count", "avg"],prefix=''):
    """
    df: DataFrame
    target_col: колонка для агрегации (напр. 'tx_amount')
    windows: словарь объектов WindowSpec из create_windows
    agg_types: типы агрегатов
    """
    
    win_cols = []
    drop_cols = []
    
    for win_name, win_spec in windows.items():

        # Базовые агрегаты
        if "count" in agg_types or "avg" in agg_types or "isnew" in agg_types:
            count_col = f"{prefix}{target_col}_cn_{win_name}"
            count_col_expr = F.count(F.lit(1)).over(win_spec)
            win_cols.append(count_col_expr.alias(count_col))
        
        if "sum" in agg_types or "avg" in agg_types:
            sum_col = f"{prefix}temp_sum_{win_name}"
            sum_col_expr = F.sum(target_col).over(win_spec)
            win_cols.append(sum_col_expr.alias(sum_col))
            
        if "avg" in agg_types:
            avg_col = f"{prefix}{target_col}_avg_{win_name}"
            win_cols.append((sum_col_expr / count_col_expr).alias(avg_col))
            
        if "std" in agg_types:
            std_col = f"{target_col}_std_{win_name}"
            win_cols.append(F.stddev(F.col(target_col)).over(win_spec).alias(std_col))

        if "isnew" in agg_types:
            isnew_col = f"{prefix}{target_col}_isnew_{win_name}"
            win_cols.append(F.when(count_col_expr == 0, 1).otherwise(0).alias(isnew_col))


        if "count" not in agg_types and ("avg" in agg_types or "isnew" in agg_types):
            drop_cols.append(count_col)

        if "sum" not in agg_types and "avg" in agg_types:
            drop_cols.append(sum_col)
   
    df_final = df.select("*",*win_cols)
    df_final = df_final.drop(*drop_cols)
    
    return df_final


def add_ratio_features_simple(df, target_col, hist_agg_cols,drop_hist=False):
    """
    df: DataFrame
    target_col: колонка для сравнения напр. 'tx_amount')
    hist_agg_cols: колонки, с которыми будет сравниваться текущее значение
    drop_hist: удалять ли hist_agg_cols по заверщении
    """
    
    for col_name in hist_agg_cols:
        ratio_col_name = f"ratio_{col_name}"
    
        df = df.withColumn(
            ratio_col_name, 
            F.col(target_col) / F.when(F.col(col_name) != 0, F.col(col_name)).otherwise(F.col(target_col))
        )
    
    # Удаляем промежуточную колонку с агрегатом
    if drop_hist:
        df = df.drop(*hist_agg_cols)
        
    return df
    
def add_aggregated_features_for_heavy(df, partition_cols, time_col, target_col, 
                                      windows_definitions, 
                                      bucket_interval=3600,
                                      agg_types=['count'],
                                      include_current=True,
                                      end_of_current=0,
                                      prefix = ""
                                      ):
    """
    Расчет агрегатов (AVG, STD, COUNT) с учетом текущей транзакции 
    без перекоса данных.
    для включения в результат текущих значений все окна должны оканчиваться одинаково на -bucket_interval
    """
    
    need_count = "count" in agg_types
    need_sum = "sum" in agg_types
    need_avg = "avg" in agg_types
    need_std = "std" in agg_types
    
    # Создаем временные бакеты
    df_with_buckets = df.withColumn(
        "_bucket", 
        (F.col(time_col) / bucket_interval).cast("long") * bucket_interval
    )
    
    agg_ops = []
    agg_col_for_del = []
    if need_count or need_avg or need_std:
        agg_ops.append(F.count(F.lit(1)).alias("_b_cnt"))
        agg_col_for_del.append("_b_cnt")
    if need_sum or need_avg:
        agg_ops.append(F.sum(target_col).alias("_b_sum"))
        agg_col_for_del.append("_b_sum")
    if need_std:
        agg_ops.append(F.sum(F.col(target_col)**2).alias("_b_sum_sq"))
        agg_col_for_del.append("_b_sum_sq")
    
    # Агрегируем компоненты для сумм, средних и стандартного отклонения
    df_buckets = df_with_buckets.groupBy(*partition_cols, "_bucket").agg(
        *agg_ops
    )
    
    win_cols = []
    # Рассчитываем агрегаты только по прошлым бакетам (не включая текущий)
    for win_name, (start, end) in windows_definitions.items():
        # Создаем новое окно для агрегации бакетов
        win_spec = Window.partitionBy(*partition_cols).orderBy("_bucket").rangeBetween(start, end)

        if need_count or need_avg or need_std:
            count_col = f"{prefix}_{target_col}_cn_{win_name}"
            win_cols.append(F.sum("_b_cnt").over(win_spec).alias(count_col+"_hist"))
        if need_sum or need_avg:
            sum_col = f"{prefix}_{target_col}_sum_{win_name}"
            win_cols.append(F.sum("_b_sum").over(win_spec).alias(sum_col+"_hist"))
        if need_std:
            sum_sq_col = f"{prefix}_{target_col}_sum_sq_{win_name}"
            win_cols.append(F.sum("_b_sum_sq").over(win_spec).alias(sum_sq_col+"_hist"))
    
    #добавляем колонки
    df_buckets = df_buckets.select("*", *win_cols)

    #удаляем временные поля
    df_buckets = df_buckets.drop(*agg_col_for_del)
    
    
    # добавляем к текущему датасету
    df_with_buckets = df_with_buckets.join(F.broadcast(df_buckets), 
                                    on=[*partition_cols, "_bucket"], how="left")
    
    
    # расчитываем в текущем бакете (эти данные будут одинаковы для всех окон, окна должны быть больше и кратны бакету)
    current_col = []
    if include_current:
        win_local = Window.partitionBy(*partition_cols, "_bucket").orderBy(time_col) \
                          .rangeBetween(Window.unboundedPreceding, end_of_current)
        
        if need_count or need_avg or need_std:
            current_col.append(F.count(F.lit(1)).over(win_local).alias('curr_cnt'))
        if need_sum or need_avg:
            current_col.append(F.sum(target_col).over(win_local).alias('curr_sum'))
        if need_std:
            current_col.append(F.sum(F.col(target_col)**2).over(win_local).alias('curr_sum_sq'))
    else:
        # Если текущий не нужен
        if need_count or need_avg or need_std:
            current_col.append(F.lit(0).alias('curr_cnt'))
        if need_sum or need_avg:
            current_col.append(F.lit(0).alias('curr_sum'))
        if need_std:
            current_col.append(F.lit(0).alias('curr_sum_sq'))
        
    
    #добавляем колонки текущего бакета
    df_with_buckets = df_with_buckets.select("*", *current_col)
    
    # объединение исторических и текущих

    final_cols = []
    drop_list = ["_bucket"]
    
    for win_name in windows_definitions.keys():
        # Берем историю из Join (если ее нет — 0)
        
        if need_count or need_avg or need_std:
            
            final_count_col = f"{prefix}_{target_col}_cn_{win_name}"
            final_h_cnt = F.coalesce(F.col(final_count_col+"_hist"), F.lit(0))
            final_curr_cnt = F.coalesce(F.col('curr_cnt'),F.lit(0))
            total_cnt = final_h_cnt + final_curr_cnt

            drop_list.append(final_count_col+"_hist")
            drop_list.append('curr_cnt')
        
        if need_sum or need_avg or need_std:
            
            final_sum_col = f"{prefix}_{target_col}_sum_{win_name}"
            final_h_sum = F.coalesce(F.col(final_sum_col+"_hist"), F.lit(0))
            final_curr_sum = F.coalesce(F.col('curr_sum'),F.lit(0))
            total_sum = final_h_sum + final_curr_sum
            drop_list.append(final_sum_col+"_hist")
            drop_list.append('curr_sum')

        if need_std:

            final_sum_sq_col = f"{prefix}_{target_col}_sum_sq_{win_name}"
            final_h_sum_sq = F.coalesce(F.col(final_sum_sq_col+"_hist"), F.lit(0))
            final_curr_sum_sq = F.coalesce(F.col('curr_sum_sq'),F.lit(0))
            total_sum_sq = final_h_sum_sq + final_curr_sum_sq
            
            std_col_name = f"{prefix}_{target_col}_std_{win_name}"
            
            drop_list.append(final_sum_sq_col+"_hist")
            drop_list.append('curr_sum_sq')

        if need_count:
            final_cols.append(total_cnt.alias(final_count_col))

        if need_sum:
            final_cols.append(total_sum.alias(final_sum_col))

        if need_avg:
            avg_col_name = f"{prefix}_{target_col}_avg_{win_name}"
            final_cols.append(
                F.when(total_cnt > 0, total_sum / total_cnt).otherwise(F.lit(0))
                .alias(avg_col_name)
                )
            
        if need_std:
            variance = F.when(total_cnt > 1,
                                (total_sum_sq / total_cnt) - ( (total_sum / total_cnt)**2 )
                            ).otherwise(F.lit(0))
            
            final_cols.append(
                F.when(total_cnt > 1, F.sqrt(F.greatest(F.lit(0), variance))).otherwise(F.lit(0))
                .alias(std_col_name)
                )

    # вычисляем итоговые колонки
    df_final = df_with_buckets.select("*",*final_cols)

    # Финальная чистка технических и исторических колонок
    df_final = df_final.drop(*list(set(drop_list)))
   
    return df_final

def get_count_risk_rolling_window_spark(df,full_colls, del_in_cols = True):
    """подсчёт риска терминалов"""

    drop_list = []
    for sum_col_name,cn_col_name in full_colls:
        
        # Считаем риск 
        risk_window = F.col(sum_col_name) / F.when(F.col(cn_col_name) == 0, None).otherwise(F.col(cn_col_name))
        
        # Добавляем колонки
        df = df.withColumn(f"{cn_col_name}_delay", F.coalesce(F.col(cn_col_name), F.lit(0))) \
               .withColumn(f"{sum_col_name.replace('_sum_','_risk_')}_delay", F.coalesce(risk_window, F.lit(0.0)))

        drop_list.append(sum_col_name)
        drop_list.append(cn_col_name)
        

    if del_in_cols:
        df = df.drop(*drop_list)
    
    return df

def get_count_risk_rolling_window_spark(df,full_colls, del_in_cols = True):
    """подсчёт риска терминалов"""

    drop_list = []
    for sum_col_name,cn_col_name in full_colls:
        
        # Считаем риск 
        risk_window = F.col(sum_col_name) / F.when(F.col(cn_col_name) == 0, None).otherwise(F.col(cn_col_name))
        
        # Добавляем колонки
        df = df.withColumn(f"{cn_col_name}_delay", F.coalesce(F.col(cn_col_name), F.lit(0))) \
               .withColumn(f"{sum_col_name.replace('_sum_','_risk_')}_delay", F.coalesce(risk_window, F.lit(0.0)))

        drop_list.append(sum_col_name)
        drop_list.append(cn_col_name)
        

    if del_in_cols:
        df = df.drop(*drop_list)
    
    return df


def prepair_data(
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

    PERSIST_BUCKET_NAME = pbn
    PERSIST_BUCKET_KEY = pbk
    PERSIST_BUCKET_SECRET = pbs

    SPECIAL_TERMINAL_ID = -1
    SPECIAL_CUSTOMER_ID = -1
    SPECIAL_SCENARIO_ID = -1
    SPECIAL_TIME_SECONDS = -1
    SPECIAL_TIME_DAYS = -1

    PI = math.pi
    
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
                .appName("Spark ML Clean Data")
                .master("local[14]")
                .config("spark.driver.memory", "20g")
                .config("spark.sql.shuffle.partitions", "64")
                .config("spark.driver.maxResultSize", "4g")
                .config("spark.local.dir", "/media/rk/2TB/spark_tmp")
                # Адаптивность
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") # Склеивать мелкие части
                .config("spark.sql.adaptive.skewJoin.enabled", "true")           # Обработка перекосов данных
                #.config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128mb") # целевой размер раздела при склейке
                .getOrCreate()
        )

        #spark.conf.set('spark.sql.repl.eagerEval.enabled', True)  # to pretty print pyspark.DataFrame in jupyter

    else:
        spark = (
            SparkSession.builder
                .appName("Spark ML Prepair")
                
                # Драйвер
                .config("spark.driver.memory", "10g")
                .config("spark.driver.maxResultSize", "2g") 

                # Исполнители
                .config("spark.executor.cores", "4")
                .config("spark.executor.memory", "10g")
                .config("spark.executor.memoryOverhead", "2g")

                # параллелизм
                .config("spark.sql.shuffle.partitions", "96")
                .config("spark.default.parallelism", "96")

                # Адаптивность
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") # Склеивать мелкие части
                .config("spark.sql.adaptive.skewJoin.enabled", "true")           # Обработка перекосов данных
                .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128mb") # целевой размер раздела при склейке

                # сетевой таймаут
                .config("spark.network.timeout", "600s")
                .getOrCreate()
        )


    spark.conf.set("spark.sql.ansi.enabled", "false")

    logger = logging.getLogger("PrepairData")
    logger.setLevel(logging.WARN)


    # =====================================================
    # Чтение файлов для временных отсчётов
    # =====================================================

    # размер пересечения с предыдущей порцией данных для подсчёта истории (максимально е окно)
    overlap = 3600*24*30 - 10
    #минимум новых записей для запуска
    time_limit = 3600*24
    # максимум сколько данных брать от последнего сохранённого timestamp (для проекта, чтобы каждый запуск добалял день данных)
    batch_size = 3600*24*30
    # первая порция данных
    first_batch = batch_size

    dates_json_obj = dates_json(
        bucket = PERSIST_BUCKET_NAME,
        aws_access_key_id=PERSIST_BUCKET_KEY,
        aws_secret_access_key=PERSIST_BUCKET_SECRET,
        file_path = 'for_ml_data_time/data.json',
        endpoint_url='https://storage.yandexcloud.net',
        region_name='ru-central1-a',
    )

    #считываем последний обработанный id транзакции
    dates_dict = dates_json_obj.read_json()
    
    old_end_datetime_str = 'old_end_datetime'

    if len(dates_dict) > 0:
        old_end_datetime = dates_json_obj.read_json()[old_end_datetime_str]      
    else:
        old_end_datetime = 0

    # =====================================================
    # Чтение данных
    # =====================================================
    fields = ['transaction_id',
        'customer_id',
        'terminal_id',
        'tx_amount',
        'tx_fraud',
        'unix_time']

    schema = StructType([
        StructField("transaction_id", LongType(), False),
        StructField("tx_datetime", StringType(), False),
        StructField("customer_id", IntegerType(), False),
        StructField("terminal_id", IntegerType(), False),
        StructField("tx_amount", DoubleType(), False),
        StructField("tx_fraud", IntegerType(), True),
        StructField("unix_time", LongType(), False)
    ])

    df = spark.read.parquet (
        INPUT_PATH,
        schema= schema
    ).select(*fields)

    #if SAMPLE_FRAQ > 0 and SAMPLE_FRAQ <=1:
    #    df=df.sample(fraction=SAMPLE_FRAQ, seed=42)

    #=====================================================
    # Смотрим с какого времени начать
    #=====================================================

    min_time = df.agg(F.min(F.col("unix_time"))).collect()[0][0]
    max_time = df.agg(F.max(F.col("unix_time"))).collect()[0][0]
    print(datetime.fromtimestamp(min_time))
    print(datetime.fromtimestamp(max_time))

    # не первый запуск (есть предыдущая метка)
    if old_end_datetime > 0:
        
        if  old_end_datetime - min_time < overlap:
            logger.warning(f"Not enough history data to start processing: {old_end_datetime - min_time} seconds with a min limit of {overlap}")
            spark.stop()
            return

        if max_time - old_end_datetime < time_limit:
            # должно быть по крайней time_limit новых данных, чтобы процесс запустился
            logger.warning(f"Not enough data to start processing: {max_time - old_end_datetime} seconds with a limit of {time_limit}")
            spark.stop()
            return 
            
        else:    
            start_time = old_end_datetime - overlap
            if batch_size > 0:
                end_time = min(old_end_datetime + batch_size, max_time)
            else:
                end_time = max_time
            
            start_good_time = old_end_datetime

    # первый запуск
    else:
        # если данных недостаточно (меньше периода максимального окна + минимальное количество новых данных)
        if max_time - min_time <= overlap + max(first_batch,batch_size):
            logger.warning(f"Not enough first data to start processing: {max_time - min_time} seconds with a min limit of {overlap + max(first_batch,batch_size)}")
            spark.stop()
            return
    
        start_time = max_time - max(first_batch,batch_size) - overlap 
        end_time = max_time
        start_good_time = start_time + overlap

    logger.warning(f'start_time: {datetime.fromtimestamp(start_time)}, end_time: {datetime.fromtimestamp(end_time)}, time_limit: {time_limit}, batch_size: {batch_size}, \
    first_batch: {(first_batch)}, overlap: {overlap}, start_good_time: {datetime.fromtimestamp(start_good_time)}')
    

    df = df.filter(
        (F.col("unix_time")>= start_time) 
        & (F.col("unix_time") < end_time)
        )
    
    # =====================================================
    # Генерация признаков
    # =====================================================
    HOUR = 3600
    DAY = 24 * 3600
    WEEK = 7 * 24 * 3600
    MONTH = 30 * 24 * 3600
    TMIN = 1*60
    PI = math.pi

    bucket_interval = 3600 # для подсчёта терминалов


    #создаём дополнительную колонку unix_time, переводим tx_fraud в int
    #features_df = df.withColumn("unix_time", F.col("tx_datetime").cast("long"))
    features_df = df.withColumn("tx_fraud", col("tx_fraud").cast(IntegerType()))
    

    # определяем основные окна

    TIME_COL = "unix_time"

    # задаём параметры окон
    ct_win_defs = {"30d_hist": (-MONTH, -1),
                   "7d_hist": (-DAY*7, -1)}

    win_defs_current = {
                "7d_full": (-WEEK, 0),
                "1d_full": (-DAY, 0),
                "1h_full": (-HOUR, 0),
            }

    win_defs_hist = {
                "30d_hist": (-MONTH, -WEEK),
                "7d_hist": (-WEEK, -DAY),
                "1d_hist": (-DAY, -HOUR),
            }
    
    win_defs_current_backet = {
                "7d_full": (-WEEK, -bucket_interval-1),
                "1d_full": (-DAY, -bucket_interval-1),
                "1h_full": (-HOUR*2, -bucket_interval-1),
            }
    
    win_defs_hist_backet = {
                "30d_hist": (-MONTH, -DAY -bucket_interval-1),
                "7d_hist": (-WEEK, -DAY -bucket_interval-1),
                "1d_hist": (-DAY, -HOUR -bucket_interval-1),
            }

    # инициализируем окна по клиенту
    win_ct = create_windows(["customer_id","terminal_id"], TIME_COL, ct_win_defs, 
                            prefix_name = 'ct')


    win_cust_current = create_windows(["customer_id"], TIME_COL, win_defs_current, 
                            prefix_name = 'cust')

    win_cust_hist = create_windows(["customer_id"], TIME_COL, win_defs_hist, 
                            prefix_name = 'cust')



    if LOG:
        logger.info(f"Окна созданы")

    # Был ли клиент на этом терминале хоть раз за 30 дней
    features_df = add_aggregated_features(features_df,'tx_amount', 
                                        win_ct, agg_types=["isnew"])

    # статистики по клиенту с текущими значениями
    features_df = add_aggregated_features(features_df,'tx_amount', win_cust_current, agg_types=["count", "avg"])

    # статистики по клиенту с историческими значениями
    features_df = add_aggregated_features(features_df,'tx_amount', win_cust_hist, agg_types=["avg","std",'count'])


    # статистики по терминалу текущие 
    # (терминал обрабатывается другими функциями, использующими подсчёт по батчам + текущие статистики внутри батча)
    features_df = add_aggregated_features_for_heavy(features_df, ['terminal_id'], TIME_COL, 'tx_amount', 
                                        win_defs_current_backet, 
                                        bucket_interval=bucket_interval, 
                                        agg_types=["avg","count"],
                                        include_current=True,
                                        end_of_current=0,
                                        prefix='term')

    # статистики по терминалу исторические
    features_df = add_aggregated_features_for_heavy(features_df, ['terminal_id'], TIME_COL, 'tx_amount', 
                                        win_defs_hist_backet, 
                                        bucket_interval=bucket_interval, 
                                        agg_types=["avg","count",'std'],
                                        include_current=False,
                                        end_of_current=0,
                                        prefix='term')


    # подсчёт новых клиентов на терминале за 30 дней
    features_df = add_aggregated_features_for_heavy(features_df, ['terminal_id'], TIME_COL, 'tx_amount_isnew_ct_30d_hist', 
                                        win_defs_current_backet,
                                        bucket_interval=bucket_interval, 
                                        agg_types=["sum"],
                                        include_current=False,
                                        end_of_current=0,
                                        prefix='term')

    # подсчёт новых клиентов на терминале за 7 дней
    features_df = add_aggregated_features_for_heavy(features_df, ['terminal_id'], TIME_COL, 'tx_amount_isnew_ct_7d_hist', 
                                        win_defs_current_backet,
                                        bucket_interval=bucket_interval, 
                                        agg_types=["sum"],
                                        include_current=False,
                                        end_of_current=0,
                                        prefix='term')


    win_fraud  = {
                "1d_full": (-WEEK-DAY, -WEEK),
                "7d_full": (-WEEK-WEEK, -WEEK),
                #три недели
                "21d_full": (-MONTH, -WEEK)
            }

    # получаем исторические данные на момент неделю назад
    features_df = add_aggregated_features_for_heavy(features_df, ['terminal_id'], TIME_COL, 'tx_fraud', 
                                        win_fraud,
                                        bucket_interval=bucket_interval, 
                                        agg_types=["sum","count"],
                                        include_current=False,
                                        end_of_current=0,
                                        prefix='fraud')

    #подсчёт риска терминала
    features_df = get_count_risk_rolling_window_spark(features_df,
                                                    [('fraud_tx_fraud_sum_1d_full','fraud_tx_fraud_cn_1d_full'),
                                                    ('fraud_tx_fraud_sum_7d_full','fraud_tx_fraud_cn_7d_full'),
                                                    ('fraud_tx_fraud_sum_21d_full','fraud_tx_fraud_cn_21d_full')],
                                                    del_in_cols = True)

    # Убираем первый период, чтобы убрать записи, где признаки не успели накопиться
    features_df = features_df.filter(F.col("unix_time") >= start_good_time)

    features_df = features_df.withColumn("tx_datetime", F.col("unix_time").cast("timestamp"))

    features_df = features_df \
        .withColumn("hour", F.hour("tx_datetime")) \
        .withColumn("is_night", F.when((F.col("hour") >= 0) & (F.col("hour") < 6), 1).otherwise(0)) \
        .withColumn("day_of_week", F.dayofweek("tx_datetime")) \
        .withColumn("is_weekend", F.when(F.col("day_of_week").isin(1, 7), 1).otherwise(0)) \
        .withColumn("is_rovn_sum", ((F.col("tx_amount") % 100 == 0) | (F.col("tx_amount") % 1000 == 0)).cast("int")) \
        .withColumn("is_unknonw_terminal", (F.col("terminal_id") == -1).cast("int")) \
        .withColumn("is_unknonw_customer", (F.col("customer_id") == -1).cast("int")) \
        .withColumn("hour_sin", F.sin(2 * PI * F.col("hour") / 24)) \
        .withColumn("hour_cos", F.cos(2 * PI * F.col("hour") / 24)) \
        .withColumn("day_sin", F.sin(2 * PI * F.col("day_of_week") / 7)) \
        .withColumn("day_cos", F.cos(2 * PI * F.col("day_of_week") / 7))  


    #print(df.columns)
    # считаем поля -отношения
    rate_cols = ["term_tx_amount_avg_30d_hist","term_tx_amount_avg_7d_hist",
            "term_tx_amount_std_30d_hist","term_tx_amount_std_7d_hist",
            'tx_amount_avg_cust_30d_hist','tx_amount_std_cust_30d_hist',
            'tx_amount_avg_cust_7d_hist','tx_amount_std_cust_7d_hist']

    features_df = add_ratio_features_simple(features_df, "tx_amount", rate_cols,drop_hist=False)

    #для сохранения по дням
    features_df = features_df.withColumn("date", F.to_date("tx_datetime"))
    
    #удаляем всё лишнее
    drop_cols =['terminal_id',
                'customer_id',
                'tx_datetime',
                'hour',
                'day_of_week']
                
    features_df = features_df.drop(*drop_cols)

    #заполняем null значения
    #features_df = features_df.fillna(0)

    # =====================================================
    # Запись результата
    # =====================================================
    # (
    #     features_df
    #     .coalesce(n_out)
    #     .write
    #     .mode("overwrite")
    #     .parquet(OUTPUT_PATH)
    # )
    
    

    (   features_df
        .repartition("date") # Группируем данные одной даты в один раздел Spark
        .sortWithinPartitions("unix_time") # Сортируем внутри каждой даты по времени
        .write
        .partitionBy("date") # Создаем структуру папок /date=YYYY-MM-DD/
        .mode("append")      # Добавляем новую порцию к существующим данным
        .parquet(OUTPUT_PATH)
    )

    data_dict = {
        old_end_datetime_str: end_time,
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
        prepair_data(
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

# python ./proc_for_ml_data.py \
# --in-path=s3a://otus-bucket3-b1gukkncvsp3tvci7gp3/data_for_test_in \
# --out-path=./out_folder \
# --log-stats \
# --local

# python scripts/proc_for_ml_data3.py \
# --in-path="out_folder" \
# --out-path="out_folder_for_ml" \
# --log-stats \
# --local

#python scripts/proc_for_ml_data3.py --in-path="out_folder" --out-path="out_folder_for_ml" --log-stats --local