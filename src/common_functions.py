import math
import re
import boto3
import json
from pyspark.sql import functions as F

# COMMONS

def get_coalesce_number(
    spark,
    logger,
    in_path: str,
    target_size_mb: int = 512,
    zip_coeff: int = 3
) -> int:
    """
    Вычисляет N для coalesce на основе суммарного размера данных
    Работает с HDFS и s3a://
    """

    sc = spark.sparkContext
    jvm = sc._gateway.jvm

    Path = jvm.org.apache.hadoop.fs.Path
    FileSystem = jvm.org.apache.hadoop.fs.FileSystem
    URI = jvm.java.net.URI

    hadoop_conf = sc._jsc.hadoopConfiguration()

    uri = URI.create(in_path)
    fs = FileSystem.get(uri, hadoop_conf)

    path = Path(in_path)

    if not fs.exists(path):
        raise FileNotFoundError(f"Путь не найден: {in_path}")

    # ContentSummary работает и для HDFS, и для s3a
    content_summary = fs.getContentSummary(path)

    total_bytes = content_summary.getLength()
    file_count = content_summary.getFileCount()

    if total_bytes == 0:
        logger.warning("Источник пустой, будет создан 1 файл")
        return 1

    # учитываем сжатие (parquet + snappy и т.п.)
    total_mb = (total_bytes / zip_coeff) / (1024 * 1024)

    n_coalesce = int(math.ceil(total_mb / target_size_mb))
    n_coalesce = max(1, n_coalesce)

    logger.warning(f"Источник: {in_path}")
    logger.warning(f"Файлов во входных данных: {file_count}")
    logger.warning(f"Суммарный размер (оценка): {total_mb:.2f} MB")
    logger.warning(f"Рекомендованный coalesce({n_coalesce}) для target_size_mb={target_size_mb}")

    return n_coalesce

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

def interval_to_seconds(interval_str):
    """Превращает '1 minute', '2 hours', '10s' в секунды (int)."""
    units = {
        "s": 1, "second": 1, "seconds": 1,
        "m": 60, "minute": 60, "minutes": 60,
        "h": 3600, "hour": 3600, "hours": 3600,
        "d": 86400, "day": 86400, "days": 86400
    }
    match = re.match(r"(\d+)\s*([a-zA-Z]+)", interval_str.lower().strip())
    if match:
        value, unit = match.groups()
        return int(value) * units.get(unit, 0)
    return None


# ФУНКЦИИ РАЗДЕЛЕНИЯ ПРИЗНАКОВ И ВЫВОДА МЕТРИК
    
def split_train_test_by_time(df, train_ratio=0.8, time_col='tx_datetime'):
    """
    Разделяет датасет на train/test по времени без перемешивания.
    """
    # Находим минимальное и максимальное время (в секундах для точности)
    # stats = df.select(
    #     F.min(time_col).alias('min_t'),
    #     F.max(time_col).alias('max_t')
    # ).collect()[0]
    
    # start_t = stats['min_t']
    # end_t = stats['max_t']
    
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
