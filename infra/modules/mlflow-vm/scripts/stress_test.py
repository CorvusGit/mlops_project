import sys
import argparse
import json
import time
import pandas as pd
from confluent_kafka import Producer
from confluent_kafka import Consumer
from confluent_kafka import TopicPartition

import boto3
from io import BytesIO
from datetime import datetime

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import io
import multiprocessing as mp

def parse_arguments() -> argparse.Namespace:

    parser = argparse.ArgumentParser(
        description='Скрипт для очистки данных из HDFS и сохранения в S3'
    )

    parser.add_argument(
        '--in-path',
        type=str,
        required=True,
        help='Входящий топик в Kafka'
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
        '--bucket',
        type=str,
        required=False,
        default="",
        help='s3 bucket'
    )

    parser.add_argument(
        '--key-id',
        type=str,
        required=False,
        default="",
        help='access_key_id'
    )
    
    parser.add_argument(
        '--key-secret',
        type=str,
        required=False,
        default="",
        help='access_key_secret'
    )

    parser.add_argument(
        '--ca-path',
        type=str,
        required=False,
        default="",
        help='cert_ca_path'
    )

    parser.add_argument(
        '--kafka-conn',
        type=str,
        required=False,
        default="localhost:9092:user:pwd",
        help='подключение к kafka'
    )

    parser.add_argument(
        '--rps-steps',
        type=str,
        required=False,
        default="100,200",
        help='список скоростей которые нужно проверить'
    )

    parser.add_argument(
        '--test-duration',
        type=int,
        required=False,
        default=60,
        help='сколько времени проводится проверка каждой скорости (сек)'
    )

    parser.add_argument(
        '--check-interval',
        type=int,
        required=False,
        default=5,
        help='с какими интервалами запрашивается количество записей в топиках (сек)'
    )

    parser.add_argument(
        '--start-time',
        type=str,
        required=False,
        default="2019-10-20 00:00:00",
        help='start-time'
    )

    parser.add_argument(
        '--end-time',
        type=str,
        required=False,
        default="2019-11-20 00:00:00",
        help='end-time'
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


def get_topic_count(topic_name, conf):
    config = conf
    config.update({
        'group.id': f'checker_{time.time()}', # Уникальный ID каждый раз
        'auto.offset.reset': 'earliest'
         })
    consumer = Consumer(config)
    
    try:
        # запрашиваем список партиций
        metadata = consumer.list_topics(topic_name, timeout=5.0)
        if topic_name not in metadata.topics:
            return 0
        
        partition_ids = list(metadata.topics[topic_name].partitions.keys())
        total_count = 0
        
        for p_id in partition_ids:
            tp = TopicPartition(topic_name, p_id)
            # get_watermark_offsets
            low, high = consumer.get_watermark_offsets(tp, timeout=5.0)
            if high > 0:
                total_count += high
        
        return total_count
    except Exception as e:
        print(f"Ошибка при замере топика {topic_name}: {e}")
        return 0
    finally:
        consumer.close()


def stream_s3_parquet_to_kafka_json(
    s3_session,
    s3_prefix: str,
    conf: str,
    topic: str,
    rps: int,             
    start_date_time: str,
    end_date_time: str,
    max_duration: int = None,  # лимит работы в секундах
    max_messages: int = None,   # лимит сообщений для этого шага теста,
):
    ST = int(datetime.strptime(start_date_time, "%Y-%m-%d %H:%M:%S").timestamp())
    ET = int(datetime.strptime(end_date_time, "%Y-%m-%d %H:%M:%S").timestamp())
    
    config = conf
    #config.update({
    #    'auto.register.schemas': False,
    #    'use.latest.version': True
    #})
    producer = Producer(config)

    def delivery_report(err, msg):
        if err is not None:
            print("ERROR:", err)
            print("SIZE:", len(msg.value()) if msg.value() else None)
            print("SAMPLE:", msg.value()[:200])

    files = s3_session.get_lists(s3_prefix)
    print(f"RPS: {rps} | Лимит: {max_duration}с / {max_messages} сообщ. | Файлов: {len(files)}")

    delay = 1.0 / rps
    count = 0
    test_start_wall_time = time.time()       # Для контроля max_duration
    test_start_perf_counter = time.perf_counter() # Для точного delay

    for file_key in files:
        response = s3_session.s3.get_object(Bucket=s3_session.bucket, Key=file_key)
        df = pd.read_parquet(BytesIO(response['Body'].read()))
        
        if df['unix_time'].iloc[-1] < ST:
            continue
       
        # Оставляем только нужный временной интервал из файла
        df_filtered = df[(df['unix_time'] >= ST) & (df['unix_time'] <= ET)]
        if df_filtered.empty: continue

        for _, row in df_filtered.iterrows():
            # проверка лимитов
            if max_messages and count >= max_messages:
                print(f"Достигнут лимит сообщений: {max_messages}")
                producer.flush()
                return count
            
            if max_duration and (time.time() - test_start_wall_time) >= max_duration:
                print(f"Достигнут лимит времени: {max_duration} сек")
                producer.flush()
                return count

            # Формирование сообщения
            message = row.to_dict()
            message['transaction_id'] = int(message['transaction_id'])
            message['unix_time'] = int(message['unix_time'])
            message['tx_fraud'] = int(message['tx_fraud'])
            
            payload = json.dumps(message, default=str).encode('utf-8')
            producer.produce(topic, value=payload, callback=delivery_report)
            count += 1
            
            # Ритмичная отправка (прецизионный тайминг)
            expected_time = test_start_perf_counter + (count * delay)
            current_time = time.perf_counter()
            sleep_time = expected_time - current_time
            if sleep_time > 0:
                time.sleep(sleep_time)
                
            if count % 1000 == 0:
                producer.poll(0)

    producer.flush()
    print(f"Поток завершен. Всего отправлено: {count}")
    return count


def run_test_step(rps, duration, step_args):
    # запускаем эмулятор в фоновом процессе
    p = mp.Process(target=stream_s3_parquet_to_kafka_json, kwargs={
        **step_args,
        'rps': rps,
        'max_duration': duration
    })
    p.start()
    return p

def plot_clean_stress_results(df, dates_json_session=""):
    
    sns.set_theme(style="whitegrid")
    
    g = sns.FacetGrid(df, col="rps", col_wrap=3, height=4, sharey=False)
    
    g.map(sns.lineplot, "timestamp", "lag", marker="o", color="royalblue")
    
    g.set_axis_labels("Секунды теста", "Lag (сообщений)")
    g.set_titles("Нагрузка: {col_name} RPS")
    
    plt.subplots_adjust(top=0.9)
    g.fig.suptitle('Динамика Lag (без аномальных выбросов)', fontsize=16)

    img_data = io.BytesIO()
    plt.savefig(img_data, dpi=300, bbox_inches='tight')
    img_data.seek(0)
    dates_json_session.write_img(img_data)
    print(f"График сохранен в s3 как {dates_json_session.file_path}")
    
    #plt.show()

def s3_to_kafka(
        in_path: str,
        in_topic: str,
        out_topic: str,
        kafka_conn: str = "", #IP:PORT[:user:pwd]
        bucket: str = "",
        access_key_id: str = "",
        access_key_secret: str = "",
        cert_ca_path: str = "",
        rps_steps: str = "100,200",
        test_duration: int = 60,
        check_interval: int = 5,
        start_time: str = "",
        end_time: str = "",
    ):

    INPUT_PATH = in_path
    IN_TOPIC = in_topic
    OUTPUT_TOPIC = out_topic
    BUCKET = bucket
    ACCESS_KEY_ID = access_key_id
    ACCESS_KEY_SECRET = access_key_secret
    ST = start_time
    ET = end_time

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
    
    CA_CERT_PATH = cert_ca_path
    RPS_STEPS = [int(x) for x in rps_steps.split(',')]
    

    PATH_IMG_FILE="stress_test_data/result.png"


    dates_json_session = dates_json(
        bucket = BUCKET,
        aws_access_key_id=ACCESS_KEY_ID,
        aws_secret_access_key=ACCESS_KEY_SECRET,
        file_path = PATH_IMG_FILE,
        endpoint_url='https://storage.yandexcloud.net',
        region_name='ru-central1-a',
        )

    if KAFKA_USER:
        conf = {
        'bootstrap.servers': KAFKA_CONN,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'SCRAM-SHA-512',
        'sasl.username': KAFKA_USER,
        'sasl.password': KAFKA_PWD,
        'ssl.ca.location': CA_CERT_PATH
        }
    else:
        conf = {'bootstrap.servers': KAFKA_CONN}

    
    results = []

    step_args = {
        's3_session': dates_json_session,
        's3_prefix': INPUT_PATH,
        'conf': conf,
        'topic': IN_TOPIC,
        'start_date_time': ST,
        'end_date_time': ET
    }
    
    for current_rps in RPS_STEPS:
        print(f"\n>>> Начинаем тест: {current_rps} RPS")
        
        # Запускаем генератор в фоне
        proc = run_test_step(current_rps, test_duration, step_args)
        
        start_time = time.time()
        # Пока процесс живет, делаем замеры
        while proc.is_alive():
            time.sleep(check_interval)
            
            in_total = get_topic_count(IN_TOPIC,conf)
            out_total = get_topic_count(OUTPUT_TOPIC,conf)
            
            results.append({
                'rps': current_rps,
                'timestamp': int(time.time() - start_time),
                'input_total': in_total,
                'output_total': out_total,
                'lag': in_total - out_total
            })
            print(f"[{current_rps} RPS] Lag: {in_total - out_total}")

        proc.join() # убеждаемся, что процесс завершен
        print(f"Ступень {current_rps} завершена. Пауза 10с для стабилизации...")
        time.sleep(10)

    # Собираем результат
    df_perf = pd.DataFrame(results)

    # Данные из файлов подтягиваются, поэтому есть пики, убираем их
    df_clean = df_perf[
        df_perf['lag'] <= df_perf.groupby('rps')['lag'].transform('mean') * 3
    ].copy()

    plot_clean_stress_results(df_clean,dates_json_session)

    return 

def main() -> None:
    try:
        args = parse_arguments()

        s3_to_kafka(
            in_path = args.in_path,
            in_topic = args.in_topic,
            out_topic = args.out_topic,
            kafka_conn = args.kafka_conn,
            bucket = args.bucket,
            access_key_id = args.key_id,
            access_key_secret = args.key_secret,
            cert_ca_path = args.ca_path,
            rps_steps = args.rps_steps,
            test_duration = args.test_duration,
            check_interval = args.check_interval,
            start_time = args.start_time,
            end_time = args.end_time
        )
        

    except Exception as e:
        print(f"Ошибка: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

#python3 ./scripts/to_kafka.py --in-path=output_data_for_ml/ --in-topic=fraud_test --out-topic=predictions --kafka-conn=localhost:9092 --bucket="" \
#--key-id='' --key-secret='' --ca-path='./kafka/secrets/ca-cert' \
#--rps-steps='100,200' --test-duration=60 --check-interval=5