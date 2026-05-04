"""
Application
"""

import os

from fastapi import FastAPI, HTTPException
from loguru import logger
from pydantic import BaseModel
import numpy as np
import pandas as pd
pd.DataFrame.iteritems = pd.DataFrame.items
import mlflow

from typing import List

from inference_functions import check_mlflow_availability, get_model, FraudFeatures

from typing import List
import asyncio
from aiokafka import AIOKafkaProducer
import json
import ssl

from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from fastapi import Response

# 1. Счетчик общего количества предсказаний
PREDICTIONS_TOTAL = Counter(
    'fraud_predictions_total', 
    'Total number of predictions made',
    ['status', 'model_version'] # Labeled метрики: fraud/normal и версия
)

# 2. Гистограмма времени инференса (полезно для мониторинга лагов)
PREDICTION_LATENCY = Histogram(
    'fraud_prediction_latency_seconds',
    'Time spent processing prediction',
    ['model_version']
)

FEATURES = [x for x in list(FraudFeatures.model_fields.keys()) if x not in ['transaction_id', 'tx_fraud']]

MLFLOW_CONN = os.getenv("MLFLOW_TRACKING_URI", "http://127.0.0.1:5005")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

KAFKA_USER = os.getenv("KAFKA_USER")
KAFKA_PASS = os.getenv("KAFKA_PASS")

# Путь к сертификату в образе (после update-ca-certificates он доступен в системе)
CERT_PATH = "/usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt"

KAFKA_TOPIC_OUT = os.getenv("KAFKA_TOPIC_OUT", "predictions")
print(f'KAFKA_BOOTSTRAP_SERVERS: {KAFKA_BOOTSTRAP_SERVERS}')

os.environ["MLFLOW_TRACKING_URI"] = MLFLOW_CONN
mlflow.set_tracking_uri(MLFLOW_CONN)

check_mlflow_availability(MLFLOW_CONN)

# Load model
logger.info("Loading model")
experiment_name = "Anti-Fraud-System_Optuna"
model_name = f'{experiment_name}_model'
MODEL, VERSION = get_model(model_name, "best")
print(MODEL)
if MODEL is None:
    logger.critical("Failed to load model. Exiting.")
    exit(1)

logger.info("Model loaded")


app = FastAPI()
producer = None


@app.on_event("startup")
async def startup_event():
    global producer
    
    context = ssl.create_default_context()

    # producer = AIOKafkaProducer(
    #     bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    #     value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    #     # Настройки для Yandex Managed Kafka
    #     security_protocol="SASL_SSL",
    #     sasl_mechanism="SCRAM-SHA-512",
    #     sasl_plain_username=KAFKA_USER,
    #     sasl_plain_password=KAFKA_PASS,
    #     ssl_context=context # Использовать системные сертификаты (куда мы добавили Yandex CA)
    # )
    # await producer.start()
    logger.info(f"Kafka Producer started on {KAFKA_BOOTSTRAP_SERVERS}")

@app.on_event("shutdown")
async def shutdown_event():
    #await producer.stop()
    logger.info("Kafka Producer stopped")

@app.get("/")
def health_check() -> dict:
    """Health check"""
    return {"status": "ok"}

@app.post("/predict")
async def make_prediction(features: List[FraudFeatures]) -> dict:
    # Засекаем время начала
    with PREDICTION_LATENCY.labels(model_version=VERSION).time():
        try:
            full_df = pd.DataFrame([f.model_dump() for f in features])
            
            ids = full_df['transaction_id'].tolist()

            X = full_df[FEATURES].copy()

            for col in X.columns:
                # Если колонка должна быть целым числом (IntegerType в Spark)
                if X[col].dtype == np.int64:
                    X[col] = X[col].astype(np.int32)
                # Если колонка должна быть double
                elif X[col].dtype == np.float32:
                    X[col] = X[col].astype(np.float64)
            
            X = X.fillna(0.0)

            predictions = MODEL.predict(X)
            
            results = []
            kafka_tasks = []
            
            for i, pred in enumerate(predictions):
                res_int = int(pred)
                label = "fraud" if res_int == 1 else "normal"
                
                # ОБНОВЛЕНИЕ МЕТРИКИ:
                # Увеличиваем счетчик с учетом результата (fraud или normal)
                PREDICTIONS_TOTAL.labels(status=label, model_version=VERSION).inc()

                res_payload = {
                    "transaction_id": ids[i],
                    "prediction": res_int,
                    "label": "fraud" if res_int == 1 else "normal",
                    "version": VERSION
                }
                results.append(res_payload)
                #kafka_tasks.append(producer.send(KAFKA_TOPIC_OUT, res_payload))
                
            #await asyncio.gather(*kafka_tasks)
                
            return {
                "results": results, 
                "version": VERSION,
                "count": len(results)
            }

        except Exception as e:
            logger.error(f"Batch prediction error: {e}")
            raise HTTPException(status_code=500, detail="Inference error")
    
@app.get("/metrics")
def metrics():
    return Response(
        content=generate_latest(), 
        media_type=CONTENT_TYPE_LATEST
    )