import mlflow.pyfunc
from mlflow.tracking import MlflowClient
import requests
import sys
from pydantic import BaseModel, Field
from loguru import logger

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

def get_model(model_name: str, flag: str = "last"):
    client = MlflowClient()
    try:
        if flag == "best":
            # Загружаем через pyfunc по алиасу
            model_uri = f"models:/{model_name}@best"
            model_v = client.get_model_version_by_alias(model_name, "best")
            version_num = model_v.version
            
            # ВАЖНО: используем pyfunc.load_model для универсального интерфейса .predict()
            model = mlflow.pyfunc.load_model(model_uri)
            return model, version_num
        
        else:  # last
            model_details = client.get_registered_model(model_name)
            # Берем самую последнюю версию из списка доступных
            latest_version_info = sorted(model_details.latest_versions, key=lambda x: int(x.version))[-1]
            latest_v = latest_version_info.version
            
            model_uri = f"models:/{model_name}/{latest_v}"
            model = mlflow.pyfunc.load_model(model_uri)
            return model, str(latest_v)
            
    except Exception as e:
        print(f"Ошибка при получении модели {model_name} ({flag}): {e}")
        return None, None


class FraudFeatures(BaseModel):
    transaction_id: int
    tx_amount: float
    #tx_fraud: Optional[int] = 0  # Обычно это таргет, но в схеме он есть
    #unix_time: int
    tx_amount_isnew_ct_30d_hist: int
    tx_amount_isnew_ct_7d_hist: int
    tx_amount_cn_cust_7d_full: int
    tx_amount_avg_cust_7d_full: float
    tx_amount_cn_cust_1d_full: int
    tx_amount_avg_cust_1d_full: float
    tx_amount_cn_cust_1h_full: int
    tx_amount_avg_cust_1h_full: float
    tx_amount_cn_cust_30d_hist: int
    tx_amount_avg_cust_30d_hist: float
    tx_amount_std_cust_30d_hist: float
    tx_amount_cn_cust_7d_hist: int
    tx_amount_avg_cust_7d_hist: float
    tx_amount_std_cust_7d_hist: float
    tx_amount_cn_cust_1d_hist: int
    tx_amount_avg_cust_1d_hist: float
    tx_amount_std_cust_1d_hist: float
    term_tx_amount_cn_7d_full: int
    term_tx_amount_avg_7d_full: float
    term_tx_amount_cn_1d_full: int
    term_tx_amount_avg_1d_full: float
    term_tx_amount_cn_1h_full: int
    term_tx_amount_avg_1h_full: float
    term_tx_amount_cn_30d_hist: int
    term_tx_amount_avg_30d_hist: float
    term_tx_amount_std_30d_hist: float
    term_tx_amount_cn_7d_hist: int
    term_tx_amount_avg_7d_hist: float
    term_tx_amount_std_7d_hist: float
    term_tx_amount_cn_1d_hist: int
    term_tx_amount_avg_1d_hist: float
    term_tx_amount_std_1d_hist: float
    term_tx_amount_isnew_ct_30d_hist_sum_7d_full: int
    term_tx_amount_isnew_ct_30d_hist_sum_1d_full: int
    term_tx_amount_isnew_ct_30d_hist_sum_1h_full: int
    term_tx_amount_isnew_ct_7d_hist_sum_7d_full: int
    term_tx_amount_isnew_ct_7d_hist_sum_1d_full: int
    term_tx_amount_isnew_ct_7d_hist_sum_1h_full: int
    fraud_tx_fraud_cn_1d_full_delay: int
    fraud_tx_fraud_risk_1d_full_delay: float
    fraud_tx_fraud_cn_7d_full_delay: int
    fraud_tx_fraud_risk_7d_full_delay: float
    fraud_tx_fraud_cn_21d_full_delay: int
    fraud_tx_fraud_risk_21d_full_delay: float
    is_night: int
    is_weekend: int
    is_rovn_sum: int
    is_unknonw_terminal: int
    is_unknonw_customer: int
    hour_sin: float
    hour_cos: float
    day_sin: float
    day_cos: float
    ratio_term_tx_amount_avg_30d_hist: float
    ratio_term_tx_amount_avg_7d_hist: float
    ratio_term_tx_amount_std_30d_hist: float
    ratio_term_tx_amount_std_7d_hist: float
    ratio_tx_amount_avg_cust_30d_hist: float
    ratio_tx_amount_std_cust_30d_hist: float
    ratio_tx_amount_avg_cust_7d_hist: float
    ratio_tx_amount_std_cust_7d_hist: float
    #date: str  # Будет сконвертировано в datetime в обработчике

    class Config:
        json_schema_extra = {
            "example": {
                "transaction_id": 12345,
                "tx_amount": 500.0,
                "unix_time": 1672531200,
                "date": "2023-01-01",
                # ... остальные поля по умолчанию заполнятся нулями
            }
        }