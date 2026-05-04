import pytest
import pandas as pd
import numpy as np

# Импортируем из вашего кода список колонок и модель данных
#from scripts.rest_api_model import FEATURES
from scripts.inference_functions import FraudFeatures
FEATURES = [x for x in list(FraudFeatures.model_fields.keys()) if x not in ['transaction_id', 'tx_fraud']]

def test_features_list():
    """Проверяем, что мы не забыли исключить ID и таргет из признаков"""
    
    assert "transaction_id" not in FEATURES
    assert "tx_fraud" not in FEATURES
    # Проверка, что список не пустой и содержит наши 60+ признаков
    assert len(FEATURES) > 60

def test_pydantic_validation():
    """Проверяем, что Pydantic правильно распознает типы данных"""
    test_input = {
        "transaction_id": 1,
        "tx_amount": 1500.50,
        #"tx_fraud": 0,
        #"unix_time": 1672531200,
        "tx_amount_isnew_ct_30d_hist": 1,
        "tx_amount_isnew_ct_7d_hist": 0,
        "tx_amount_cn_cust_7d_full": 5,
        "tx_amount_avg_cust_7d_full": 1200.0,
        "tx_amount_cn_cust_1d_full": 1,
        "tx_amount_avg_cust_1d_full": 1500.5,
        "tx_amount_cn_cust_1h_full": 1,
        "tx_amount_avg_cust_1h_full": 1500.5,
        "tx_amount_cn_cust_30d_hist": 20,
        "tx_amount_avg_cust_30d_hist": 1000.0,
        "tx_amount_std_cust_30d_hist": 200.0,
        "tx_amount_cn_cust_7d_hist": 5,
        "tx_amount_avg_cust_7d_hist": 1100.0,
        "tx_amount_std_cust_7d_hist": 150.0,
        "tx_amount_cn_cust_1d_hist": 1,
        "tx_amount_avg_cust_1d_hist": 1500.5,
        "tx_amount_std_cust_1d_hist": 0.0,
        "term_tx_amount_cn_7d_full": 100,
        "term_tx_amount_avg_7d_full": 2000.0,
        "term_tx_amount_cn_1d_full": 10,
        "term_tx_amount_avg_1d_full": 1800.0,
        "term_tx_amount_cn_1h_full": 2,
        "term_tx_amount_avg_1h_full": 1600.0,
        "term_tx_amount_cn_30d_hist": 500,
        "term_tx_amount_avg_30d_hist": 2100.0,
        "term_tx_amount_std_30d_hist": 500.0,
        "term_tx_amount_cn_7d_hist": 120,
        "term_tx_amount_avg_7d_hist": 1900.0,
        "term_tx_amount_std_7d_hist": 400.0,
        "term_tx_amount_cn_1d_hist": 15,
        "term_tx_amount_avg_1d_hist": 1700.0,
        "term_tx_amount_std_1d_hist": 300.0,
        "term_tx_amount_isnew_ct_30d_hist_sum_7d_full": 10,
        "term_tx_amount_isnew_ct_30d_hist_sum_1d_full": 2,
        "term_tx_amount_isnew_ct_30d_hist_sum_1h_full": 0,
        "term_tx_amount_isnew_ct_7d_hist_sum_7d_full": 5,
        "term_tx_amount_isnew_ct_7d_hist_sum_1d_full": 1,
        "term_tx_amount_isnew_ct_7d_hist_sum_1h_full": 0,
        "fraud_tx_fraud_cn_1d_full_delay": 0,
        "fraud_tx_fraud_risk_1d_full_delay": 0.01,
        "fraud_tx_fraud_cn_7d_full_delay": 1,
        "fraud_tx_fraud_risk_7d_full_delay": 0.05,
        "fraud_tx_fraud_cn_21d_full_delay": 3,
        "fraud_tx_fraud_risk_21d_full_delay": 0.1,
        "is_night": 0,
        "is_weekend": 1,
        "is_rovn_sum": 0,
        "is_unknonw_terminal": 0,
        "is_unknonw_customer": 0,
        "hour_sin": -0.5,
        "hour_cos": 0.86,
        "day_sin": 0.78,
        "day_cos": 0.62,
        "ratio_term_tx_amount_avg_30d_hist": 1.2,
        "ratio_term_tx_amount_avg_7d_hist": 1.1,
        "ratio_term_tx_amount_std_30d_hist": 0.5,
        "ratio_term_tx_amount_std_7d_hist": 0.4,
        "ratio_tx_amount_avg_cust_30d_hist": 1.5,
        "ratio_tx_amount_std_cust_30d_hist": 0.8,
        "ratio_tx_amount_avg_cust_7d_hist": 1.3,
        "ratio_tx_amount_std_cust_7d_hist": 0.7,
        #"date": "2023-01-01"
    }
            
    obj = FraudFeatures(**test_input)
    assert obj.transaction_id == 1
    assert isinstance(obj.tx_amount, float)


def test_mapping():
    """Проверяем, что мы правильно называем результаты"""
    prediction_fraud = 1
    prediction_normal = 0
    
    label_fraud = "fraud" if prediction_fraud == 1 else "normal"
    label_normal = "fraud" if prediction_normal == 1 else "normal"
    
    assert label_fraud == "fraud"
    assert label_normal == "normal"