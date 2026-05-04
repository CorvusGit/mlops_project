
from pyspark.sql import functions as F
from datetime import datetime

import matplotlib
matplotlib.use('Agg') # Переключает matplotlib в режим записи в файл, без GUI
import matplotlib.pyplot as plt
import seaborn as sns

import mlflow
from mlflow.tracking import MlflowClient

import pandas as pd
from scipy.stats import ttest_ind
import numpy as np
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.functions import vector_to_array


### ML FUNCTIONS ###

def get_metrics_and_log(predictions,target_col="tx_fraud", mlflow_log=True,prefix=''):
    metrics_raw = predictions.select(
        F.sum(F.when((F.col("prediction") == 1) & (F.col(target_col) == 1), 1).otherwise(0)).alias("TP"),
        F.sum(F.when((F.col("prediction") == 1) & (F.col(target_col) == 0), 1).otherwise(0)).alias("FP"),
        F.sum(F.when((F.col("prediction") == 0) & (F.col(target_col) == 1), 1).otherwise(0)).alias("FN"),
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

    if mlflow_log:
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


# FOR A/B TEST
    
def evaluate_model(model, test_df,target_col='tx_fraud'):
    """DataFrame
    Оценивает качество модели
    
    Parameters
    ----------
    model : sklearn estimator
        Обученная модель
    X_test : array-like
        Тестовые признаки
    y_test : array-like
        Тестовые метки
        
    Returns
    -------
    metrics : dict
        Словарь с метриками качества модели
    y_pred : array-like
        Предсказания модели
    """
    predictions = model.transform(test_df).select('unix_time','prediction','probability',target_col)
    predictions_with_prob = predictions.withColumn("prob_1", vector_to_array("probability")[1])
    
    P, R, F1, _ = get_metrics_and_log(predictions,target_col=target_col,
                                      mlflow_log=False,prefix='')
    
    auc = roc_auc_score(predictions_with_prob,target_col=target_col)
    
    metrics = {
        "precision": P,
        "recall": R,
        "f1_score": F1,
        "auc": auc
    }
    
    return metrics, predictions_with_prob

def roc_auc_score(predictions,target_col='tx_fraud'):
    
    evaluator = BinaryClassificationEvaluator(
        labelCol="tx_fraud",      # Ваша целевая переменная (судя по данным)
        rawPredictionCol="prob_1", # Колонка, которую мы создали выше
        metricName="areaUnderROC"
    )

    roc_auc = evaluator.evaluate(predictions)

    return roc_auc

def bootstrap_metrics_spark(predictions_df, n_iterations=100, seed=42):

    df = predictions_df.select("tx_fraud", "prediction", "prob_1")
    
    # Кэшируем, чтобы не пересчитывать модель 100 раз
    df.cache()

    # Оценщики
    auc_eval = BinaryClassificationEvaluator(labelCol="tx_fraud", rawPredictionCol="prob_1", metricName="areaUnderROC")
    multi_eval = MulticlassClassificationEvaluator(labelCol="tx_fraud", predictionCol="prediction")

    scores = []

    for i in range(n_iterations):
        # Генерируем выборку с возвращением (withReplacement=True)
        # seed меняем каждую итерацию, чтобы выборки были разными
        sample = df.sample(withReplacement=True, fraction=1.0, seed=seed + i)
        sample.cache()
        # Считаем метрики
        metrics = {
            "AUC": auc_eval.evaluate(sample),
            "F1": multi_eval.evaluate(sample, {multi_eval.metricName: "f1"}),
            "P": multi_eval.evaluate(sample, {multi_eval.metricName: "weightedPrecision"}),
            "R": multi_eval.evaluate(sample, {multi_eval.metricName: "weightedRecall"})
        }
        scores.append(metrics)
        sample.unpersist()
    df.unpersist()
    return pd.DataFrame(scores)

def statistical_comparison(scores_base, scores_candidate, alpha=0.01, metrics=['F1', 'P']):
    """
    Статистическое сравнение двух моделей с помощью t-теста
    
    Parameters
    ----------
    scores_base : pandas.DataFrame
        Метрики базовой модели
    scores_candidate : pandas.DataFrame
        Метрики модели-кандидата
    alpha : float, default=0.01
        Уровень значимости
    metrics : list, default=['F1', 'P']
        Список метрик для сравнения
        
    Returns
    -------
    results : dict
        Словарь с результатами статистического сравнения
    """
    results = {}

    for metric in metrics:
        t_stat, pvalue = ttest_ind(scores_base[metric], scores_candidate[metric])
        
        # Размер эффекта (Cohen's d)
        pooled_std = np.sqrt((scores_base[metric].var() + scores_candidate[metric].var()) / 2)
        effect_size = abs(scores_candidate[metric].mean() - scores_base[metric].mean()) / pooled_std
        
        is_significant = pvalue < alpha
        
        results[metric] = {
            't_statistic': t_stat,
            'p_value': pvalue,
            'effect_size': effect_size,
            'is_significant': is_significant,
            'base_mean': scores_base[metric].mean(),
            'candidate_mean': scores_candidate[metric].mean(),
            'improvement': scores_candidate[metric].mean() - scores_base[metric].mean()
        }
    
    return results
