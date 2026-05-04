FROM python:3.8-slim

# Установка Java, curl и сертификатов
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    procps \
    curl \
    && mkdir -p /usr/local/share/ca-certificates/Yandex \
    && curl https://storage.yandexcloud.net/cloud-certs/CA.pem \
    -o /usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt \
    && chmod 644 /usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt \
    && update-ca-certificates \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Настройка переменных окружения для Java
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

WORKDIR /app

# Копируем и устанавливаем зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем код
COPY ./scripts/ .

# Переменные окружения по умолчанию (будут переопределены в k8s)
ENV MLFLOW_TRACKING_URI="http://127.0.0.1:5005"
ENV KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
ENV KAFKA_TOPIC_OUT="predictions"

# Открываем порт для FastAPI
EXPOSE 8000

# Запуск через gunicorn с использованием uvicorn-воркеров для промышленной эксплуатации
# -w 4: количество процессов (воркеров). Можно настроить под количество ядер CPU в k8s.
CMD ["gunicorn", "-w", "1", "-k", "uvicorn.workers.UvicornWorker", "rest_api_model:app", "--bind", "0.0.0.0:8000", "--timeout", "300"]
