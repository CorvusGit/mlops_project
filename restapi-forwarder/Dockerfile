FROM python:3.8-slim

# Устанавливаем только сертификаты и curl
RUN apt-get update && apt-get install -y \
    curl \
    ca-certificates \
    && mkdir -p /usr/local/share/ca-certificates/Yandex \
    && curl https://storage.yandexcloud.net/cloud-certs/CA.pem \
    -o /usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt \
    && chmod 644 /usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt \
    && update-ca-certificates \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Копируем и устанавливаем зависимости (aiokafka, aiohttp)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем код
COPY ./scripts/forwarder.py .

# Переменные окружения
ENV KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
ENV KAFKA_TOPIC_IN="for_ml_data"
ENV FRAUD_API_URL="http://fraud-app:80/predict"

# Запуск асинхронного скрипта
CMD ["python", "forwarder.py"]