import asyncio
import os
import json
import logging
import aiohttp
from aiokafka import AIOKafkaConsumer
import ssl

logging.basicConfig(level=logging.INFO)


async def send_to_api(session, url, data):
    try:
        async with session.post(url, json=data, timeout=10) as response:
            if response.status == 200:
                return True
            logging.error(f"API error: {response.status}")
            return False
    except Exception as e:
        logging.error(f"Connection failed: {e}")
        return False

async def consume():
    # Настройка SSL для Yandex Cloud
    
    cert_path = '/usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt'
    
    context = ssl.create_default_context(cafile=cert_path)

    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    KAFKA_USER = os.getenv("KAFKA_USER")
    KAFKA_PASS = os.getenv("KAFKA_PASS")
    KAFKA_TOPIC_IN = os.getenv("KAFKA_TOPIC_IN")
    FRAUD_API_URL = os.getenv("FRAUD_API_URL","http://fraud-app:80/predict")

    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC_IN,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM-SHA-512",
        sasl_plain_username=KAFKA_USER,
        sasl_plain_password=KAFKA_PASS,
        ssl_context=context,
        group_id="forwarder-group",
        retry_backoff_ms=500,
        request_timeout_ms=30000
    )

    try:
        await consumer.start()
        logging.info("Kafka Consumer started successfully")
    except Exception as e:
        logging.error(f"Failed to start Kafka Consumer: {e}", exc_info=True)
        return  # Или sys.exit(1)
    
    # Используем одну сессию для всех запросов (рекомендуется aiohttp)
    async with aiohttp.ClientSession() as session:
        try:
            async for msg in consumer:
                try:
                    payload = json.loads(msg.value.decode('utf-8'))
                    # Отправляем в API
                    success = await send_to_api(session, FRAUD_API_URL , payload)
                    if not success:
                        # Тут можно добавить логику ретраев или отправку в DLQ
                        pass
                except Exception as e:
                    logging.error(f"Error processing message: {e}")
        finally:
            await consumer.stop()

if __name__ == "__main__":
    asyncio.run(consume())