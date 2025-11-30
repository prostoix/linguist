import asyncio
import logging
import os
import json
from aio_pika import connect, Message, DeliveryMode
from aio_pika.abc import AbstractIncomingMessage

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("linguist")

from src.services.speech_client import SaluteSpeechClient

async def process_audio_message(message: AbstractIncomingMessage, channel, speech_client):
    """Обработка аудио сообщения"""
    try:
        data = json.loads(message.body.decode())
        filename = data.get('filename', 'unknown')
        logger.info(f"🎵 Начинаем обработку аудио: {filename}")
        
        # Извлекаем аудио данные
        audio_data_hex = data.get('audio_data')
        audio_bytes = bytes.fromhex(audio_data_hex)
        
        # Распознаем аудио через REAL SaluteSpeech
        recognized_text = await speech_client.recognize_audio(audio_bytes, data.get('format', 'wav'))
        
        # Формируем ответ
        from datetime import datetime
        response_message = {
            "type": "recognized_text",
            "original_message": {
                "timestamp": data.get('timestamp'),
                "audio_source": data.get('audio_source'),
                "filename": data.get('filename')
            },
            "recognized_text": recognized_text,
            "processing_timestamp": datetime.now().isoformat(),
            "additional_tag": data.get('additional_tag')
        }
        
        # Отправляем результат в RabbitMQ
        exchange = await channel.get_exchange("message_router")
        await exchange.publish(
            Message(
                body=json.dumps(response_message).encode(),
                content_type="application/json",
                delivery_mode=DeliveryMode.PERSISTENT
            ),
            routing_key="text"
        )
        
        # Подтверждаем обработку
        await message.ack()
        logger.info("✅ Аудио обработано и отправлено информатору")
        
    except Exception as e:
        logger.error(f"❌ Ошибка обработки: {e}")
        await message.nack(requeue=False)

async def main():
    logger.info("🚀 Запуск Ленгвиста с REAL SaluteSpeech OAuth...")
    
    try:
        # Инициализация SaluteSpeech клиента с OAuth
        client_id = os.getenv("SALUTE_SPEECH_CLIENT_ID")
        client_secret = os.getenv("SALUTE_SPEECH_CLIENT_SECRET")
        scope = os.getenv("SALUTE_SPEECH_SCOPE", "salutespeech")
        
        if not client_id or not client_secret:
            logger.error("❌ Не заданы SALUTE_SPEECH_CLIENT_ID или SALUTE_SPEECH_CLIENT_SECRET")
            return
        
        speech_client = SaluteSpeechClient(client_id, client_secret, scope)
        
        # Получаем первый токен
        await speech_client._get_access_token()
        
        # Проверка доступности SaluteSpeech
        if await speech_client.health_check():
            logger.info("✅ SaluteSpeech доступен")
        else:
            logger.error("❌ SaluteSpeech недоступен")
            return
        
        # Подключение к RabbitMQ
        rabbitmq_url = os.getenv("RABBITMQ_URL", "amqp://guest:guest@192.168.1.137/")
        logger.info(f"🔗 Подключаемся к RabbitMQ: {rabbitmq_url}")
        
        connection = await connect(rabbitmq_url)
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=1)
        
        # Объявляем очередь и обменник
        queue = await channel.declare_queue("to_linguist", durable=True)
        exchange = await channel.declare_exchange("message_router", type="direct", durable=True)
        
        logger.info("✅ Все сервисы подключены!")
        logger.info("🎧 Начинаем слушать сообщения...")
        
        # Обработчик сообщений
        async def on_message(message):
            await process_audio_message(message, channel, speech_client)
        
        await queue.consume(on_message)
        
        # Бесконечный цикл
        while True:
            await asyncio.sleep(60)
            logger.info("💓 Сервис работает...")
            
    except Exception as e:
        logger.error(f"❌ Ошибка: {e}")
        raise

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("⏹️ Остановлено пользователем")
    except Exception as e:
        logger.error(f"💥 Критическая ошибка: {e}")