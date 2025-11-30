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

class SaluteSpeechClient:
    async def recognize_audio(self, audio_data: bytes, audio_format: str = "wav") -> str:
        """Заглушка для распознавания аудио"""
        logger.info(f"🔊 Вызов SaluteSpeech API: {len(audio_data)} байт, формат: {audio_format}")
        
        # Имитация обработки
        await asyncio.sleep(2)
        
        # Тестовый распознанный текст
        recognized_text = "Это тестовый распознанный текст. В реальности здесь будет вызов SaluteSpeech API."
        
        logger.info(f"📝 Результат распознавания: {recognized_text}")
        return recognized_text

async def process_audio_message(message: AbstractIncomingMessage, channel):
    """Обработка аудио сообщения"""
    try:
        data = json.loads(message.body.decode())
        logger.info(f"🎵 Начинаем обработку аудио: {data.get('filename')}")
        
        # Извлекаем аудио данные
        audio_data_hex = data.get('audio_data')
        audio_bytes = bytes.fromhex(audio_data_hex)
        
        # Распознаем аудио
        speech_client = SaluteSpeechClient()
        recognized_text = await speech_client.recognize_audio(audio_bytes, data.get('format', 'wav'))
        
        # Формируем ответ
        response_message = {
            "type": "recognized_text",
            "original_message": {
                "timestamp": data.get('timestamp'),
                "audio_source": data.get('audio_source'),
                "filename": data.get('filename')
            },
            "recognized_text": recognized_text,
            "processing_timestamp": "2025-11-30T16:44:40",
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
            routing_key="text"  # Отправляем информатору
        )
        
        # Подтверждаем обработку
        await message.ack()
        logger.info("✅ Аудио обработано и отправлено информатору")
        
    except Exception as e:
        logger.error(f"❌ Ошибка обработки: {e}")
        await message.nack(requeue=False)

async def main():
    logger.info("🚀 Запуск Ленгвиста...")
    
    try:
        rabbitmq_url = os.getenv("RABBITMQ_URL", "amqp://guest:guest@192.168.1.137/")
        logger.info(f"🔗 Подключаемся к RabbitMQ: {rabbitmq_url}")
        
        connection = await connect(rabbitmq_url)
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=1)
        
        # Объявляем очередь
        queue = await channel.declare_queue("to_linguist", durable=True)
        
        # Объявляем обменник (должен совпадать с роутером)
        exchange = await channel.declare_exchange(
            "message_router", 
            type="direct",
            durable=True
        )
        
        logger.info("✅ RabbitMQ подключен успешно!")
        logger.info("🎧 Начинаем слушать сообщения...")
        
        # Обработчик сообщений
        async def on_message(message):
            await process_audio_message(message, channel)
        
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