import json
from aio_pika.abc import AbstractIncomingMessage
from src.services.speech_client import SaluteSpeechClient
from src.utils.logger import logger

class MessageHandler:
    """Обработчик входящих сообщений"""
    
    def __init__(self, speech_client: SaluteSpeechClient, exchange):
        self.speech_client = speech_client
        self.exchange = exchange
    
    async def process_audio_message(self, message: AbstractIncomingMessage):
        """Обработка входящего аудио сообщения"""
        try:
            # Парсим сообщение
            data = json.loads(message.body.decode())
            filename = data.get('filename', 'unknown')
            logger.info(f"🎵 Получено аудио сообщение: {filename}")
            
            # Валидация сообщения
            if not await self._validate_message(data):
                await message.nack(requeue=False)
                return
            
            # Извлекаем аудио данные
            audio_data_hex = data.get('audio_data')
            audio_bytes = bytes.fromhex(audio_data_hex)
            audio_format = data.get('format', 'wav')
            
            # Распознаем аудио
            recognized_text = await self.speech_client.recognize_audio(
                audio_bytes, 
                audio_format
            )
            
            # Формируем ответное сообщение
            response_message = self._build_response_message(data, recognized_text)
            
            # Отправляем результат
            await self._send_response(response_message, data)
            
            # Подтверждаем обработку
            await message.ack()
            logger.info("✅ Аудио распознано и отправлено информатору")
            
        except json.JSONDecodeError as e:
            logger.error(f"❌ Ошибка декодирования JSON: {e}")
            await message.nack(requeue=False)
        except Exception as e:
            logger.error(f"❌ Ошибка обработки аудио сообщения: {e}")
            await message.nack(requeue=True)
    
    async def _validate_message(self, data: dict) -> bool:
        """Валидация входящего сообщения"""
        if 'audio_data' not in data:
            logger.error("❌ Аудио данные отсутствуют в сообщении")
            return False
        
        try:
            audio_data_hex = data['audio_data']
            # Проверяем, что это валидный hex
            bytes.fromhex(audio_data_hex)
            return True
        except (ValueError, TypeError):
            logger.error("❌ Невалидные аудио данные (ожидается hex строка)")
            return False
    
    def _build_response_message(self, original_data: dict, recognized_text: str) -> dict:
        """Формирование ответного сообщения"""
        return {
            "type": "recognized_text",
            "original_message": {
                "timestamp": original_data.get('timestamp'),
                "audio_source": original_data.get('audio_source'),
                "filename": original_data.get('filename'),
                "filepath": original_data.get('filepath')
            },
            "recognized_text": recognized_text,
            "processing_timestamp": self._get_current_timestamp(),
            "additional_tag": original_data.get('additional_tag')
        }
    
    def _get_current_timestamp(self) -> str:
        """Получение текущего времени в ISO формате"""
        from datetime import datetime
        return datetime.now().isoformat()
    
    async def _send_response(self, response_data: dict, original_data: dict):
        """Отправка ответного сообщения"""
        from aio_pika import Message, DeliveryMode
        
        await self.exchange.publish(
            Message(
                body=json.dumps(response_data).encode(),
                content_type="application/json",
                delivery_mode=DeliveryMode.PERSISTENT,
                headers=original_data.get("headers", {})
            ),
            routing_key="text"  # Маршрут к информатору
        )