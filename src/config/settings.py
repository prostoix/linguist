import os
from typing import Optional

class Settings:
    """Настройки приложения"""
    
    def __init__(self):
        self.rabbitmq_url = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost/")
        self.salute_speech_api_key = os.getenv("SALUTE_SPEECH_API_KEY", "")
        self.salute_speech_host = os.getenv("SALUTE_SPEECH_HOST", "smartspeech.sber.ru")
        self.salute_speech_port = int(os.getenv("SALUTE_SPEECH_PORT", "443"))
        
        # Настройки очередей
        self.input_queue = "to_linguist"
        self.output_routing_key = "text"
        self.exchange_name = "message_router"
        
        # Настройки обработки
        self.prefetch_count = 1
        self.max_retries = 3

    def validate(self):
        """Проверка обязательных настроек"""
        if not self.salute_speech_api_key:
            raise ValueError("SALUTE_SPEECH_API_KEY не установлен")
        return True

# Глобальный экземпляр настроек
settings = Settings()