import asyncio
from aio_pika import connect, Message, DeliveryMode
from aio_pika.abc import AbstractRobustConnection, AbstractChannel

from src.config.settings import settings
from src.utils.logger import logger
from src.services.speech_client import SaluteSpeechClient
from src.handlers.message_handler import MessageHandler

class LinguistService:
    """Основной сервис для распознавания аудио в текст"""
    
    def __init__(self):
        self.settings = settings
        self.speech_client: SaluteSpeechClient = None
        self.connection: AbstractRobustConnection = None
        self.channel: AbstractChannel = None
        self.exchange = None
        self.message_handler: MessageHandler = None
        
    async def setup(self):
        """Настройка сервиса"""
        try:
            # Валидация настроек
            self.settings.validate()
            
            # Инициализация клиента распознавания
            self.speech_client = SaluteSpeechClient(
                api_key=self.settings.salute_speech_api_key,
                host=self.settings.salute_speech_host,
                port=self.settings.salute_speech_port
            )
            
            # Проверка здоровья сервиса распознавания
            if not await self.speech_client.health_check():
                raise RuntimeError("Сервис распознавания недоступен")
            
            # Настройка RabbitMQ
            await self._setup_rabbitmq()
            
            # Инициализация обработчика сообщений
            self.message_handler = MessageHandler(self.speech_client, self.exchange)
            
            logger.info("✅ Ленгвист настроен и готов к работе")
            
        except Exception as e:
            logger.error(f"❌ Ошибка настройки Ленгвиста: {e}")
            raise
    
    async def _setup_rabbitmq(self):
        """Настройка соединения с RabbitMQ"""
        # Подключаемся к RabbitMQ
        self.connection = await connect(self.settings.rabbitmq_url)
        self.channel = await self.connection.channel()
        
        # Устанавливаем лимит неподтвержденных сообщений
        await self.channel.set_qos(prefetch_count=self.settings.prefetch_count)
        
        # Объявляем очередь для входящих сообщений
        input_queue = await self.channel.declare_queue(
            self.settings.input_queue, 
            durable=True
        )
        
        # Объявляем обменник для отправки результатов
        self.exchange = await self.channel.declare_exchange(
            self.settings.exchange_name, 
            type="direct",
            durable=True
        )
        
        # Начинаем слушать очередь
        await input_queue.consume(self.message_handler.process_audio_message)
        
        logger.info(f"✅ RabbitMQ настроен, слушаем очередь: {self.settings.input_queue}")
    
    async def start(self):
        """Запуск сервиса"""
        await self.setup()
        logger.info("🚀 Ленгвист запущен и слушает сообщения")
        
        # Бесконечный цикл для поддержания работы
        try:
            while True:
                await asyncio.sleep(3600)  # Спим 1 час
        except asyncio.CancelledError:
            logger.info("Получен запрос на остановку")
    
    async def stop(self):
        """Остановка сервиса"""
        logger.info("🛑 Остановка Ленгвиста...")
        
        if self.speech_client:
            await self.speech_client.close()
        
        if self.connection:
            await self.connection.close()
        
        logger.info("✅ Ленгвист остановлен")