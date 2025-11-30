import asyncio
import signal
import sys
from src.services.linguist_service import LinguistService
from src.utils.logger import logger

class Application:
    """Основное приложение"""
    
    def __init__(self):
        self.linguist_service = LinguistService()
        self.shutdown_event = asyncio.Event()
    
    def setup_signal_handlers(self):
        """Настройка обработчиков сигналов"""
        loop = asyncio.get_event_loop()
        
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, self._signal_handler, sig)
    
    def _signal_handler(self, sig):
        """Обработчик сигналов"""
        logger.info(f"Получен сигнал {sig.name}")
        self.shutdown_event.set()
    
    async def run(self):
        """Запуск приложения"""
        logger.info("🚀 Запуск сервиса Ленгвист")
        
        try:
            # Настройка обработчиков сигналов
            self.setup_signal_handlers()
            
            # Запуск сервиса
            await self.linguist_service.start()
            
            # Ожидание сигнала остановки
            await self.shutdown_event.wait()
            
        except KeyboardInterrupt:
            logger.info("Получен сигнал KeyboardInterrupt")
        except Exception as e:
            logger.error(f"Неожиданная ошибка: {e}")
        finally:
            # Корректная остановка
            await self.linguist_service.stop()
    
    async def health_check(self):
        """Проверка здоровья приложения"""
        # Здесь можно добавить дополнительные проверки
        return True

async def main():
    """Точка входа в приложение"""
    app = Application()
    await app.run()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Приложение завершено по запросу пользователя")
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        sys.exit(1)