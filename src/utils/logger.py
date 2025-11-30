import logging
import sys

def setup_logger(name: str = "linguist") -> logging.Logger:
    """Настройка логгера"""
    
    logger = logging.getLogger(name)
    
    if logger.hasHandlers():
        return logger
    
    logger.setLevel(logging.INFO)
    
    # Форматтер
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Обработчик для stdout
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    
    # Обработчик для файла (если нужно)
    # file_handler = logging.FileHandler("linguist.log")
    # file_handler.setFormatter(formatter)
    
    logger.addHandler(stream_handler)
    # logger.addHandler(file_handler)
    
    return logger

# Глобальный логгер
logger = setup_logger()