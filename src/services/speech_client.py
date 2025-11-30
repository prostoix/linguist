import asyncio
import json
import logging
import base64
from typing import Optional
import aiohttp

logger = logging.getLogger("linguist.speech")

class SaluteSpeechClient:
    """Клиент для SaluteSpeech API с Authorization Key"""
    
    def __init__(self, auth_key: str):
        self.auth_key = auth_key
        self.api_url = "https://smartspeech.sber.ru/rest/v1"
        self.headers = {
            "Authorization": f"Bearer {auth_key}",
            "Content-Type": "application/json"
        }
    
    async def recognize_audio(self, audio_data: bytes, audio_format: str = "wav") -> str:
        """
        Распознавание аудио через SaluteSpeech REST API
        
        Args:
            audio_data: Байты аудио файла
            audio_format: Формат аудио (wav, mp3, etc)
            
        Returns:
            Распознанный текст
        """
        try:
            logger.info(f"🔊 Отправка аудио в SaluteSpeech: {len(audio_data)} байт")
            
            # Кодируем аудио в base64
            audio_base64 = base64.b64encode(audio_data).decode('utf-8')
            
            # Подготовка запроса
            request_data = {
                "model": "general",
                "audio": {
                    "data": audio_base64,
                    "format": audio_format.upper()
                },
                "options": {
                    "language": "ru-RU",
                    "profanity_filter": True,
                    "literature_text": True
                }
            }
            
            # Отправка запроса
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self.api_url}/data:recognize",
                    headers=self.headers,
                    json=request_data,
                    timeout=30,
                    ssl=False
                ) as response:
                    
                    if response.status == 200:
                        result = await response.json()
                        recognized_text = result.get("result", "")
                        
                        if recognized_text:
                            logger.info(f"✅ Распознано: {recognized_text}")
                            return recognized_text
                        else:
                            logger.warning("⚠️ Пустой ответ от SaluteSpeech")
                            return "Текст не распознан"
                    
                    elif response.status == 401:
                        logger.error("❌ Неверный Authorization Key")
                        raise Exception("Invalid Authorization Key")
                    
                    else:
                        error_text = await response.text()
                        logger.error(f"❌ Ошибка SaluteSpeech: {response.status} - {error_text}")
                        raise Exception(f"SaluteSpeech API error: {response.status}")
                        
        except asyncio.TimeoutError:
            logger.error("⏰ Таймаут подключения к SaluteSpeech")
            raise
        except Exception as e:
            logger.error(f"❌ Ошибка распознавания: {e}")
            raise
    
    async def health_check(self) -> bool:
        """Проверка доступности SaluteSpeech"""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{self.api_url}/data:status",
                    headers=self.headers,
                    timeout=10,
                    ssl=False
                ) as response:
                    return response.status == 200
        except Exception as e:
            logger.error(f"❌ Health check failed: {e}")
            return False