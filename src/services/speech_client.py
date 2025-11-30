import asyncio
import json
import logging
import base64
from typing import Optional
import aiohttp

logger = logging.getLogger("linguist.speech")

class SaluteSpeechClient:
    """Клиент для SaluteSpeech API с OAuth аутентификацией"""
    
    def __init__(self, client_id: str, client_secret: str, scope: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self.scope = scope
        self.token_url = "https://ngw.devices.sberbank.ru:9443/api/v2/oauth"
        self.api_url = "https://smartspeech.sber.ru/rest/v1"
        self.access_token = None
        self.token_expires = 0
    
    async def _get_access_token(self) -> str:
        """Получение OAuth токена"""
        try:
            # Кодируем client_id:client_secret в base64
            credentials = base64.b64encode(
                f"{self.client_id}:{self.client_secret}".encode()
            ).decode()
            
            headers = {
                "Authorization": f"Basic {credentials}",
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/json"
            }
            
            data = {
                "scope": self.scope
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self.token_url,
                    headers=headers,
                    data=data,
                    ssl=False  # Важно для их самоподписанного сертификата
                ) as response:
                    
                    if response.status == 200:
                        token_data = await response.json()
                        self.access_token = token_data.get("access_token")
                        # Токен живет 1 час, обновляем через 50 минут
                        self.token_expires = asyncio.get_event_loop().time() + 3000
                        logger.info("✅ OAuth токен получен успешно")
                        return self.access_token
                    else:
                        error_text = await response.text()
                        logger.error(f"❌ Ошибка получения токена: {response.status} - {error_text}")
                        raise Exception(f"Token error: {response.status}")
                        
        except Exception as e:
            logger.error(f"❌ Ошибка аутентификации: {e}")
            raise
    
    async def _ensure_token_valid(self):
        """Проверка и обновление токена при необходимости"""
        if not self.access_token or asyncio.get_event_loop().time() >= self.token_expires:
            await self._get_access_token()
    
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
            await self._ensure_token_valid()
            
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
            
            headers = {
                "Authorization": f"Bearer {self.access_token}",
                "Content-Type": "application/json"
            }
            
            # Отправка запроса
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self.api_url}/data:recognize",
                    headers=headers,
                    json=request_data,
                    timeout=30,
                    ssl=False  # Для их сертификата
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
                        # Токен протух, пробуем обновить
                        logger.warning("🔄 Токен истек, обновляем...")
                        await self._get_access_token()
                        return await self.recognize_audio(audio_data, audio_format)
                    
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
            await self._ensure_token_valid()
            
            headers = {
                "Authorization": f"Bearer {self.access_token}"
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{self.api_url}/data:status",
                    headers=headers,
                    timeout=10,
                    ssl=False
                ) as response:
                    return response.status == 200
                    
        except Exception as e:
            logger.error(f"❌ Health check failed: {e}")
            return False