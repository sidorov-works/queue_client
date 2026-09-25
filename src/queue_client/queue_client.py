# src/queue_client/queue_client.py

from abc import ABC, abstractmethod
from typing import Optional, Dict, List, Any
import json
import asyncio
import redis.exceptions
from redis.asyncio import Redis, ConnectionPool
from custom_json_encoder import dumps

import logging
logger = logging.getLogger(__name__)

class BaseQueueClient(ABC):
    """Абстрактный интерфейс для брокера очередей"""
    
    @abstractmethod
    async def push(self, message: Dict[str, Any]) -> bool:
        """Добавление сообщения в очередь"""
        pass

    @abstractmethod
    async def push_batch(self, messages: List[Dict[str, Any]]) -> bool:
        """Добавление сообщения в очередь"""
        pass

    @abstractmethod
    async def push_front(self, message: Dict[str, Any]) -> bool:
        """Возврат сообщения в начало очереди"""
        pass

    @abstractmethod
    async def push_front_batch(self, messages: List[Dict[str, Any]]) -> bool:
        """Добавление сообщения в очередь"""
        pass
    
    @abstractmethod
    async def pop(self, timeout: int) -> Optional[Dict[str, Any]]:
        """Извлечение сообщения из очереди"""
        pass
    
    @abstractmethod
    async def connect(self):
        """Подключение к брокеру очередей"""
        pass
    
    @abstractmethod
    async def close(self):
        """Закрытие соединения"""
        pass
    
    @abstractmethod
    async def size(self) -> int:
        """Текущий размер очереди"""
        pass
    
    @abstractmethod
    async def clear(self) -> bool:
        """Полная очистка очереди"""
        pass

    @abstractmethod
    async def is_overflowed_or_unavailable(self) -> bool:
        """Проверка переполнения очереди"""
        pass

    @abstractmethod
    def get_name(self) -> str:
        """Возвращает имя очереди"""
        pass


class RedisQueueClient(BaseQueueClient):
    """Клиент для работы с очередями Redis на основе пула соединений
    
    Особенности:
    - Использует общий пул соединений из shared/redis_pool.py
    - Потокобезопасные операции (блокировка на уровне клиента)
    - Автоматические повторы операций при временных ошибках
    - Ленивая инициализация подключения
    """
    
    def __init__(
            self, 
            name: str, 
            connection_pool: ConnectionPool,
            max_retries: int = 3,
            prefix: Optional[str] = None,
            maxlen: Optional[int] = None
            ):
        """
        Args:
            name: Название очереди
            maxlen: Максимальный размер очереди (None - без ограничений)
        """
        self.queue_name = f"{prefix}:{name}" if prefix else name
        self._maxlen = maxlen
        self._max_retries = max_retries
        self._redis = None  # Ленивая инициализация
        self._connection_pool = connection_pool
        self._connection_lock = asyncio.Lock()

    async def connect(self):
        """Лениво создает подключение. Если Redis недоступен - бросает исключение."""
        async with self._connection_lock:
            if self._redis is not None:
                try:
                    # Быстрая проверка, что соединение живо
                    await asyncio.wait_for(self._redis.ping(), timeout=1.0)
                    return
                except (asyncio.TimeoutError, redis.exceptions.RedisError):
                    # Соединение мертво, сбрасываем его
                    self._redis = None
                    # Не бросаем исключение здесь, пробуем переподключиться ниже
            
            # Создаем новое соединение
            try:
                self._redis = Redis(connection_pool=self._connection_pool)
                await asyncio.wait_for(self._redis.ping(), timeout=1.0)
            except Exception as e:
                self._redis = None
                logger.error(f"Failed to connect to Redis: {e}")
                raise redis.exceptions.ConnectionError(f"Redis connection failed: {e}") from e

    async def close(self):
        """Аккуратно закрывает соединение, но оставляет возможность переподключения."""
        async with self._connection_lock:
            if self._redis is not None:
                try:
                    await self._redis.close()
                except Exception as e:
                    logger.warning(f"Error closing Redis connection: {e}")
                finally:
                    self._redis = None  # Клиент может быть использован снова!

    async def _execute_with_retry(self, operation_name: str, *args, **kwargs):
        """Выполняет операцию с повторами при временных ошибках."""
        max_retries = self._max_retries
        
        for attempt in range(max_retries):
            try:
                # Всегда пытаемся получить рабочее соединение
                await self.connect()
                # Получаем метод по имени ПОСЛЕ успешного connect()
                operation = getattr(self._redis, operation_name)
                return await operation(*args, **kwargs)
            except (redis.exceptions.ConnectionError, 
                    redis.exceptions.TimeoutError,
                    redis.exceptions.ResponseError,
                    AttributeError) as e:  # Добавляем AttributeError для случая self._redis = None
                
                # Сбрасываем клиент при ошибках соединения
                async with self._connection_lock:
                    self._redis = None
                
                if attempt == max_retries - 1:
                    logger.error(f"Operation {operation_name} failed after {max_retries} attempts: {e}")
                    raise
                
                delay = 0.5 * (2 ** attempt)  # Экспоненциальная задержка
                logger.warning(f"Retry {attempt + 1} after error: {e}")
                await asyncio.sleep(delay)

    # Реализация основных методов интерфейса

    async def push(self, message: Dict[str, Any]) -> bool:
        """Добавляет сообщение в конец очереди"""
        try:
            json_data = dumps(message)
            # Передаём имя метода вместо self._redis.rpush
            return await self._execute_with_retry(
                "rpush",  # Имя метода Redis
                self.queue_name, 
                json_data
            )
        except (TypeError, ValueError) as e:
            logger.error(f"Serialization error: {e}")
            return False
        except Exception as e:
            logger.error(f"Push operation failed: {e}")
            return False
        
    async def push_front(self, message: Dict[str, Any]) -> bool:
        """Добавляет сообщение в начало очереди (высший приоритет)"""
        try:
            json_data = dumps(message)
            # Передаём имя метода вместо self._redis.lpush
            return await self._execute_with_retry(
                "lpush",  # Имя метода Redis
                self.queue_name,
                json_data
            )
        except Exception as e:
            logger.error(f"Push_front failed: {e}")
            return False

    async def pop(self, timeout: int = 5) -> Optional[Dict[str, Any]]:
        """Извлекает сообщение из начала очереди с таймаутом"""
        try:
            # Передаём имя метода вместо self._redis.blpop
            result = await self._execute_with_retry(
                "blpop",  # Имя метода Redis
                self.queue_name,
                timeout=timeout
            )
            return json.loads(result[1]) if result else None
        except (asyncio.TimeoutError, redis.exceptions.TimeoutError):
            return None  # Таймаут - нормальная ситуация
        except Exception as e:
            logger.error(f"Pop operation failed: {e}")
            return None

    async def size(self) -> Optional[int]:
        """Возвращает текущее количество элементов в очереди"""
        try:
            # Передаём имя метода вместо self._redis.llen
            return await self._execute_with_retry(
                "llen",  # Имя метода Redis
                self.queue_name
            )
        except Exception as e:
            logger.error(f"Size operation failed: {e}")
            return None

    async def clear(self) -> bool:
        """Полностью очищает очередь"""
        try:
            # Передаём имя метода вместо self._redis.delete
            deleted = await self._execute_with_retry(
                "delete",  # Имя метода Redis
                self.queue_name
            )
            return deleted > 0
        except Exception as e:
            logger.error(f"Clear operation failed: {e}")
            return False
        
    async def is_overflowed_or_unavailable(self) -> bool:
        """
        Проверяет, достигла ли очередь максимального размера.
        Если проверить не удалось - считаем, что переполнена.
        """
        if self._maxlen is None:
            return False
        size_result = await self.size()
        # Если проверить не удалось, считаем что очередь недоступна
        if size_result is None:
            return True
        return size_result >= self._maxlen
    
    def get_name(self) -> str:
        """Возвращает имя очереди"""
        return self.queue_name
    
    async def _execute_pipeline_with_retry(self, pipeline_commands_func):
        """
        Выполняет pipeline операции с той же логикой retry, что и _execute_with_retry
        
        Args:
            pipeline_commands_func: Функция, которая принимает pipeline объект
                                и добавляет в него команды
        """
        max_retries = self._max_retries
        
        for attempt in range(max_retries):
            try:
                await self.connect()
                
                async with self._redis.pipeline(transaction=True) as pipe:
                    # Добавляем команды в pipeline
                    await pipeline_commands_func(pipe)
                    # Выполняем
                    return await pipe.execute()
                    
            except (redis.exceptions.ConnectionError,
                    redis.exceptions.TimeoutError,
                    redis.exceptions.ResponseError,
                    AttributeError) as e:
                
                async with self._connection_lock:
                    self._redis = None
                
                if attempt == max_retries - 1:
                    logger.error(f"Pipeline operation failed after {max_retries} attempts: {e}")
                    raise
                
                delay = 0.5 * (2 ** attempt)
                logger.warning(f"Pipeline retry {attempt + 1} after error: {e}")
                await asyncio.sleep(delay)

    async def push_batch(self, messages: List[Dict[str, Any]]) -> bool:
        """
        Атомарно отправляет батч сообщений в очередь.
        """
        if not messages:
            return True
            
        try:
            # Сериализуем все сообщения
            json_messages = []
            for message in messages:
                try:
                    json_data = dumps(message)
                    json_messages.append(json_data)
                except (TypeError, ValueError) as e:
                    logger.error(f"Serialization error in batch: {e}")
                    return False
            
            # Определяем функцию для добавления команд в pipeline
            async def add_batch_commands(pipe):
                for json_data in json_messages:
                    pipe.rpush(self.queue_name, json_data)
            
            # Выполняем с retry
            results = await self._execute_pipeline_with_retry(add_batch_commands)
            
            # Проверяем результаты
            if results and all(isinstance(r, int) for r in results):
                logger.debug(f"Batch push successful: {len(messages)} messages")
                return True
            else:
                logger.error(f"Batch push returned unexpected results: {results}")
                return False
            
        except Exception as e:
            logger.error(f"Batch push operation failed: {e}")
            return False
        
    async def push_front_batch(self, messages: List[Dict[str, Any]]) -> bool:
        """
        Атомарно возвращает батч сообщений в начало очереди (высший приоритет).
        
        Полезно для возврата задач при ошибках обработки батча.
        
        Args:
            messages: Список сообщений для возврата в начало очереди
            
        Returns:
            True если все сообщения успешно отправлены, False при ошибке
        """
        if not messages:
            return True
            
        try:
            # Сериализуем все сообщения
            json_messages = []
            for message in messages:
                try:
                    json_data = dumps(message)
                    json_messages.append(json_data)
                except (TypeError, ValueError) as e:
                    logger.error(f"Serialization error in batch: {e}")
                    return False
            
            # Определяем функцию для добавления команд в pipeline
            async def add_batch_front_commands(pipe):
                # Важно: добавляем в обратном порядке, чтобы сохранить порядок сообщений
                for json_data in reversed(json_messages):
                    pipe.lpush(self.queue_name, json_data)
            
            # Выполняем с retry
            results = await self._execute_pipeline_with_retry(add_batch_front_commands)
            
            # Проверяем результаты
            if results and all(isinstance(r, int) for r in results):
                logger.debug(f"Batch push_front successful: {len(messages)} messages")
                return True
            else:
                logger.error(f"Batch push_front returned unexpected results: {results}")
                return False
            
        except Exception as e:
            logger.error(f"Batch push_front operation failed: {e}")
            return False


def get_queue_client(
        name: str,
        connection_pool: ConnectionPool,
        prefix: Optional[str] = None,
        maxlen: Optional[int] = None
        ) -> BaseQueueClient:
    """Фабрика для создания клиента очереди"""
    return RedisQueueClient(name, prefix, connection_pool=connection_pool, maxlen=maxlen)