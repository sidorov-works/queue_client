# src/queue_client/custom_json_encoder.py

import json
from datetime import datetime, date
from decimal import Decimal
from typing import Any

class CustomJSONEncoder(json.JSONEncoder):
    """
    Кастомный JSON энкодер для сериализации нестандартных типов Python.
    
    Обеспечивает корректную работу с типами данных, которые часто используются
    в системе микросервисов техподдержки, но не поддерживаются стандартным json.dumps().
    """
    def default(self, obj: Any) -> Any:
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return float(obj)
        if hasattr(obj, 'dict'):  # Для моделей Pydantic
            return obj.dict()
        return super().default(obj)

def dumps(data: Any) -> str:
    """
    Сериализует Python объект в JSON строку с поддержкой специальных типов.
    
    Используется преимущественно для подготовки данных к отправке в Redis очереди,
    где требуется надежная сериализация сложных объектов.
    
    Поддерживаемые типы:
    - datetime, date → ISO формат строки (например, "2023-12-15T10:30:00")
    - Decimal → float (для финансовых вычислений)
    - Pydantic модели → автоматически вызывает метод .dict()
    - Все стандартные JSON-совместимые типы
    
    Args:
        data: Любой Python объект для сериализации. Может содержать даты,
              Decimal числа, Pydantic модели и вложенные структуры.
    
    Returns:
        JSON-строка, готовую для сохранения в Redis или передачи между сервисами.
    
    Raises:
        TypeError: Если объект содержит неподдерживаемые типы, которые не могут
                  быть сериализованы даже кастомным энкодером.
    
    Examples:
        >>> from datetime import datetime
        >>> from decimal import Decimal
        >>> from pydantic import BaseModel
        >>>
        >>> class User(BaseModel):
        ...     name: str
        ...     created_at: datetime
        >>>
        >>> user = User(name="John", created_at=datetime.now())
        >>> data = {
        ...     "user": user,
        ...     "price": Decimal("99.99"),
        ...     "timestamp": datetime.now()
        ... }
        >>>
        >>> json_str = dumps(data)
        >>> # Результат: корректная JSON строка со всеми специальными типами
    
    Note:
        Критически важен для работы очередей Redis в системе, так как стандартный
        json.dumps() не может сериализовать объекты с датами и Pydantic моделями.
    """
    return json.dumps(data, cls=CustomJSONEncoder)