from __future__ import annotations

class ContactOutError(Exception):
    """Базовое исключение для ContactOut-клиента."""
    pass

class BadCredentialsError(ContactOutError):
    """400 — плохие креденшлы / хедеры."""
    pass

class BadRequestError(ContactOutError):
    """401 — неверный запрос / входные данные."""
    pass

class OutOfCreditsError(ContactOutError):
    """403 — нет кредитов."""
    pass

class NoAccessError(ContactOutError):
    """403 — нет доступа к эндпоинту."""
    pass

class RateLimitError(ContactOutError):
    """429 — rate limit. Атрибут retry_after содержит секунды, если есть заголовок Retry-After."""
    def __init__(self, message: str = "", retry_after: int | None = None):
        super().__init__(message)
        self.retry_after = retry_after

class RemoteServerError(ContactOutError):
    """5xx ошибки на стороне ContactOut."""
    pass
