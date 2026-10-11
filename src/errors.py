"""Классификация ошибок загрузки: что имеет смысл повторять, а что нет.

Строки `error_type` из дня 1 (`timeout`, `http`, `network`, `unknown`)
сохраняются: на них опираются таблицы вывода и тесты. Класс исключения отвечает
на другой вопрос — временная это беда или окончательная, — и именно по классу
[retry.py](retry.py) решает, повторять ли запрос.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any

import aiohttp

from src.models import (
    ERROR_CIRCUIT,
    ERROR_HTTP,
    ERROR_NETWORK,
    ERROR_PARSE,
    ERROR_ROBOTS,
    ERROR_STORAGE,
    ERROR_TIMEOUT,
    ERROR_UNKNOWN,
)

logger = logging.getLogger(__name__)

# Серверу тяжело прямо сейчас: запрос ушёл не вовремя, а не неправильно.
TRANSIENT_STATUSES = frozenset({408, 425})
TOO_MANY_REQUESTS = 429


class CrawlerError(Exception):
    """Ошибка загрузки с типом дня 1 и HTTP-статусом, если он известен.

    Поле `attempts` заполняет `RetryStrategy`: по нему видно, сколько попыток
    стоило за этой ошибкой.
    """

    error_type: str = ERROR_UNKNOWN

    def __init__(
        self,
        message: str,
        *,
        error_type: str | None = None,
        status: int | None = None,
        retry_after: float | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        if error_type is not None:
            self.error_type = error_type
        self.status = status
        # Сколько ждать по требованию сервера (заголовок Retry-After).
        self.retry_after = retry_after
        self.attempts = 1
        # Что успела узнать неудачная попытка: время запроса и ожидание в
        # лимитере. Нужно, чтобы итоговый FetchResult не терял эти цифры.
        self.elapsed = 0.0
        self.waited = 0.0

    @property
    def error_class(self) -> str:
        return type(self).__name__


class TransientError(CrawlerError):
    """Временная ошибка: та же самая попытка позже может удаться."""

    error_type = ERROR_HTTP


class CrawlerTimeoutError(TransientError):
    """Сервер не ответил за отведённое время.

    Имя с префиксом не случайно: встроенный `TimeoutError` в Python 3.11 — это и
    есть `asyncio.TimeoutError`, перекрывать его своим классом нельзя.
    """

    error_type = ERROR_TIMEOUT


class RateLimitError(TransientError):
    """429: мы сами попросили слишком много. Повтор — с увеличенной паузой."""


class ServerError(TransientError):
    """5xx: сервер сломался на своей стороне. Повторяем ограниченное число раз."""


class PermanentError(CrawlerError):
    """Ответ не изменится от повтора: 404, 403, 401 и прочие 4xx."""

    error_type = ERROR_HTTP


class NetworkError(CrawlerError):
    """Сеть: DNS не разрешился, соединение отвергнуто или разорвано."""

    error_type = ERROR_NETWORK


class ParseError(CrawlerError):
    """Ответ получен, но разобрать его не удалось.

    Повторять бессмысленно: второй раз придёт тот же HTML.
    """

    error_type = ERROR_PARSE


class CircuitOpenError(CrawlerError):
    """Домен временно отключён circuit breaker'ом: запроса в сеть не было."""

    error_type = ERROR_CIRCUIT


class RobotsBlockedError(CrawlerError):
    """robots.txt запретил загрузку: запроса в сеть не было.

    Как исключение не поднимается — краулер отказывает ещё до попытки, — но
    даёт отказу имя класса. Без него запрет попадал бы в отчёт «неизвестной
    ошибкой», хотя это сознательный отказ, а не сбой.
    """

    error_type = ERROR_ROBOTS


class StorageError(TransientError):
    """Сохранить страницу не удалось, но попытка имеет смысл.

    Переполненный диск освобождают, заблокированную базу отпускают, временный
    сбой файловой системы проходит — поэтому запись повторяется так же, как
    запрос к серверу, через `RetryStrategy`.
    """

    error_type = ERROR_STORAGE


class StorageDataError(PermanentError):
    """Сама запись не годится для сохранения: повтор ничего не изменит.

    Например, в данных оказался объект, который не переводится в JSON. Второй
    раз он переведётся ровно так же.
    """

    error_type = ERROR_STORAGE


def storage_error(exc: BaseException, *, target: str = "") -> CrawlerError:
    """Классифицирует сбой записи: повторять его или бессмысленно.

    `OSError` (и `sqlite3.OperationalError`, который до нас доходит как
    `OperationalError`) — про внешние обстоятельства, а `TypeError` и
    `ValueError` — про сами данные.
    """
    if isinstance(exc, CrawlerError):
        return exc

    where = f" ({target})" if target else ""
    detail = f"{type(exc).__name__}: {exc}{where}"
    if isinstance(exc, (TypeError, ValueError)):
        return StorageDataError(detail)
    return StorageError(detail)


def from_http_status(
    status: int,
    message: str = "",
    *,
    headers: Any = None,
) -> CrawlerError:
    """Выбирает класс ошибки по коду ответа.

    429 и 5xx — временные, но с разными политиками повторов: «слишком много
    запросов» просит подождать нас, а 500 означает поломку на той стороне.
    Остальные 4xx считаются окончательными: повтор вернёт тот же ответ.
    """
    text = message or f"HTTP {status}"
    detail = f"HTTP {status}: {message}" if message else text
    retry_after = parse_retry_after(_header(headers, "Retry-After"))

    if status == TOO_MANY_REQUESTS:
        return RateLimitError(detail, status=status, retry_after=retry_after)
    if status >= 500:
        return ServerError(detail, status=status, retry_after=retry_after)
    if status in TRANSIENT_STATUSES:
        return TransientError(detail, status=status, retry_after=retry_after)
    if status >= 400:
        return PermanentError(detail, status=status)
    # Сюда попадают только неожиданные коды: `raise_for_status` поднимает
    # исключение лишь на 4xx и 5xx.
    return CrawlerError(detail, error_type=ERROR_HTTP, status=status)


def from_exception(exc: BaseException) -> CrawlerError:
    """Приводит исключение к нашей классификации.

    Порядок проверок повторяет день 1: `ClientResponseError` — подкласс
    `ClientError`, поэтому идёт раньше, иначе HTTP-ошибки попали бы в сетевые.
    Таймаут проверяется первым: `aiohttp.ServerTimeoutError` наследуется и от
    `ClientError`, и от `asyncio.TimeoutError`.
    """
    if isinstance(exc, CrawlerError):
        return exc
    if isinstance(exc, asyncio.TimeoutError):
        return CrawlerTimeoutError("превышен таймаут")
    if isinstance(exc, aiohttp.ClientResponseError):
        return from_http_status(exc.status, exc.message or "", headers=exc.headers)
    if isinstance(exc, aiohttp.ClientError):
        return NetworkError(f"{type(exc).__name__}: {exc}")
    return CrawlerError(f"{type(exc).__name__}: {exc}", error_type=ERROR_UNKNOWN)


def parse_retry_after(value: str | None) -> float | None:
    """Разбирает заголовок `Retry-After`: секунды или HTTP-дата.

    Всё непонятное, включая отрицательные секунды, даёт `None` — тогда пауза
    считается по обычному backoff. А вот прошедшая дата осмысленна: она значит
    «уже можно», то есть 0 секунд, а не отрицательное ожидание.
    """
    if value is None:
        return None
    text = value.strip()
    if not text:
        return None

    try:
        seconds = float(text)
    except ValueError:
        pass
    else:
        return seconds if seconds >= 0 else None

    try:
        moment = parsedate_to_datetime(text)
    except (TypeError, ValueError):
        logger.debug("Непонятный Retry-After: %r", value)
        return None
    if moment is None:
        return None
    if moment.tzinfo is None:  # дата без зоны по RFC 7231 считается UTC
        moment = moment.replace(tzinfo=timezone.utc)
    return max(0.0, (moment - datetime.now(timezone.utc)).total_seconds())


def _header(headers: Any, name: str) -> str | None:
    """Достаёт заголовок из любого словареподобного объекта или из None."""
    if headers is None:
        return None
    getter = getattr(headers, "get", None)
    if getter is None:
        return None
    value = getter(name)
    return value if isinstance(value, str) else None
