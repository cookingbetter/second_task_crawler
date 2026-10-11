"""Модели данных краулера."""

from __future__ import annotations

from dataclasses import dataclass

ERROR_TIMEOUT = "timeout"
ERROR_HTTP = "http"
ERROR_NETWORK = "network"
ERROR_ROBOTS = "robots"
ERROR_PARSE = "parse"
# Домен отключён circuit breaker'ом: запроса в сеть не было.
ERROR_CIRCUIT = "circuit"
ERROR_UNKNOWN = "unknown"


@dataclass(slots=True)
class FetchResult:
    """Результат загрузки одного URL: либо тело ответа, либо описание ошибки."""

    url: str
    status: int | None = None
    text: str | None = None
    elapsed: float = 0.0
    error: str | None = None
    error_type: str | None = None
    truncated: bool = False
    # URL после редиректов: именно он служит базой для относительных ссылок.
    final_url: str | None = None
    # Сколько запрос прождал разрешения у лимитера скорости.
    waited: float = 0.0
    # Агент, под которым ушёл запрос: при ротации он у страниц разный.
    user_agent: str | None = None
    # Сколько попыток ушло на этот результат: 1 — повторов не было.
    attempts: int = 1
    # Класс ошибки из src/errors.py: по нему видно, временная она или нет.
    error_class: str | None = None
    # Суммарная пауза между попытками (без времени самих запросов).
    retry_waited: float = 0.0

    @property
    def ok(self) -> bool:
        return self.error is None


@dataclass(slots=True)
class CrawlStats:
    """Сводная статистика по одному запуску загрузки."""

    total: int = 0
    succeeded: int = 0
    failed: int = 0
    elapsed: float = 0.0

    @classmethod
    def from_results(cls, results: list[FetchResult], elapsed: float) -> CrawlStats:
        succeeded = sum(1 for result in results if result.ok)
        return cls(
            total=len(results),
            succeeded=succeeded,
            failed=len(results) - succeeded,
            elapsed=elapsed,
        )
