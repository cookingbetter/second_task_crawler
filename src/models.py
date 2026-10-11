"""Модели данных краулера."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field, fields
from datetime import datetime
from typing import Any

ERROR_TIMEOUT = "timeout"
ERROR_HTTP = "http"
ERROR_NETWORK = "network"
ERROR_ROBOTS = "robots"
ERROR_PARSE = "parse"
# Домен отключён circuit breaker'ом: запроса в сеть не было.
ERROR_CIRCUIT = "circuit"
# Страница получена, но сохранить её не удалось: диск, права, база.
ERROR_STORAGE = "storage"
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
    # Тип содержимого из заголовка ответа — его сохраняют вместе со страницей.
    content_type: str | None = None
    # Когда пришёл ответ. Записывается при загрузке, а не при сохранении:
    # страница может пролежать в буфере, и время записи — это не время обхода.
    fetched_at: datetime | None = None

    @property
    def ok(self) -> bool:
        return self.error is None


@dataclass(slots=True)
class PageRecord:
    """Стандартная форма сохраняемых данных: одна страница — одна запись.

    Первые восемь полей — договор дня 6, одинаковый для JSON, CSV и базы.
    Остальные служебные: без них по сохранённым данным не понять, с какой
    глубины пришла страница и почему она пустая.

    Порядок полей важен: по нему строятся колонки CSV и столбцы таблицы.
    """

    url: str
    title: str = ""
    text: str = ""
    links: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    crawled_at: datetime = field(default_factory=datetime.now)
    status_code: int | None = None
    content_type: str | None = None
    final_url: str | None = None
    depth: int = 0
    ok: bool = True
    error: str | None = None
    error_class: str | None = None
    attempts: int = 1
    elapsed: float = 0.0

    @classmethod
    def field_names(cls) -> list[str]:
        """Имена полей в порядке объявления — заголовки CSV и колонки таблицы."""
        return [item.name for item in fields(cls)]

    @classmethod
    def from_page(
        cls, page: dict[str, Any], *, text_limit: int | None = None
    ) -> PageRecord:
        """Собирает запись из словаря страницы, который отдаёт краулер.

        Имена полей не совпадают с ключами страницы (`status` -> `status_code`,
        `fetched_at` -> `crawled_at`): словарь страницы описывает загрузку, а
        запись — сохранённые данные, и переименование здесь единственное место,
        где эти два словаря встречаются.
        """
        url = page.get("url")
        if not url:
            raise ValueError("у страницы нет URL, сохранять такую запись нельзя")

        text = page.get("text") or ""
        if text_limit is not None and len(text) > text_limit:
            text = text[:text_limit]

        return cls(
            url=url,
            title=page.get("title") or "",
            text=text,
            links=list(page.get("links") or []),
            metadata=dict(page.get("metadata") or {}),
            crawled_at=page.get("fetched_at") or datetime.now(),
            status_code=page.get("status"),
            content_type=page.get("content_type"),
            final_url=page.get("final_url"),
            depth=page.get("depth", 0),
            ok=bool(page.get("ok")),
            error=page.get("error"),
            error_class=page.get("error_class"),
            attempts=page.get("attempts", 1),
            elapsed=page.get("elapsed", 0.0),
        )

    def as_dict(self) -> dict[str, Any]:
        """Запись словарём: в таком виде её принимают все хранилища."""
        return asdict(self)


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
