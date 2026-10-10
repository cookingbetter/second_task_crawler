"""Вспомогательные функции: логирование, замер времени, форматирование вывода."""

from __future__ import annotations

import logging
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

from src.models import CrawlStats, FetchResult

LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
DATE_FORMAT = "%H:%M:%S"


def setup_logging(level: str | int = "INFO", log_file: str | None = None) -> None:
    """Настраивает корневой логгер, переустанавливая хендлеры при повторном вызове."""
    handlers: list[logging.Handler] = [logging.StreamHandler()]
    if log_file:
        handlers.append(logging.FileHandler(log_file, encoding="utf-8"))

    logging.basicConfig(
        level=level,
        format=LOG_FORMAT,
        datefmt=DATE_FORMAT,
        handlers=handlers,
        force=True,
    )


@dataclass(slots=True)
class Elapsed:
    """Держатель измеренного времени: заполняется при выходе из `timer()`."""

    seconds: float = 0.0


@contextmanager
def timer() -> Iterator[Elapsed]:
    """Измеряет время выполнения блока по монотонным часам."""
    elapsed = Elapsed()
    started = time.perf_counter()
    try:
        yield elapsed
    finally:
        elapsed.seconds = time.perf_counter() - started


def format_stats(stats: CrawlStats, title: str = "Статистика") -> str:
    return (
        f"{title}: всего {stats.total}, успешно {stats.succeeded}, "
        f"ошибок {stats.failed}, время {stats.elapsed:.2f} с"
    )


def format_results_table(results: list[FetchResult]) -> str:
    """Собирает таблицу по результатам: статус, время, размер тела или ошибка."""
    header = f"{'URL':<45} {'Статус':<8} {'Время':>8}  Детали"
    lines = [header, "-" * len(header)]

    for result in results:
        url = result.url if len(result.url) <= 45 else result.url[:42] + "..."
        if result.ok:
            status = str(result.status or "")
            details = f"{len(result.text or '')} байт"
            if result.truncated:
                details += " (обрезано)"
        else:
            status = str(result.status) if result.status else "-"
            details = f"[{result.error_type}] {result.error}"
        lines.append(f"{url:<45} {status:<8} {result.elapsed:>7.2f}s  {details}")

    return "\n".join(lines)


def format_page_summary(page: dict[str, Any], *, max_links: int = 5) -> str:
    """Собирает читаемую сводку по одной разобранной странице."""
    lines = [f"URL: {page.get('url', '')}"]

    if not page.get("ok", True):
        lines.append(f"  Ошибка: [{page.get('error_type')}] {page.get('error')}")
        return "\n".join(lines)

    lines.append(f"  Заголовок: {page.get('title') or '—'}")
    lines.append(
        f"  Текст: {page.get('text_length', 0)} симв."
        f" | ссылок: {page.get('links_count', 0)}"
        f" | изображений: {page.get('images_count', 0)}"
    )

    headings: dict[str, list[str]] = page.get("headings") or {}
    counts = ", ".join(f"{name}={len(items)}" for name, items in headings.items())
    lines.append(
        f"  Заголовки: {counts or '—'}"
        f" | таблиц: {len(page.get('tables') or [])}"
        f" | списков: {len(page.get('lists') or [])}"
    )

    metadata: dict[str, Any] = page.get("metadata") or {}
    if description := metadata.get("description"):
        lines.append(f"  Описание: {_shorten(description, 70)}")
    if keywords := metadata.get("keywords"):
        lines.append(f"  Ключевые слова: {', '.join(keywords)}")

    links: list[str] = page.get("links") or []
    for link in links[:max_links]:
        lines.append(f"    - {link}")
    if len(links) > max_links:
        lines.append(f"    ... и ещё {len(links) - max_links}")

    if page.get("truncated"):
        lines.append("  Внимание: тело ответа было обрезано по лимиту размера")
    for note in page.get("warnings") or []:
        lines.append(f"  Предупреждение парсинга: {note}")

    return "\n".join(lines)


def format_parse_stats(pages: list[dict[str, Any]]) -> str:
    """Агрегирует итоги по всем разобранным страницам."""
    parsed = [page for page in pages if page.get("ok")]
    if not parsed:
        return "Парсинг: ни одна страница не разобрана"

    links = sum(page.get("links_count", 0) for page in parsed)
    images = sum(page.get("images_count", 0) for page in parsed)
    text = sum(page.get("text_length", 0) for page in parsed)
    return (
        f"Парсинг: страниц {len(parsed)}/{len(pages)}, ссылок {links}, "
        f"изображений {images}, текста {text} симв. "
        f"(в среднем {text // len(parsed)} на страницу)"
    )


def _shorten(text: str, limit: int) -> str:
    return text if len(text) <= limit else text[: limit - 3] + "..."
