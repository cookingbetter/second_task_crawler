"""Вспомогательные функции: логирование, замер времени, форматирование вывода."""

from __future__ import annotations

import json
import logging
import sys
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.models import CrawlStats, FetchResult

logger = logging.getLogger(__name__)

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


def format_progress(stats: dict[str, Any]) -> str:
    """Строка прогресса обхода: что сделано, что осталось, с какой скоростью."""
    return (
        f"Страниц: {stats.get('processed', 0)}"
        f" | в очереди: {stats.get('pending', 0)}"
        f" | в работе: {stats.get('in_progress', 0)}"
        f" | ошибок: {stats.get('failed', 0)}"
        f" | {stats.get('pages_per_sec', 0.0):.1f} стр/с"
        f" | глубина: {stats.get('max_depth_reached', 0)}"
    )


def print_progress(stats: dict[str, Any]) -> None:
    """Показывает прогресс обхода: в терминале — строкой на месте.

    Возврат каретки годится только для терминала: в файле или пайпе все
    обновления склеились бы в одну строку, поэтому там каждое печатается
    отдельно. Печать идёт в stdout, а логи — в stderr, так что при
    перенаправлении вывода они не перемешиваются.
    """
    line = format_progress(stats)
    if sys.stdout.isatty():
        # Хвост из пробелов затирает остаток предыдущей, более длинной строки.
        sys.stdout.write(f"\r{line}   ")
    else:
        sys.stdout.write(f"{line}\n")
    sys.stdout.flush()


def format_crawl_stats(stats: dict[str, Any], title: str = "Итог обхода") -> str:
    """Многострочная сводка после обхода: страницы, пропуски, семафоры."""
    skipped: dict[str, int] = stats.get("skipped") or {}
    semaphores: dict[str, Any] = stats.get("semaphores") or {}
    peak_by_host: dict[str, int] = semaphores.get("peak_by_host") or {}

    lines = [
        f"{title}:",
        f"  Загружено страниц: {stats.get('processed', 0)}"
        f" | ошибок: {stats.get('failed', 0)}"
        f" | осталось в очереди: {stats.get('pending', 0)}",
        f"  Найдено URL: {stats.get('seen', 0)}"
        f" | максимальная глубина: {stats.get('max_depth_reached', 0)}"
        f" | время: {stats.get('elapsed', 0.0):.2f} с"
        f" | {stats.get('pages_per_sec', 0.0):.1f} стр/с",
    ]

    if skipped or stats.get("skipped_duplicates"):
        parts = [f"{name}={count}" for name, count in sorted(skipped.items())]
        parts.append(f"дубликатов в очереди={stats.get('skipped_duplicates', 0)}")
        lines.append(f"  Пропущено ссылок: {', '.join(parts)}")

    if semaphores:
        lines.append(
            f"  Конкурентность: лимит {semaphores.get('max_concurrent')}"
            f" (на домен {semaphores.get('max_per_domain')})"
            f", пик {semaphores.get('peak_active', 0)}"
            f", домены {semaphores.get('hosts', 0)}"
        )
    for host, peak in sorted(peak_by_host.items(), key=lambda item: -item[1])[:3]:
        lines.append(f"    {host}: пик {peak}")

    return "\n".join(lines)


def save_crawl_results(
    pages: list[dict[str, Any]],
    stats: dict[str, Any],
    path: str | Path,
) -> Path:
    """Сохраняет страницы и статистику обхода в один JSON-файл.

    Намеренно простой синхронный `json.dump`: полноценный слой сохранения
    (потоковая запись, форматы, база данных) — тема дня 5.
    """
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "stats": stats,
        "pages_count": len(pages),
        "pages": pages,
    }
    with target.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2, default=str)

    logger.info("Результаты обхода сохранены: %s (%d страниц)", target, len(pages))
    return target


def _shorten(text: str, limit: int) -> str:
    return text if len(text) <= limit else text[: limit - 3] + "..."
