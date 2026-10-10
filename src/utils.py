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
from src.ratelimit import GLOBAL_KEY as GLOBAL_RATE_KEY

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
    """Строка прогресса обхода: что сделано, что осталось, с какой скоростью.

    Про вежливость пишется только когда она включена: иначе строка для дней 1-3
    обросла бы нулями, которые ничего не значат.
    """
    line = (
        f"Страниц: {stats.get('processed', 0)}"
        f" | в очереди: {stats.get('pending', 0)}"
        f" | в работе: {stats.get('in_progress', 0)}"
        f" | ошибок: {stats.get('failed', 0)}"
        f" | {stats.get('pages_per_sec', 0.0):.1f} стр/с"
        f" | глубина: {stats.get('max_depth_reached', 0)}"
    )

    rate: dict[str, Any] = stats.get("rate_limit") or {}
    if rate.get("enabled"):
        line += f" | ожидание: {rate.get('avg_wait', 0.0):.2f} с"
    robots: dict[str, Any] = stats.get("robots") or {}
    if robots.get("enabled"):
        line += f" | robots: {robots.get('blocked', 0)}"
    return line


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

    rate: dict[str, Any] = stats.get("rate_limit") or {}
    if rate.get("enabled") or rate.get("penalties"):
        lines.append(format_rate_stats(rate))
    robots: dict[str, Any] = stats.get("robots") or {}
    if robots.get("enabled"):
        lines.append(format_robots_stats(robots, blocked_urls=stats.get("blocked")))

    return "\n".join(lines)


def format_rate_stats(
    stats: dict[str, Any], title: str = "  Ограничение скорости"
) -> str:
    """Что лимитер требовал и во что это обошлось по времени."""
    limit = stats.get("requests_per_second")
    scope = "на домен" if stats.get("per_domain") else "общий"
    requests = stats.get("requests", 0)

    lines = [
        f"{title}: {f'{limit:.2f} зап/с' if limit else 'без лимита'} ({scope})"
        f", min_delay {stats.get('min_delay', 0.0):.2f} с"
        f", jitter {stats.get('jitter', 0.0):.2f} с",
        f"    Запросов: {requests}"
        f" | фактически {stats.get('actual_rps', 0.0):.2f} зап/с"
        f" | средний интервал {stats.get('avg_gap', 0.0):.2f} с",
        f"    Ожидание в лимитере: всего {stats.get('waited_total', 0.0):.2f} с"
        f" | в среднем {stats.get('avg_wait', 0.0):.2f} с"
        f" | максимум {stats.get('max_wait', 0.0):.2f} с",
    ]

    backoff: dict[str, float] = stats.get("backoff") or {}
    if stats.get("penalties") or backoff:
        penalty = ", ".join(
            f"{domain} +{delay:.2f} с" for domain, delay in sorted(backoff.items())
        )
        lines.append(
            f"    Замедление после ошибок: {stats.get('penalties', 0)} раз"
            + (f" (сейчас: {penalty})" if penalty else "")
        )

    intervals: dict[str, float] = stats.get("interval_by_domain") or {}
    for domain, interval in sorted(intervals.items(), key=lambda item: -item[1])[:3]:
        if interval:
            # Общий лимит живёт под служебным ключом, домена с таким именем нет.
            name = "все домены" if domain == GLOBAL_RATE_KEY else domain
            lines.append(f"      {name}: шаг {interval:.2f} с")

    return "\n".join(lines)


def format_robots_stats(
    stats: dict[str, Any],
    title: str = "  robots.txt",
    *,
    blocked_urls: int | None = None,
) -> str:
    """Сколько правил загружено и сколько URL из-за них не обошли."""
    if not stats.get("enabled", True):
        return f"{title}: не учитывается"

    lines = [
        f"{title}: проверок {stats.get('checks', 0)}"
        f" | запрещено {stats.get('blocked', 0)}"
        + (f" (уникальных URL {blocked_urls})" if blocked_urls is not None else "")
        + f" | файлов загружено {stats.get('fetched', 0)}"
        f" | нет файла {stats.get('missing', 0)}"
        f" | ошибок {stats.get('errors', 0)}"
    ]

    delays: dict[str, float] = stats.get("crawl_delays") or {}
    if delays:
        parts = ", ".join(f"{domain} {delay:.2f} с" for domain, delay in sorted(delays.items()))
        lines.append(f"    Crawl-delay: {parts}")

    by_domain: dict[str, int] = stats.get("blocked_by_domain") or {}
    if by_domain:
        parts = ", ".join(
            f"{domain} {count}"
            for domain, count in sorted(by_domain.items(), key=lambda item: -item[1])[:3]
        )
        lines.append(f"    Запрещено по доменам: {parts}")

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
