"""Вспомогательные функции: логирование, замер времени, форматирование вывода."""

from __future__ import annotations

import json
import logging
import sys
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

from src.models import CrawlStats, FetchResult
from src.ratelimit import GLOBAL_KEY as GLOBAL_RATE_KEY

logger = logging.getLogger(__name__)

LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
DATE_FORMAT = "%H:%M:%S"
FILE_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
# Ротация лога: 5 МБ на файл и три старых копии. Больше держать незачем —
# разбор сбоя смотрит в последние записи, а не в прошлую неделю.
LOG_MAX_BYTES = 5_000_000
LOG_BACKUPS = 3

# Прогресс-бар: ширина в символах и чем рисуется.
BAR_WIDTH = 24
BAR_DONE = "#"
BAR_LEFT = "·"


def json_default(value: Any) -> str:
    """Чем заменить объект, который `json` не умеет сам.

    Дата записывается в ISO 8601: такую строку читают и Python, и SQLite, и
    сортировка по ней совпадает с сортировкой по времени. Функция одна на весь
    проект, чтобы время в дампе обхода и в хранилищах выглядело одинаково.
    """
    if isinstance(value, datetime):
        return value.isoformat(timespec="seconds")
    return str(value)


def setup_logging(
    level: str | int = "INFO",
    log_file: str | None = None,
    *,
    max_bytes: int = LOG_MAX_BYTES,
    backups: int = LOG_BACKUPS,
    console: bool = True,
) -> None:
    """Настраивает корневой логгер, переустанавливая хендлеры при повторном вызове.

    В файл пишется полная дата, в консоль — только время: в терминале день и
    так известен, а в логе недельного запуска без даты не разобраться.

    Файл ротируется по размеру (`max_bytes`, `backups` старых копий): длинный
    обход на уровне DEBUG пишет десятки мегабайт, и без ротации лог однажды
    займёт весь диск. `max_bytes=0` ротацию отключает.
    """
    handlers: list[logging.Handler] = []
    if console:
        stream = logging.StreamHandler()
        stream.setFormatter(logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT))
        handlers.append(stream)

    if log_file:
        target = Path(log_file)
        target.parent.mkdir(parents=True, exist_ok=True)
        file_handler: logging.Handler = RotatingFileHandler(
            target, maxBytes=max_bytes, backupCount=backups, encoding="utf-8"
        )
        file_handler.setFormatter(logging.Formatter(LOG_FORMAT, datefmt=FILE_DATE_FORMAT))
        handlers.append(file_handler)

    logging.basicConfig(level=level, handlers=handlers, force=True)
    if log_file:
        logger.debug(
            "Лог пишется в %s (ротация по %d байт, копий %d)",
            log_file,
            max_bytes,
            backups,
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
    """Собирает таблицу по результатам: статус, время, размер тела или ошибка.

    Колонка попыток появляется только если повторы были: без них таблица дней
    1-3 осталась бы с лишним столбцом единиц.
    """
    retried = any(result.attempts > 1 for result in results)
    attempts_header = f" {'Попыток':>8}" if retried else ""
    header = f"{'URL':<45} {'Статус':<8} {'Время':>8}{attempts_header}  Детали"
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
        attempts = f" {result.attempts:>8}" if retried else ""
        lines.append(
            f"{url:<45} {status:<8} {result.elapsed:>7.2f}s{attempts}  {details}"
        )

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


def format_bar(percent: float, width: int = BAR_WIDTH) -> str:
    """Полоска прогресса фиксированной ширины."""
    filled = int(max(0.0, min(percent, 100.0)) / 100 * width)
    return f"[{BAR_DONE * filled}{BAR_LEFT * (width - filled)}]"


def format_eta(seconds: float) -> str:
    """Остаток времени словами: секунды, минуты или часы."""
    if seconds <= 0:
        return "—"
    if seconds < 1:
        # «0 с» выглядит как «готово», хотя работа ещё идёт.
        return "<1 с"
    if seconds < 60:
        return f"{seconds:.0f} с"
    if seconds < 3600:
        return f"{seconds / 60:.0f} мин"
    return f"{seconds / 3600:.1f} ч"


def format_progress(stats: dict[str, Any]) -> str:
    """Строка прогресса обхода: что сделано, что осталось, с какой скоростью.

    Полоска и процент появляются только при известном пределе страниц: без него
    знаменателя нет, и «процент выполнения» был бы выдумкой. Про вежливость
    пишется только когда она включена: иначе строка для дней 1-3 обросла бы
    нулями, которые ничего не значат.
    """
    done = stats.get("processed", 0) + stats.get("failed", 0)
    max_pages = stats.get("max_pages")
    line = ""
    if max_pages:
        line = (
            f"{format_bar(stats.get('percent', 0.0))}"
            f" {stats.get('percent', 0.0):3.0f}%"
            f" | {done}/{max_pages} "
        )

    line += (
        f"Страниц: {stats.get('processed', 0)}"
        f" | в очереди: {stats.get('pending', 0)}"
        f" | в работе: {stats.get('in_progress', 0)}"
        f" | ошибок: {stats.get('failed', 0)}"
        f" | {stats.get('pages_per_sec', 0.0):.1f} стр/с"
        f" | осталось: {format_eta(stats.get('eta', 0.0))}"
        f" | глубина: {stats.get('max_depth_reached', 0)}"
    )

    rate: dict[str, Any] = stats.get("rate_limit") or {}
    if rate.get("enabled"):
        line += f" | ожидание: {rate.get('avg_wait', 0.0):.2f} с"
    robots: dict[str, Any] = stats.get("robots") or {}
    if robots.get("enabled"):
        line += f" | robots: {robots.get('blocked', 0)}"
    retry: dict[str, Any] = stats.get("retry") or {}
    if retry.get("retries"):
        line += f" | повторов: {retry['retries']}"
    circuit: dict[str, Any] = stats.get("circuit") or {}
    if circuit.get("open"):
        line += f" | отключено доменов: {len(circuit['open'])}"
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

    retry: dict[str, Any] = stats.get("retry") or {}
    if retry.get("retries") or retry.get("errors_total"):
        lines.append(format_retry_stats(retry))
    circuit: dict[str, Any] = stats.get("circuit") or {}
    if circuit.get("enabled"):
        lines.append(format_circuit_stats(circuit))
    storage: dict[str, Any] = stats.get("storage") or {}
    if storage.get("enabled"):
        lines.append(format_storage_stats(storage))
    sitemap: dict[str, Any] = stats.get("sitemap") or {}
    if sitemap.get("enabled"):
        lines.append(format_sitemap_stats(sitemap))

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
        # Счётчик загрузок обнуляется в начале обхода, а правила остаются в
        # кэше: без этой цифры вышло бы «загружено 0» при живом robots.txt,
        # прочитанном ещё при поиске карты сайта.
        f" (в кэше {stats.get('domains', 0)})"
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


def format_retry_stats(stats: dict[str, Any], title: str = "  Повторы") -> str:
    """Настройки повторов и то, что они дали: сколько URL спасено и какой ценой."""
    if not stats.get("enabled"):
        return f"{title}: выключены"

    lines = [
        f"{title}: до {stats.get('max_retries', 0)} на запрос"
        f", backoff x{stats.get('backoff_factor', 0.0):.2f}"
        f" от {stats.get('base_delay', 0.0):.2f} с"
        f" (потолок {stats.get('max_delay', 0.0):.2f} с)",
        f"    Запросов: {stats.get('calls', 0)}"
        f" | попыток {stats.get('attempts', 0)}"
        f" | повторов {stats.get('retries', 0)}"
        f" | спасено повтором {stats.get('retry_success', 0)}"
        f" | исчерпано {stats.get('exhausted', 0)}"
        f" | без повтора {stats.get('not_retried', 0)}",
    ]

    if stats.get("retries"):
        lines.append(
            f"    Ожидание повторов: всего {stats.get('wait_total', 0.0):.2f} с"
            f" | в среднем {stats.get('avg_wait', 0.0):.2f} с"
            f" | максимум {stats.get('max_wait', 0.0):.2f} с"
        )
    if classes := stats.get("by_class"):
        lines.append(f"    Классы ошибок: {_format_counts(classes)}")

    return "\n".join(lines)


def format_circuit_stats(
    stats: dict[str, Any], title: str = "  Circuit breaker"
) -> str:
    """Порог отключения домена и то, сколько раз он сработал."""
    if not stats.get("enabled"):
        return f"{title}: выключен"

    lines = [
        f"{title}: порог {stats.get('failure_threshold', 0)} ошибок"
        f" за {stats.get('window', 0.0):.0f} с"
        f", восстановление через {stats.get('recovery_time', 0.0):.0f} с",
        f"    Домены: {stats.get('domains', 0)}"
        f" | отключений {stats.get('trips', 0)}"
        f" | отказов без запроса {stats.get('refused', 0)}"
        f" | восстановлений {stats.get('recoveries', 0)}",
    ]
    if opened := stats.get("open"):
        lines.append(f"    Отключены сейчас: {', '.join(opened)}")
    if probing := stats.get("half_open"):
        lines.append(f"    Проверяются пробным запросом: {', '.join(probing)}")

    return "\n".join(lines)


def format_storage_stats(
    stats: dict[str, Any], title: str = "  Сохранение"
) -> str:
    """Куда и сколько записано, во что обошлась запись и что не записалось."""
    if not stats.get("enabled", True):
        return f"{title}: не настроено"

    lines = [
        f"{title}: {stats.get('storage', '?')} -> {stats.get('target', '?')}"
        f" (батч {stats.get('batch_size', 1)})",
        f"    Записей: {stats.get('saved', 0)}"
        f" | батчей {stats.get('batches', 0)}"
        f" | в буфере {stats.get('buffered', 0)}"
        f" | ошибок {stats.get('failed', 0)}"
        f" | время записи {stats.get('write_time', 0.0):.2f} с",
    ]
    if size := stats.get("bytes"):
        lines.append(f"    Размер файла: {_format_size(size)}")
    if stats.get("retries"):
        lines.append(
            f"    Повторы записи: {stats['retries']}"
            f" | помогли {stats.get('retry_success', 0)}"
        )
    if error := stats.get("last_error"):
        lines.append(f"    Последняя ошибка: {error}")

    # Вложенные хранилища MultiStorage: у каждого формата свои цифры.
    for child in stats.get("storages") or []:
        lines.append(
            f"      {child.get('storage')} -> {child.get('target')}:"
            f" записей {child.get('saved', 0)}"
            f", ошибок {child.get('failed', 0)}"
            + (f", в буфере {child['buffered']}" if child.get("buffered") else "")
            + (f", {_format_size(child['bytes'])}" if child.get("bytes") else "")
        )

    return "\n".join(lines)


def format_sitemap_stats(
    stats: dict[str, Any], title: str = "  Карта сайта"
) -> str:
    """Сколько карт прочитано и сколько адресов они дали."""
    if not stats.get("enabled", True):
        return f"{title}: не использовалась"

    line = (
        f"{title}: файлов {stats.get('files', 0)}"
        f" (индексов {stats.get('indexes', 0)})"
        f" | адресов {stats.get('urls', 0)}"
        f" | дублей {stats.get('duplicates', 0)}"
        f" | ошибок {stats.get('errors', 0)}"
    )
    if stats.get("truncated"):
        line += (
            f"\n    Разбор оборван лимитами: глубина {stats.get('max_depth')}"
            f", адресов {stats.get('max_urls')}"
        )
    return line


def format_saved_sample(
    rows: list[dict[str, Any]], title: str = "Прочитано из хранилища"
) -> str:
    """Таблица нескольких сохранённых записей — проверка, что их можно читать."""
    header = f"{'URL':<45} {'Статус':>7} {'Ссылок':>7} {'Текст':>8}  Заголовок"
    lines = [f"{title} ({len(rows)}):", header, "-" * len(header)]

    for row in rows:
        url = _shorten(str(row.get("url", "")), 45)
        links = row.get("links")
        text = row.get("text") or ""
        lines.append(
            f"{url:<45} {str(row.get('status_code') or '—'):>7}"
            f" {len(links) if isinstance(links, list) else 0:>7}"
            f" {len(text):>8}  {_shorten(str(row.get('title') or ''), 40)}"
        )

    return "\n".join(lines)


def _format_size(size: int) -> str:
    """Размер файла в человеческих единицах."""
    if size < 1024:
        return f"{size} Б"
    if size < 1024 * 1024:
        return f"{size / 1024:.1f} КБ"
    return f"{size / (1024 * 1024):.1f} МБ"


def format_error_report(
    report: dict[str, Any],
    title: str = "Ошибки",
    *,
    max_urls: int = 5,
) -> str:
    """Сводка по ошибкам: типы, классы, польза повторов и постоянные URL."""
    failed = report.get("failed", 0)
    lines = [
        f"{title}: результатов {report.get('total', 0)}"
        f", неудачных {failed}"
        f", постоянных {report.get('permanent_count', 0)}"
    ]
    if not failed and not report.get("retried_urls"):
        return lines[0] + " — ошибок не было"

    if by_type := report.get("by_type"):
        lines.append(f"  По типам: {_format_counts(by_type)}")
    if by_class := report.get("by_class"):
        lines.append(f"  По классам: {_format_counts(by_class)}")
    if report.get("retried_urls"):
        lines.append(
            f"  Повторы: URL с повторами {report['retried_urls']}"
            f" | спасено {report.get('retry_success', 0)}"
            f" | ожидание {report.get('retry_waited', 0.0):.2f} с"
            f" (в среднем {report.get('avg_retry_wait', 0.0):.2f} с на повтор)"
        )

    permanent: list[dict[str, Any]] = report.get("permanent_urls") or []
    if permanent:
        lines.append(f"  Постоянные ошибки ({report.get('permanent_count', 0)}):")
        for item in permanent[:max_urls]:
            lines.append(f"    {item.get('url')} -> {item.get('error')}")
        if len(permanent) > max_urls:
            lines.append(f"    ... и ещё {len(permanent) - max_urls}")

    return "\n".join(lines)


def _format_counts(counts: dict[str, Any]) -> str:
    """Счётчики по убыванию: самое частое — первым."""
    return ", ".join(
        f"{name}={count}"
        for name, count in sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    )


def save_error_report(report: dict[str, Any], path: str | Path) -> Path:
    """Сохраняет отчёт об ошибках в JSON: его удобно приложить к разбору сбоя."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        **report,
    }
    with target.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2, default=json_default)

    logger.info(
        "Отчёт об ошибках сохранён: %s (неудачных %s)", target, report.get("failed")
    )
    return target


def save_crawl_results(
    pages: list[dict[str, Any]],
    stats: dict[str, Any],
    path: str | Path,
) -> Path:
    """Сохраняет страницы и статистику обхода в один JSON-файл.

    Намеренно простой синхронный `json.dump`: полноценный слой сохранения
    (потоковая запись, форматы, база данных) — тема отдельного дня.
    """
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "stats": stats,
        "pages_count": len(pages),
        "pages": pages,
    }
    with target.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2, default=json_default)

    logger.info("Результаты обхода сохранены: %s (%d страниц)", target, len(pages))
    return target


def _shorten(text: str, limit: int) -> str:
    return text if len(text) <= limit else text[: limit - 3] + "..."
