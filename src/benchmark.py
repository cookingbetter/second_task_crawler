"""Замер производительности: асинхронный обход против синхронного (День 7).

Мерить на настоящем сайте нельзя: результат зависел бы от чужой сети и был бы
разным при каждом запуске, а тысяча запросов к постороннему серверу — не то,
что краулер должен делать ради графика. Поэтому бенчмарк поднимает локальный
сайт из нужного числа страниц и обходит его двумя способами: пулом воркеров на
aiohttp и последовательно на `urllib` из стандартной библиотеки.

Разбор HTML в обоих случаях один и тот же — публичные методы `HTMLParser`, —
так что сравниваются именно схемы работы, а не парсеры.
"""

from __future__ import annotations

import asyncio
import logging
import time
import tracemalloc
from collections import deque
from collections.abc import AsyncIterator, Iterator, Sequence
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass, field
from typing import Any
from urllib.error import URLError
from urllib.parse import urljoin
from urllib.request import Request, urlopen

from aiohttp import web

from src.crawler import DEFAULT_USER_AGENT, AsyncCrawler
from src.parser import HTMLParser

logger = logging.getLogger(__name__)

# Размеры из задания: на них видно и выигрыш от конкурентности, и то, как он
# перестаёт расти, когда узким местом становится разбор HTML.
DEFAULT_SIZES = (100, 500, 1000)
DEFAULT_LINKS = 10
DEFAULT_CONCURRENT = 10
# Задержка ответа локального сервера. У сайта в сети она есть всегда — десятки
# миллисекунд на запрос, — и именно её прячет конкурентность. Локальный сервер
# отвечает мгновенно, поэтому без этой задержки бенчмарк мерил бы только
# накладные расходы asyncio и показывал бы, что асинхронность «не нужна».
DEFAULT_DELAY = 0.01
# Глубина синтетического сайта не ограничена смыслом, поэтому предел ставится
# заведомо большой: иначе обход упёрся бы в глубину, а не в число страниц.
SITE_DEPTH = 1000
SYNC_TIMEOUT = 10.0

_PARAGRAPH = (
    "Асинхронный краулер загружает страницы параллельно, ограничивая число "
    "одновременных запросов семафором и соблюдая правила вежливости. "
)


@dataclass(slots=True)
class Measurement:
    """Результат одного прогона: время, скорость, память, ошибки."""

    mode: str
    pages: int
    elapsed: float
    peak_memory: int
    failed: int = 0

    @property
    def pages_per_sec(self) -> float:
        return self.pages / self.elapsed if self.elapsed > 0 else 0.0

    def as_dict(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "pages": self.pages,
            "elapsed": round(self.elapsed, 3),
            "pages_per_sec": round(self.pages_per_sec, 1),
            "peak_memory": self.peak_memory,
            "failed": self.failed,
        }


@dataclass(slots=True)
class BenchmarkResult:
    """Пара прогонов на одном размере сайта."""

    size: int
    async_run: Measurement
    sync_run: Measurement
    concurrent: int = DEFAULT_CONCURRENT

    @property
    def speedup(self) -> float:
        """Во сколько раз асинхронный обход быстрее последовательного."""
        if self.async_run.elapsed <= 0:
            return 0.0
        return self.sync_run.elapsed / self.async_run.elapsed

    def as_dict(self) -> dict[str, Any]:
        return {
            "size": self.size,
            "concurrent": self.concurrent,
            "speedup": round(self.speedup, 1),
            "async": self.async_run.as_dict(),
            "sync": self.sync_run.as_dict(),
        }


@dataclass(slots=True)
class _Memory:
    peak: int = 0


@contextmanager
def _traced() -> Iterator[_Memory]:
    """Замеряет пик памяти по данным `tracemalloc`.

    Считаются только аллокации Python: буферы сокетов и прочее вне интерпретатора
    в эту цифру не попадают, поэтому она годится для сравнения двух прогонов, а
    не как «сколько памяти заняло приложение».

    Если трассировку включил кто-то снаружи (отладка, тесты), она не
    выключается — сбрасывается только пик.
    """
    stats = _Memory()
    started_here = not tracemalloc.is_tracing()
    if started_here:
        tracemalloc.start()
    else:
        tracemalloc.reset_peak()
    try:
        yield stats
    finally:
        _current, peak = tracemalloc.get_traced_memory()
        stats.peak = peak
        if started_here:
            tracemalloc.stop()


def _page_body(number: int, total: int, links: int) -> str:
    """Страница синтетического сайта: заголовок, текст и ссылки на соседей."""
    targets = [(number * links + offset + 1) % total for offset in range(links)]
    anchors = "".join(
        f'<li><a href="/page/{target}">страница {target}</a></li>' for target in targets
    )
    return (
        "<!DOCTYPE html><html lang=\"ru\"><head>"
        f"<title>Страница {number}</title>"
        f'<meta name="description" content="Синтетическая страница {number}">'
        "</head><body>"
        f"<h1>Страница {number} из {total}</h1>"
        f"<p>{_PARAGRAPH * 3}</p>"
        f"<ul>{anchors}</ul>"
        "</body></html>"
    )


@asynccontextmanager
async def synthetic_site(
    pages: int,
    *,
    links: int = DEFAULT_LINKS,
    delay: float = DEFAULT_DELAY,
) -> AsyncIterator[str]:
    """Поднимает локальный сайт из `pages` страниц и отдаёт его адрес.

    Сервер живёт в том же процессе: так замер не зависит ни от сети, ни от
    чужого сайта, и его можно повторить сколько угодно раз. `delay` изображает
    задержку настоящего сайта — без неё сравнивать способы обхода бессмысленно.
    """
    if pages < 1:
        raise ValueError("pages должен быть >= 1")

    async def handle(request: web.Request) -> web.Response:
        number = int(request.match_info.get("number", 0)) % pages
        if delay:
            await asyncio.sleep(delay)
        return web.Response(
            text=_page_body(number, pages, links), content_type="text/html"
        )

    app = web.Application()
    app.router.add_get("/", handle)
    app.router.add_get("/page/{number:\\d+}", handle)

    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = runner.addresses[0][1]
    logger.info("Синтетический сайт поднят: %d страниц на порту %d", pages, port)
    try:
        yield f"http://127.0.0.1:{port}/"
    finally:
        await runner.cleanup()


async def crawl_async(
    base_url: str, max_pages: int, *, max_concurrent: int = DEFAULT_CONCURRENT
) -> Measurement:
    """Асинхронный обход: пул воркеров, без повторов и лимитов скорости.

    Повторы и вежливость выключены намеренно: они добавляют паузы, и сравнение
    измеряло бы их, а не схему работы.
    """
    with _traced() as memory:
        started = time.perf_counter()
        async with AsyncCrawler(
            max_concurrent=max_concurrent,
            max_depth=SITE_DEPTH,
            max_retries=0,
            parser=HTMLParser(),
        ) as crawler:
            pages = await crawler.crawl([base_url], max_pages=max_pages)
        elapsed = time.perf_counter() - started

    failed = sum(1 for page in pages if not page.get("ok"))
    return Measurement(
        mode="async",
        pages=len(pages),
        elapsed=elapsed,
        peak_memory=memory.peak,
        failed=failed,
    )


def sync_crawl(
    base_url: str,
    max_pages: int,
    *,
    parser: HTMLParser | None = None,
    timeout: float = SYNC_TIMEOUT,
) -> dict[str, Any]:
    """Последовательный обход на `urllib` — эталон для сравнения.

    Делает ровно то же, что асинхронный: берёт URL из очереди, загружает,
    разбирает, ставит найденные ссылки обратно. Разница одна: следующий запрос
    начинается только после того, как закончился предыдущий.
    """
    html_parser = parser or HTMLParser()
    queue: deque[str] = deque([base_url])
    seen: set[str] = {base_url}
    pages: list[dict[str, Any]] = []
    failed = 0

    while queue and len(pages) < max_pages:
        url = queue.popleft()
        request = Request(url, headers={"User-Agent": DEFAULT_USER_AGENT})
        try:
            with urlopen(request, timeout=timeout) as response:  # noqa: S310 - локальный адрес
                body = response.read().decode("utf-8", errors="replace")
        except (URLError, OSError) as exc:
            failed += 1
            logger.debug("Синхронная загрузка не удалась: %s — %s", url, exc)
            continue

        soup = html_parser.make_soup(body)
        links = html_parser.extract_links(soup, url)
        pages.append(
            {
                "url": url,
                "title": html_parser.extract_metadata(soup).get("title", ""),
                "text_length": len(html_parser.extract_text(soup)),
                "links_count": len(links),
                "ok": True,
            }
        )
        for link in links:
            if link not in seen:
                seen.add(link)
                queue.append(link)

    return {"pages": pages, "failed": failed}


def crawl_sync(base_url: str, max_pages: int) -> Measurement:
    """Тот же последовательный обход, но с замером времени и памяти."""
    with _traced() as memory:
        started = time.perf_counter()
        result = sync_crawl(base_url, max_pages)
        elapsed = time.perf_counter() - started

    return Measurement(
        mode="sync",
        pages=len(result["pages"]),
        elapsed=elapsed,
        peak_memory=memory.peak,
        failed=result["failed"],
    )


async def run_benchmark(
    sizes: Sequence[int] = DEFAULT_SIZES,
    *,
    max_concurrent: int = DEFAULT_CONCURRENT,
    links: int = DEFAULT_LINKS,
    delay: float = DEFAULT_DELAY,
) -> list[BenchmarkResult]:
    """Прогоняет оба обхода на сайтах заданных размеров.

    Для каждого размера поднимается свой сайт, и порядок всегда один: сначала
    последовательный обход, потом асинхронный. Так разогрев (DNS, импорты,
    кэши) достаётся медленному способу, и выигрыш не оказывается завышенным.
    """
    results: list[BenchmarkResult] = []

    for size in sizes:
        async with synthetic_site(size, links=links, delay=delay) as base_url:
            logger.info("Бенчмарк: %d страниц, конкурентность %d", size, max_concurrent)
            sync_run = await asyncio.to_thread(crawl_sync, base_url, size)
            async_run = await crawl_async(
                base_url, size, max_concurrent=max_concurrent
            )

        results.append(
            BenchmarkResult(
                size=size,
                async_run=async_run,
                sync_run=sync_run,
                concurrent=max_concurrent,
            )
        )
        logger.info(
            "Бенчмарк %d страниц: синхронно %.2f с, асинхронно %.2f с (x%.1f)",
            size,
            sync_run.elapsed,
            async_run.elapsed,
            results[-1].speedup,
        )

    return results


def format_benchmark_table(results: list[BenchmarkResult]) -> str:
    """Таблица замеров: время, скорость, память и ускорение по каждому размеру."""
    header = (
        f"{'Страниц':>8} {'Способ':<10} {'Время, с':>10} {'Стр/с':>8}"
        f" {'Память':>10} {'Ошибок':>7} {'Ускорение':>10}"
    )
    lines = [header, "-" * len(header)]

    for result in results:
        for run in (result.sync_run, result.async_run):
            speedup = (
                f"x{result.speedup:.1f}" if run is result.async_run else ""
            )
            name = (
                "синхронно"
                if run.mode == "sync"
                else f"async x{result.concurrent}"
            )
            lines.append(
                f"{run.pages:>8} {name:<10} {run.elapsed:>10.2f} {run.pages_per_sec:>8.1f}"
                f" {_memory(run.peak_memory):>10} {run.failed:>7} {speedup:>10}"
            )

    return "\n".join(lines)


def _memory(size: int) -> str:
    if size < 1024 * 1024:
        return f"{size / 1024:.0f} КБ"
    return f"{size / (1024 * 1024):.1f} МБ"
