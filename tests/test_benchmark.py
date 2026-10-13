"""Тесты замера производительности (День 7).

Размеры здесь маленькие, а задержка ответа нулевая: проверяется методика, а не
скорость. Настоящие цифры даёт `python -m src.main --benchmark`.
"""

from __future__ import annotations

import asyncio
import json
import tracemalloc
from collections.abc import Callable
from typing import Any
from urllib.request import urlopen

import pytest

from src.benchmark import (
    BenchmarkResult,
    Measurement,
    _traced,
    crawl_async,
    crawl_sync,
    format_benchmark_table,
    run_benchmark,
    sync_crawl,
    synthetic_site,
)

SMALL = 8


# --- синтетический сайт -----------------------------------------------------


async def test_site_serves_requested_pages():
    async with synthetic_site(SMALL, delay=0) as base_url:
        body = await _load(base_url)
        page = await _load(f"{base_url}page/3")

    assert "Страница 0 из 8" in body
    assert "Страница 3 из 8" in page


async def test_site_pages_link_to_each_other():
    async with synthetic_site(SMALL, links=3, delay=0) as base_url:
        body = await _load(base_url)

    assert body.count('<a href="/page/') == 3


async def test_site_wraps_page_numbers():
    """Номер за пределами сайта берётся по кругу: битых ссылок нет."""
    async with synthetic_site(2, delay=0) as base_url:
        body = await _load(f"{base_url}page/99")

    assert "из 2" in body


async def test_site_needs_at_least_one_page():
    with pytest.raises(ValueError, match=">= 1"):
        async with synthetic_site(0):
            pass


# --- синхронный обход -------------------------------------------------------


async def test_sync_crawl_respects_limit():
    async with synthetic_site(SMALL, delay=0) as base_url:
        result = await _in_thread(sync_crawl, base_url, 5)

    assert len(result["pages"]) == 5
    assert result["failed"] == 0


async def test_sync_crawl_visits_each_page_once():
    async with synthetic_site(SMALL, delay=0) as base_url:
        result = await _in_thread(sync_crawl, base_url, SMALL)

    urls = [page["url"] for page in result["pages"]]
    assert len(urls) == len(set(urls))


async def test_sync_crawl_parses_pages():
    async with synthetic_site(SMALL, delay=0) as base_url:
        result = await _in_thread(sync_crawl, base_url, 1)

    page = result["pages"][0]
    assert page["title"].startswith("Страница")
    assert page["text_length"] > 0
    assert page["links_count"] > 0


async def test_sync_crawl_survives_dead_server():
    """Сервер уже выключен: обход считает ошибку и заканчивается."""
    async with synthetic_site(SMALL, delay=0) as base_url:
        pass

    result = await _in_thread(sync_crawl, base_url, 3)

    assert result["pages"] == []
    assert result["failed"] == 1


# --- сравнимость двух способов ----------------------------------------------


async def test_both_modes_see_the_same_site():
    """Если способы обходят разное число страниц, сравнивать их бессмысленно."""
    async with synthetic_site(SMALL, delay=0) as base_url:
        sync_run = await _in_thread(crawl_sync, base_url, SMALL)
        async_run = await crawl_async(base_url, SMALL, max_concurrent=4)

    assert sync_run.pages == async_run.pages == SMALL
    assert sync_run.failed == async_run.failed == 0
    assert sync_run.mode == "sync" and async_run.mode == "async"


async def test_measurements_record_time_and_memory():
    async with synthetic_site(SMALL, delay=0) as base_url:
        measurement = await crawl_async(base_url, 4, max_concurrent=2)

    assert measurement.elapsed > 0
    assert measurement.peak_memory > 0
    assert measurement.pages_per_sec > 0


# --- таблица и сводка -------------------------------------------------------


async def test_run_benchmark_covers_every_size():
    results = await run_benchmark([4, SMALL], max_concurrent=4, delay=0)

    assert [result.size for result in results] == [4, SMALL]
    assert all(result.concurrent == 4 for result in results)
    assert all(result.async_run.pages == result.size for result in results)


def test_speedup_is_time_ratio():
    result = BenchmarkResult(
        size=10,
        async_run=Measurement(mode="async", pages=10, elapsed=0.5, peak_memory=1),
        sync_run=Measurement(mode="sync", pages=10, elapsed=2.0, peak_memory=1),
    )

    assert result.speedup == pytest.approx(4.0)


def test_speedup_without_time_is_zero():
    """Нулевое время бывает на вырожденных прогонах — делить на него нельзя."""
    result = BenchmarkResult(
        size=1,
        async_run=Measurement(mode="async", pages=1, elapsed=0.0, peak_memory=1),
        sync_run=Measurement(mode="sync", pages=1, elapsed=1.0, peak_memory=1),
    )

    assert result.speedup == 0.0
    assert result.async_run.pages_per_sec == 0.0


def test_result_serializes_to_json():
    result = BenchmarkResult(
        size=10,
        async_run=Measurement(mode="async", pages=10, elapsed=0.5, peak_memory=2048),
        sync_run=Measurement(mode="sync", pages=10, elapsed=2.0, peak_memory=1024),
    )

    data = json.loads(json.dumps(result.as_dict()))
    assert data["speedup"] == 4.0
    assert data["async"]["pages"] == 10
    assert data["sync"]["peak_memory"] == 1024


def test_table_has_a_row_per_run():
    results = [
        BenchmarkResult(
            size=size,
            async_run=Measurement(mode="async", pages=size, elapsed=1.0, peak_memory=2048),
            sync_run=Measurement(mode="sync", pages=size, elapsed=3.0, peak_memory=1024),
            concurrent=5,
        )
        for size in (10, 20)
    ]

    lines = format_benchmark_table(results).splitlines()

    assert len(lines) == 2 + 2 * len(results)  # заголовок, черта и по две строки
    assert lines[2].count("синхронно") == 1
    assert "async x5" in lines[3]


def test_speedup_shown_only_for_async_row():
    results = [
        BenchmarkResult(
            size=10,
            async_run=Measurement(mode="async", pages=10, elapsed=1.0, peak_memory=1),
            sync_run=Measurement(mode="sync", pages=10, elapsed=2.0, peak_memory=1),
        )
    ]

    sync_line, async_line = format_benchmark_table(results).splitlines()[2:]

    assert "x2.0" in async_line
    assert "x2.0" not in sync_line


def test_memory_shown_in_readable_units():
    results = [
        BenchmarkResult(
            size=1,
            async_run=Measurement(mode="async", pages=1, elapsed=1.0, peak_memory=5 * 1024**2),
            sync_run=Measurement(mode="sync", pages=1, elapsed=1.0, peak_memory=200 * 1024),
        )
    ]

    table = format_benchmark_table(results)

    assert "200 КБ" in table
    assert "5.0 МБ" in table


# --- замер памяти -----------------------------------------------------------


def test_traced_measures_peak():
    with _traced() as memory:
        ballast = [bytes(100_000) for _ in range(10)]
        assert len(ballast) == 10

    assert memory.peak > 500_000


def test_traced_keeps_foreign_tracing_on():
    """Трассировку включили снаружи — бенчмарк не должен её выключать."""
    tracemalloc.start()
    try:
        with _traced() as memory:
            ballast = bytes(200_000)
            assert ballast

        assert tracemalloc.is_tracing() is True
        assert memory.peak > 0
    finally:
        tracemalloc.stop()


def test_traced_stops_its_own_tracing():
    assert tracemalloc.is_tracing() is False

    with _traced():
        assert tracemalloc.is_tracing() is True

    assert tracemalloc.is_tracing() is False


# --- вспомогательное --------------------------------------------------------


async def _load(url: str) -> str:
    """Читает страницу синтетического сайта, не занимая event loop."""
    return await _in_thread(_get, url)


def _get(url: str) -> str:
    with urlopen(url, timeout=5) as response:  # noqa: S310 - локальный адрес
        return response.read().decode("utf-8")


async def _in_thread(func: Callable[..., Any], *args: Any) -> Any:
    """Синхронный вызов в потоке: сервер живёт в этом же event loop."""
    return await asyncio.to_thread(func, *args)
