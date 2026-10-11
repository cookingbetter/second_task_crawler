"""Тесты базового асинхронного HTTP-клиента (День 1)."""

from __future__ import annotations

import pytest

from src.crawler import AsyncCrawler, FetchError
from src.models import ERROR_HTTP, ERROR_NETWORK, ERROR_TIMEOUT
from tests.conftest import CHUNKED_SIZE, NO_CHARSET_BODY, OK_BODY

UNREACHABLE_URL = "http://this-domain-does-not-exist-12345.invalid/"
# День 5 включает повторы по умолчанию. Тестам дня 1 важен сам факт ошибки, а
# не её повторная загрузка, поэтому они просят одну попытку.
NO_RETRIES = {"max_retries": 0}


async def test_fetch_url_returns_body(url):
    async with AsyncCrawler(max_concurrent=2) as crawler:
        text = await crawler.fetch_url(url("/ok"))

    assert text == OK_BODY


async def test_fetch_one_success_fields(url):
    async with AsyncCrawler() as crawler:
        result = await crawler.fetch_one(url("/ok"))

    assert result.ok
    assert result.status == 200
    assert result.error is None
    assert result.elapsed > 0


@pytest.mark.parametrize("code", [404, 500])
async def test_http_errors_are_captured(url, code):
    async with AsyncCrawler(**NO_RETRIES) as crawler:
        result = await crawler.fetch_one(url(f"/status/{code}"))

    assert not result.ok
    assert result.error_type == ERROR_HTTP
    assert result.status == code


async def test_fetch_url_raises_on_http_error(url):
    async with AsyncCrawler() as crawler:
        with pytest.raises(FetchError) as exc_info:
            await crawler.fetch_url(url("/status/404"))

    assert exc_info.value.result.status == 404


async def test_unreachable_host_is_network_error():
    async with AsyncCrawler(**NO_RETRIES) as crawler:
        result = await crawler.fetch_one(UNREACHABLE_URL)

    assert not result.ok
    assert result.error_type == ERROR_NETWORK


async def test_read_timeout(url):
    async with AsyncCrawler(
        timeout_total=5.0, timeout_read=0.2, **NO_RETRIES
    ) as crawler:
        result = await crawler.fetch_one(url("/slow?delay=1.0"))

    assert not result.ok
    assert result.error_type == ERROR_TIMEOUT


async def test_fetch_urls_keeps_only_successful(url):
    urls = [
        url("/ok"),
        url("/status/404"),
        url("/slow?delay=0.05"),
        UNREACHABLE_URL,
    ]

    async with AsyncCrawler(max_concurrent=4, **NO_RETRIES) as crawler:
        pages = await crawler.fetch_urls(urls)
        stats = crawler.stats
        results = crawler.results

    assert set(pages) == {url("/ok"), url("/slow?delay=0.05")}
    assert stats.total == 4
    assert stats.succeeded == 2
    assert stats.failed == 2
    assert len(results) == 4


async def test_parallel_is_faster_than_sequential(url):
    delay = 0.3
    urls = [url(f"/slow?delay={delay}&n={i}") for i in range(5)]

    async with AsyncCrawler(max_concurrent=5) as crawler:
        await crawler.fetch_urls(urls)
        parallel = crawler.stats.elapsed

    async with AsyncCrawler(max_concurrent=1) as crawler:
        await crawler.fetch_urls_sequential(urls)
        sequential = crawler.stats.elapsed

    assert sequential > parallel * 2
    assert parallel < delay * len(urls)


async def test_multi_chunk_body_is_read_fully(url):
    """Тело из нескольких TCP-чанков не должно обрезаться до первого."""
    async with AsyncCrawler() as crawler:
        result = await crawler.fetch_one(url("/chunked"))

    assert result.ok
    assert not result.truncated
    assert len(result.text or "") == CHUNKED_SIZE


async def test_body_limit_marks_truncated(url):
    async with AsyncCrawler(max_body_bytes=500) as crawler:
        result = await crawler.fetch_one(url("/chunked"))

    assert result.ok
    assert result.truncated
    assert len(result.text or "") == 500


async def test_response_without_charset_is_decoded(url):
    """Content-Type без charset — не ошибка: декодируем как utf-8."""
    async with AsyncCrawler() as crawler:
        result = await crawler.fetch_one(url("/nocharset"))

    assert result.ok
    assert result.error_type is None
    assert result.text == NO_CHARSET_BODY


async def test_duplicate_urls_fetched_once(url, hits):
    target = url("/counted")

    async with AsyncCrawler() as crawler:
        pages = await crawler.fetch_urls([target, target, target])
        stats = crawler.stats

    assert hits["/counted"] == 1
    assert len(pages) == stats.succeeded == 1
    assert stats.total == 1


async def test_elapsed_excludes_queue_wait(url):
    """elapsed — время запроса, а не время ожидания семафора."""
    delay = 0.2
    urls = [url(f"/slow?delay={delay}&n={i}") for i in range(4)]

    async with AsyncCrawler(max_concurrent=1) as crawler:
        await crawler.fetch_urls(urls)
        results = crawler.results
        batch_elapsed = crawler.stats.elapsed

    assert all(result.ok for result in results)
    assert max(result.elapsed for result in results) < batch_elapsed / 2
    assert all(result.elapsed >= delay for result in results)


async def test_close_is_idempotent(url):
    crawler = AsyncCrawler()
    await crawler.fetch_one(url("/ok"))

    await crawler.close()
    await crawler.close()

    assert crawler.closed
    with pytest.raises(RuntimeError):
        await crawler.fetch_one(url("/ok"))


async def test_semaphore_limits_concurrency(url):
    delay = 0.2
    urls = [url(f"/slow?delay={delay}&n={i}") for i in range(6)]

    async with AsyncCrawler(max_concurrent=2) as crawler:
        await crawler.fetch_urls(urls)
        limited = crawler.stats.elapsed

    assert crawler.stats.succeeded == 6
    # 6 запросов по 2 за раз — минимум три «волны» задержки.
    assert limited >= delay * 3
