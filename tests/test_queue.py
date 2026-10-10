"""Тесты очереди, семафоров и фильтрации URL (День 3)."""

from __future__ import annotations

import asyncio

import pytest

from src.filters import UrlFilter, domain_key, normalize_url
from src.queue import CrawlerQueue
from src.semaphore import SemaphoreManager


async def drain(queue: CrawlerQueue) -> list[str]:
    """Забирает всё из очереди, честно отмечая каждый URL обработанным."""
    taken: list[str] = []
    while (url := await queue.get_next()) is not None:
        taken.append(url)
        queue.mark_processed(url)
    return taken


# --- CrawlerQueue -----------------------------------------------------------


async def test_queue_respects_priority():
    queue = CrawlerQueue()
    queue.add_url("http://a/low", priority=0)
    queue.add_url("http://a/high", priority=10)
    queue.add_url("http://a/medium", priority=5)

    assert await drain(queue) == ["http://a/high", "http://a/medium", "http://a/low"]


async def test_queue_is_fifo_within_same_priority():
    queue = CrawlerQueue()
    urls = [f"http://a/{i}" for i in range(5)]
    for url in urls:
        queue.add_url(url)

    assert await drain(queue) == urls


async def test_queue_rejects_duplicates():
    queue = CrawlerQueue()

    assert queue.add_url("http://a/page") is True
    assert queue.add_url("http://a/page") is False
    assert queue.add_url("http://a/page", priority=100) is False

    assert len(queue) == 1
    assert queue.get_stats()["skipped_duplicates"] == 2


async def test_queue_keeps_depth_of_each_url():
    queue = CrawlerQueue()
    queue.add_url("http://a/", depth=0)
    queue.add_url("http://a/deep", depth=3)

    assert queue.depth_of("http://a/") == 0
    assert queue.depth_of("http://a/deep") == 3
    assert queue.depth_of("http://a/unknown") == 0
    assert queue.get_stats()["max_depth_queued"] == 3


async def test_max_depth_reached_counts_only_processed():
    """«Достигнутая глубина» — про обработанные URL, а не про стоящие в очереди."""
    queue = CrawlerQueue()
    queue.add_url("http://a/", depth=0)
    queue.add_url("http://a/deep", depth=5)

    queue.mark_processed(await queue.get_next())

    stats = queue.get_stats()
    assert stats["max_depth_reached"] == 0
    assert stats["max_depth_queued"] == 5


async def test_add_alias_blocks_url_without_queueing_it():
    queue = CrawlerQueue()

    assert queue.add_alias("http://a/target") is True
    assert queue.add_alias("http://a/target") is False
    assert queue.add_url("http://a/target") is False

    assert len(queue) == 0
    assert "http://a/target" in queue.seen
    assert queue.get_stats()["pending"] == 0


async def test_get_next_waits_while_work_is_in_flight():
    """Пустая очередь при непустом in_progress — не конец обхода, а ожидание."""
    queue = CrawlerQueue()
    queue.add_url("http://a/first")
    first = await queue.get_next()

    waiter = asyncio.create_task(queue.get_next())
    await asyncio.sleep(0.05)
    assert not waiter.done()  # ждёт, а не возвращает None

    queue.add_url("http://a/second")
    assert await asyncio.wait_for(waiter, timeout=1) == "http://a/second"
    assert first == "http://a/first"


async def test_all_waiting_workers_get_none_when_drained():
    """Регрессия: последний mark_processed должен разбудить всех ожидающих."""
    queue = CrawlerQueue()
    queue.add_url("http://a/only")
    url = await queue.get_next()

    waiters = [asyncio.create_task(queue.get_next()) for _ in range(3)]
    await asyncio.sleep(0.05)
    assert not any(waiter.done() for waiter in waiters)

    queue.mark_processed(url)
    results = await asyncio.wait_for(asyncio.gather(*waiters), timeout=1)

    assert results == [None, None, None]


async def test_close_stops_waiting_workers():
    queue = CrawlerQueue()
    queue.add_url("http://a/one")
    await queue.get_next()
    waiters = [asyncio.create_task(queue.get_next()) for _ in range(2)]
    await asyncio.sleep(0.05)

    queue.close()

    assert await asyncio.wait_for(asyncio.gather(*waiters), timeout=1) == [None, None]
    assert queue.closed
    # Закрытая очередь больше не принимает URL и не выдаёт их.
    assert queue.add_url("http://a/late") is False
    assert await queue.get_next() is None


async def test_queue_stats_cover_progress():
    queue = CrawlerQueue()
    for i in range(3):
        queue.add_url(f"http://a/{i}")

    first = await queue.get_next()
    queue.mark_processed(first)
    second = await queue.get_next()
    queue.mark_failed(second, "HTTP 500")
    third = await queue.get_next()

    stats = queue.get_stats()
    assert stats["processed"] == 1
    assert stats["failed"] == 1
    assert stats["in_progress"] == 1
    assert stats["pending"] == 0
    assert stats["added"] == stats["seen"] == 3
    assert stats["pages_per_sec"] > 0
    assert queue.failed[second] == "HTTP 500"
    assert third not in queue.processed


async def test_mark_skipped_releases_url_without_counting_it():
    queue = CrawlerQueue()
    queue.add_url("http://a/over-limit")
    url = await queue.get_next()

    queue.mark_skipped(url)

    stats = queue.get_stats()
    assert stats["in_progress"] == 0
    assert stats["processed"] == stats["failed"] == 0
    assert stats["skipped_unfetched"] == 1


# --- SemaphoreManager -------------------------------------------------------


async def run_through(
    manager: SemaphoreManager, urls: list[str], hold: float = 0.05
) -> None:
    async def one(url: str) -> None:
        async with manager.acquire(url):
            await asyncio.sleep(hold)

    await asyncio.gather(*(one(url) for url in urls))


async def test_semaphore_limits_global_concurrency():
    manager = SemaphoreManager(max_concurrent=2)

    await run_through(manager, [f"http://a/{i}" for i in range(6)])

    assert manager.peak_total == 2
    assert manager.active_total == 0
    assert manager.get_stats()["acquired_total"] == 6


async def test_semaphore_limits_per_domain_without_blocking_others():
    """Лимит на домен не должен мешать запросам к другим доменам."""
    manager = SemaphoreManager(max_concurrent=10, max_per_domain=1)
    urls = [f"http://a/{i}" for i in range(3)] + [f"http://b/{i}" for i in range(3)]

    await run_through(manager, urls)

    stats = manager.get_stats()
    assert stats["peak_by_host"] == {"a": 1, "b": 1}
    assert manager.peak_total == 2  # по одному на каждый из двух доменов


async def test_semaphore_treats_www_and_port_as_one_domain():
    """Иначе один сайт получил бы несколько семафоров и кратный лимит."""
    manager = SemaphoreManager(max_concurrent=10, max_per_domain=1)

    await run_through(
        manager,
        [
            "https://example.com/a",
            "https://www.example.com/b",
            "https://example.com:443/c",
            "https://EXAMPLE.com/d",
        ],
    )

    assert manager.get_stats()["peak_by_host"] == {"example.com": 1}
    assert manager.peak_total == 1


async def test_semaphore_reset_stats_keeps_active_slots():
    manager = SemaphoreManager(max_concurrent=2)
    await run_through(manager, ["http://a/1", "http://a/2"])
    assert manager.get_stats()["peak_active"] == 2

    entered = asyncio.Event()

    async def hold() -> None:
        async with manager.acquire("http://a/busy"):
            entered.set()
            await asyncio.sleep(0.1)

    task = asyncio.create_task(hold())
    await entered.wait()
    manager.reset_stats()
    stats = manager.get_stats()
    await task

    assert stats["acquired_total"] == 0
    assert stats["active"] == 1
    assert stats["peak_active"] == 1  # пик не ниже того, что занято сейчас


async def test_semaphore_forgets_idle_domains():
    """Семафор на каждый встреченный домен не должен жить до конца процесса."""
    manager = SemaphoreManager(max_concurrent=4)

    await run_through(manager, [f"http://host{i}/page" for i in range(5)], hold=0)

    stats = manager.get_stats()
    assert stats["hosts_active"] == 0
    assert stats["hosts"] == 5  # но сколько доменов видели — помним


async def test_semaphore_default_per_domain_equals_global():
    manager = SemaphoreManager(max_concurrent=4)

    await run_through(manager, [f"http://a/{i}" for i in range(4)])

    assert manager.max_per_domain == 4
    assert manager.peak_total == 4


async def test_semaphore_releases_slot_on_cancellation():
    manager = SemaphoreManager(max_concurrent=1)
    entered = asyncio.Event()

    async def hold() -> None:
        async with manager.acquire("http://a/slow"):
            entered.set()
            await asyncio.sleep(10)

    task = asyncio.create_task(hold())
    await entered.wait()
    waiting = asyncio.create_task(run_through(manager, ["http://a/next"], hold=0))
    await asyncio.sleep(0.05)
    assert manager.get_stats()["waiting"] == 1

    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    await asyncio.wait_for(waiting, timeout=1)

    stats = manager.get_stats()
    assert stats["active"] == 0
    assert stats["waiting"] == 0


@pytest.mark.parametrize(
    ("max_concurrent", "max_per_domain"), [(0, None), (1, 0), (-1, None)]
)
async def test_semaphore_rejects_invalid_limits(max_concurrent, max_per_domain):
    with pytest.raises(ValueError):
        SemaphoreManager(max_concurrent, max_per_domain)


# --- UrlFilter --------------------------------------------------------------


def test_filter_allows_everything_by_default():
    url_filter = UrlFilter.build(["http://a/"])

    assert url_filter.allows("http://a/page")
    assert url_filter.allows("https://other.example.org/page")


def test_filter_same_domain_ignores_www_and_port():
    url_filter = UrlFilter.build(
        ["https://example.com/start"], same_domain_only=True
    )

    assert url_filter.allows("https://example.com/page")
    assert url_filter.allows("https://www.example.com/page")
    assert url_filter.allows("http://example.com:8080/page")
    assert not url_filter.allows("https://other.example.org/page")
    assert url_filter.reject_reason("https://other.example.org/") == "domain"


def test_filter_include_patterns_keep_only_matching():
    url_filter = UrlFilter.build(include_patterns=[r"/blog/"])

    assert url_filter.allows("https://a/blog/post")
    assert not url_filter.allows("https://a/shop/item")
    assert url_filter.reject_reason("https://a/shop/item") == "include"


def test_filter_exclude_beats_include():
    url_filter = UrlFilter.build(
        include_patterns=[r"/blog/"], exclude_patterns=[r"/blog/draft"]
    )

    assert url_filter.allows("https://a/blog/post")
    assert url_filter.reject_reason("https://a/blog/draft-1") == "exclude"


@pytest.mark.parametrize(
    "url",
    [
        "mailto:crawler@example.org",
        "javascript:void(0)",
        "tel:+70000000000",
        "data:image/png;base64,AAAA",
        "/relative/path",
        "http:///no-host",
    ],
)
def test_filter_rejects_non_http_urls(url):
    assert UrlFilter.build().reject_reason(url) == "scheme"


def test_filter_skips_broken_pattern():
    """Битый паттерн не должен ронять обход."""
    url_filter = UrlFilter.build(exclude_patterns=["(unclosed", r"/logout"])

    assert len(url_filter.exclude_patterns) == 1
    assert not url_filter.allows("https://a/logout")


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("https://WWW.Example.COM/page", "example.com"),
        ("example.com", "example.com"),
        ("127.0.0.1:8080", "127.0.0.1"),
        ("http://user:pass@example.com/", "example.com"),
        ("site.com/path", "site.com"),  # строка без схемы: путь не часть домена
    ],
)
def test_domain_key_normalizes_host(value, expected):
    assert domain_key(value) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("http://Example.COM/page", "http://example.com/page"),
        ("HTTP://example.com/page", "http://example.com/page"),
        ("http://example.com:80/page", "http://example.com/page"),
        ("https://example.com:443/page", "https://example.com/page"),
        ("http://example.com:8080/page", "http://example.com:8080/page"),
        ("https://example.com", "https://example.com/"),
        ("https://example.com/page?", "https://example.com/page"),
        ("https://example.com/page#anchor", "https://example.com/page"),
        # Путь и запрос регистрозависимы, их трогать нельзя.
        ("https://example.com/Page?Q=1", "https://example.com/Page?Q=1"),
        # Нормализовать нечего: относительные и мусорные ссылки как есть.
        ("/relative/path", "/relative/path"),
        ("mailto:crawler@example.org", "mailto:crawler@example.org"),
    ],
)
def test_normalize_url(value, expected):
    assert normalize_url(value) == expected


def test_normalize_url_is_idempotent():
    once = normalize_url("HTTP://WWW.Example.com:80/Path?a=1#frag")
    assert once == "http://www.example.com/Path?a=1"
    assert normalize_url(once) == once
