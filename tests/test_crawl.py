"""Тесты обхода сайта: очередь, глубина, фильтры, статистика (День 3)."""

from __future__ import annotations

import json

import pytest

from src.crawler import AsyncCrawler
from src.filters import normalize_url
from src.main import parse_args
from src.models import ERROR_HTTP
from src.utils import (
    format_crawl_stats,
    format_progress,
    print_progress,
    save_crawl_results,
)
from tests.conftest import EXTERNAL_LINK, REDIRECT_PATH, REDIRECT_TARGET


def paths(urls) -> set[str]:
    """Оставляет от URL только путь — так ожидания в тестах читаемее."""
    return {"/" + url.split("/", 3)[3] for url in urls}


async def test_crawl_follows_links_within_depth(url, hits):
    async with AsyncCrawler(max_concurrent=4, max_depth=2) as crawler:
        pages = await crawler.crawl(
            [url("/site/")], max_pages=50, same_domain_only=True
        )

    assert paths(crawler.processed_urls) == {
        "/site/",
        "/site/a",
        "/site/b",
        "/site/a1",
        "/site/b1",
        REDIRECT_PATH,
    }
    assert len(pages) == len(crawler.visited_urls) == 7  # плюс битая /site/missing
    # Каждый URL обойдён один раз, хотя /site/a ссылается назад.
    assert len({page["url"] for page in pages}) == len(pages)
    assert all(count == 1 for path, count in hits.items() if path != REDIRECT_TARGET)
    # Цель редиректа сервер может отдать и второй раз: если прямая ссылка и сам
    # редирект грузятся одновременно, о совпадении узнать заранее нельзя.
    assert hits[REDIRECT_TARGET] <= 2


async def test_max_depth_limits_crawl(url):
    async with AsyncCrawler(max_concurrent=4) as crawler:
        await crawler.crawl(
            [url("/site/")], max_pages=50, max_depth=1, same_domain_only=True
        )

    assert paths(crawler.processed_urls) == {"/site/", "/site/a", "/site/b"}
    # Ссылки с глубины 1 дальше не пошли, но их посчитали как пропущенные.
    assert crawler.skipped["depth"] > 0
    assert crawler.get_crawl_stats()["max_depth_reached"] == 1


async def test_max_depth_zero_fetches_only_start_urls(url):
    async with AsyncCrawler(max_concurrent=2) as crawler:
        pages = await crawler.crawl(
            [url("/site/")], max_pages=50, max_depth=0, same_domain_only=True
        )

    assert len(pages) == 1
    assert paths(crawler.processed_urls) == {"/site/"}
    assert len(crawler.queue) == 0


async def test_max_pages_is_exact(url):
    async with AsyncCrawler(max_concurrent=4, max_depth=3) as crawler:
        pages = await crawler.crawl(
            [url("/site/")], max_pages=3, same_domain_only=True
        )
        stats = crawler.get_crawl_stats()

    assert len(pages) == len(crawler.visited_urls) == 3
    assert stats["pages_started"] == 3
    assert crawler.queue.closed


async def test_crawl_has_no_duplicates(url, hits):
    """Цикл /site/a -> /site/ не должен приводить к повторной загрузке."""
    async with AsyncCrawler(max_concurrent=4, max_depth=3) as crawler:
        pages = await crawler.crawl(
            [url("/site/"), url("/site/"), url("/site/a")],
            max_pages=50,
            same_domain_only=True,
        )

    fetched = [page["url"] for page in pages]
    assert len(fetched) == len(set(fetched))
    assert hits[("/site/")] == 1
    assert crawler.queue.skipped_duplicates > 0


async def test_same_domain_only_skips_external_links(url):
    async with AsyncCrawler(max_concurrent=4, max_depth=2) as crawler:
        pages = await crawler.crawl(
            [url("/site/")], max_pages=50, same_domain_only=True
        )

    assert EXTERNAL_LINK not in crawler.queue.seen
    assert crawler.skipped["filtered"] >= 1
    # Внешняя ссылка отсутствует в очереди, но в данных страницы она есть.
    start = next(page for page in pages if page["url"] == url("/site/"))
    assert EXTERNAL_LINK in start["links"]


async def test_exclude_pattern_cuts_branch(url):
    async with AsyncCrawler(max_concurrent=4, max_depth=3) as crawler:
        await crawler.crawl(
            [url("/site/")],
            max_pages=50,
            same_domain_only=True,
            exclude_patterns=[r"/site/a"],
        )

    assert paths(crawler.processed_urls) == {
        "/site/",
        "/site/b",
        "/site/b1",
        REDIRECT_PATH,
    }


async def test_include_pattern_keeps_only_matching(url):
    async with AsyncCrawler(max_concurrent=4, max_depth=3) as crawler:
        await crawler.crawl(
            [url("/site/")],
            max_pages=50,
            same_domain_only=True,
            include_patterns=[r"/site/a"],
        )

    assert paths(crawler.processed_urls) == {"/site/", "/site/a", "/site/a1"}


async def test_failed_pages_are_recorded_separately(url):
    async with AsyncCrawler(max_concurrent=4, max_depth=1) as crawler:
        pages = await crawler.crawl(
            [url("/site/")], max_pages=50, same_domain_only=True
        )

    missing = url("/site/missing")
    assert missing in crawler.failed_urls
    assert missing not in crawler.processed_urls
    assert missing in crawler.visited_urls

    failed_page = next(page for page in pages if page["url"] == missing)
    assert failed_page["ok"] is False
    assert failed_page["error_type"] == ERROR_HTTP
    assert failed_page["depth"] == 1
    assert crawler.stats.failed == 1


async def test_pages_carry_depth_and_parsed_data(url):
    async with AsyncCrawler(max_concurrent=4, max_depth=1) as crawler:
        pages = await crawler.crawl(
            [url("/site/")], max_pages=50, same_domain_only=True
        )

    by_url = {page["url"]: page for page in pages}
    assert by_url[url("/site/")]["depth"] == 0
    assert by_url[url("/site/a")]["depth"] == 1
    assert by_url[url("/site/a")]["title"] == "Страница /site/a"
    assert by_url[url("/site/a")]["links_count"] == 2


async def test_progress_callback_receives_updates(url):
    updates: list[dict] = []

    async with AsyncCrawler(max_concurrent=2, max_depth=2) as crawler:
        await crawler.crawl(
            [url("/site/")],
            max_pages=50,
            same_domain_only=True,
            on_progress=updates.append,
            progress_interval=0.01,
        )

    assert len(updates) >= 2  # хотя бы один тик и финальный вызов
    final = updates[-1]
    assert final["processed"] == len(crawler.processed_urls)
    assert final["pending"] == 0
    assert final["in_progress"] == 0
    assert "стр/с" in format_progress(final)


async def test_broken_progress_callback_does_not_stop_crawl(url):
    def boom(stats: dict) -> None:
        raise RuntimeError("callback сломан")

    async with AsyncCrawler(max_concurrent=2, max_depth=1) as crawler:
        pages = await crawler.crawl(
            [url("/site/")],
            max_pages=50,
            same_domain_only=True,
            on_progress=boom,
            progress_interval=0.01,
        )

    assert len(pages) > 1


async def test_crawl_stats_are_consistent(url):
    async with AsyncCrawler(max_concurrent=3, max_depth=2, max_per_domain=2) as crawler:
        pages = await crawler.crawl(
            [url("/site/")], max_pages=50, same_domain_only=True
        )
        stats = crawler.get_crawl_stats()

    assert stats["processed"] == len(crawler.processed_urls)
    assert stats["failed"] == len(crawler.failed_urls)
    assert stats["visited"] == len(pages)
    assert stats["seen"] == len(crawler.queue.seen)
    assert stats["semaphores"]["peak_active"] <= 3
    assert max(stats["semaphores"]["peak_by_host"].values()) <= 2
    assert "Итог обхода" in format_crawl_stats(stats)


async def test_crawl_resets_state_between_runs(url):
    async with AsyncCrawler(max_concurrent=2, max_depth=0) as crawler:
        await crawler.crawl([url("/site/")], max_pages=50, same_domain_only=True)
        first = dict(crawler.processed_urls)

        pages = await crawler.crawl([url("/site/a")], max_pages=50, same_domain_only=True)

    assert len(first) == 1
    assert len(pages) == 1
    assert paths(crawler.processed_urls) == {"/site/a"}
    assert crawler.queue.skipped_duplicates == 0


async def test_single_worker_crawl_still_completes(url):
    """Пул из одного воркера — вырожденный случай протокола завершения."""
    async with AsyncCrawler(max_concurrent=1, max_depth=2) as crawler:
        pages = await crawler.crawl(
            [url("/site/")], max_pages=50, same_domain_only=True
        )

    assert len(pages) == 7
    assert crawler.semaphores.peak_total == 1


async def test_redirect_target_is_not_queued_twice(url):
    """Цель редиректа запоминается, чтобы ссылка на неё не породила вторую загрузку."""
    async with AsyncCrawler(max_concurrent=4, max_depth=2) as crawler:
        pages = await crawler.crawl(
            [url(REDIRECT_PATH)], max_pages=50, same_domain_only=True
        )

    target = url(REDIRECT_TARGET)
    assert len(pages) == 1  # сам редирект, цель повторно не ставилась в очередь
    assert pages[0]["final_url"] == target
    assert target in crawler.queue.seen
    assert target not in crawler.processed_urls


async def test_redirect_target_already_queued_is_skipped(url, hits):
    """Ссылка на цель могла попасть в очередь раньше, чем редирект загрузился."""
    async with AsyncCrawler(max_concurrent=1, max_depth=0) as crawler:
        pages = await crawler.crawl(
            [url(REDIRECT_PATH), url(REDIRECT_TARGET)], max_pages=50
        )

    assert hits[REDIRECT_TARGET] == 1  # одна загрузка на два адреса
    assert len(pages) == 1
    assert crawler.skipped["duplicate"] == 1
    assert crawler.queue.get_stats()["skipped_unfetched"] == 1


async def test_url_variants_are_fetched_once(url, hits):
    """`/site/b1` и `/site/b1?` — один и тот же адрес после нормализации."""
    variants = [url("/site/b1"), url("/site/b1") + "?", url("/SITE/b1".lower())]

    async with AsyncCrawler(max_concurrent=3, max_depth=0) as crawler:
        pages = await crawler.crawl(variants, max_pages=50)

    assert hits[REDIRECT_TARGET] == 1
    assert len(pages) == 1
    assert pages[0]["url"] == normalize_url(variants[0])


async def test_links_after_queue_close_are_not_called_duplicates(url):
    """Лимит страниц закрывает очередь: это не дубликаты, а отдельная причина."""
    async with AsyncCrawler(max_concurrent=2, max_depth=3) as crawler:
        await crawler.crawl([url("/site/")], max_pages=2, same_domain_only=True)

    assert crawler.skipped["closed"] > 0
    assert crawler.skipped["duplicate"] == crawler.queue.skipped_duplicates


async def test_max_depth_reached_counts_crawled_pages(url):
    """При лимите страниц очередь полна ссылок глубины 1, но пройдена только 0."""
    async with AsyncCrawler(max_concurrent=1, max_depth=2) as crawler:
        await crawler.crawl([url("/site/")], max_pages=1, same_domain_only=True)
        stats = crawler.get_crawl_stats()

    assert stats["processed"] == 1
    assert stats["pending"] > 0
    assert stats["max_depth_reached"] == 0
    assert stats["max_depth_queued"] == 1


async def test_semaphore_stats_do_not_leak_between_crawls(url):
    async with AsyncCrawler(max_concurrent=4, max_depth=2) as crawler:
        await crawler.crawl([url("/site/")], max_pages=50, same_domain_only=True)
        first = crawler.get_crawl_stats()["semaphores"]

        await crawler.crawl([url("/site/b1")], max_pages=50, max_depth=0)
        second = crawler.get_crawl_stats()["semaphores"]

    assert first["acquired_total"] > 1
    assert second["acquired_total"] == 1
    assert second["peak_active"] == 1


async def test_crawl_on_closed_crawler_raises(url):
    crawler = AsyncCrawler(max_concurrent=2)
    await crawler.close()

    with pytest.raises(RuntimeError):
        await crawler.crawl([url("/site/")], max_pages=5)


def test_print_progress_without_tty_writes_plain_line(capsys):
    """В пайпе возврат каретки склеил бы все обновления в одну строку."""
    print_progress({"processed": 3, "pending": 1, "failed": 0, "pages_per_sec": 2.5})
    out = capsys.readouterr().out

    assert "\r" not in out
    assert out.endswith("\n")
    assert "Страниц: 3" in out


@pytest.mark.parametrize(
    "argv",
    [
        ["--crawl", "--max-pages", "0"],
        ["--crawl", "--max-depth", "-1"],
        ["--crawl", "--max-concurrent", "0"],
        ["--crawl", "--max-per-domain", "0"],
        ["--crawl", "--progress-interval", "-1"],
    ],
)
def test_cli_rejects_invalid_limits(argv):
    with pytest.raises(SystemExit):
        parse_args(argv)


async def test_save_crawl_results_writes_json(url, tmp_path):
    async with AsyncCrawler(max_concurrent=2, max_depth=1) as crawler:
        pages = await crawler.crawl(
            [url("/site/")], max_pages=50, same_domain_only=True
        )
        stats = crawler.get_crawl_stats()

    target = save_crawl_results(pages, stats, tmp_path / "nested" / "crawl.json")
    saved = json.loads(target.read_text(encoding="utf-8"))

    assert target.exists()
    assert saved["pages_count"] == len(pages)
    assert saved["stats"]["processed"] == len(crawler.processed_urls)
    assert saved["pages"][0]["url"] == url("/site/")
