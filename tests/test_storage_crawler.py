"""Тесты сохранения данных при обходе сайта (День 6)."""

from __future__ import annotations

import json
from typing import Any

import pytest

from src.crawler import AsyncCrawler
from src.models import PageRecord
from src.storage import (
    CSVStorage,
    DataStorage,
    JSONStorage,
    MultiStorage,
    SQLiteStorage,
)


class Broken(DataStorage):
    """Хранилище, в которое ничего не записывается: проверка живучести обхода."""

    def __init__(self, **kwargs: Any) -> None:
        kwargs.setdefault("max_retries", 0)
        super().__init__(**kwargs)
        self.seen: list[str] = []

    @property
    def target(self) -> str:
        return "сломанное"

    async def _open(self) -> None:
        return None

    async def _write(self, batch: list[PageRecord]) -> None:
        self.seen.extend(record.url for record in batch)
        raise OSError("на диске нет места")

    async def load(self, limit: int | None = None) -> list[dict[str, Any]]:
        return []


@pytest.fixture
def json_storage(tmp_path) -> JSONStorage:
    return JSONStorage(tmp_path / "pages.json")


# --- сохранение при обходе --------------------------------------------------


async def test_crawl_saves_every_page(url, json_storage, tmp_path):
    async with AsyncCrawler(max_concurrent=2, storage=json_storage) as crawler:
        pages = await crawler.crawl([url("/site/")], max_pages=50, same_domain_only=True)
        stats = crawler.get_crawl_stats()["storage"]

    saved = json.loads((tmp_path / "pages.json").read_text(encoding="utf-8"))
    assert stats["enabled"] is True
    assert stats["saved"] == len(crawler.processed_urls)
    assert {item["url"] for item in saved} == set(crawler.processed_urls)
    assert len(saved) < len(pages)  # неудачная страница сайта не сохранялась


async def test_saved_page_keeps_crawl_data(url, json_storage, tmp_path):
    """Целостность: в файле то же, что в результатах обхода."""
    async with AsyncCrawler(storage=json_storage) as crawler:
        pages = await crawler.fetch_and_parse_many([url("/page")])

    page = pages[0]
    saved = json.loads((tmp_path / "pages.json").read_text(encoding="utf-8"))[0]
    assert saved["url"] == page["url"]
    assert saved["title"] == page["title"]
    assert saved["text"] == page["text"]
    assert saved["links"] == page["links"]
    assert saved["metadata"]["description"] == page["metadata"]["description"]
    assert saved["status_code"] == page["status"]
    assert saved["content_type"] == "text/html"
    assert saved["crawled_at"]


async def test_saved_page_knows_its_depth(url, json_storage):
    async with AsyncCrawler(max_concurrent=2, storage=json_storage) as crawler:
        await crawler.crawl([url("/site/")], max_pages=50, same_domain_only=True)

    rows = await json_storage.load()
    depths = {row["url"]: row["depth"] for row in rows}
    assert depths[url("/site/")] == 0
    assert depths[url("/site/a")] == 1
    assert depths[url("/site/a1")] == 2


async def test_failed_pages_are_skipped_by_default(url, json_storage):
    async with AsyncCrawler(storage=json_storage, max_retries=0) as crawler:
        await crawler.fetch_and_parse_many([url("/ok"), url("/status/404")])

    rows = await json_storage.load()
    assert [row["url"] for row in rows] == [url("/ok")]


async def test_failed_pages_can_be_saved_on_request(url, json_storage):
    async with AsyncCrawler(
        storage=json_storage, storage_save_failed=True, max_retries=0
    ) as crawler:
        await crawler.fetch_and_parse_many([url("/ok"), url("/status/404")])

    rows = await json_storage.load()
    broken = [row for row in rows if not row["ok"]]
    assert len(rows) == 2
    assert broken[0]["status_code"] == 404
    assert broken[0]["error_class"] == "PermanentError"


async def test_each_page_is_saved_once(url, json_storage):
    """Хук один, поэтому страница не может попасть в файл дважды."""
    async with AsyncCrawler(max_concurrent=2, storage=json_storage) as crawler:
        await crawler.crawl([url("/site/")], max_pages=50, same_domain_only=True)

    rows = await json_storage.load()
    assert len(rows) == len({row["url"] for row in rows})


async def test_storage_is_not_connected_by_default(url):
    async with AsyncCrawler() as crawler:
        await crawler.fetch_and_parse_many([url("/ok")])
        stats = crawler.get_crawl_stats()

    assert crawler.storage is None
    assert stats["storage"] == {"enabled": False}


# --- закрытие и буфер -------------------------------------------------------


async def test_closing_the_crawler_flushes_the_buffer(url, tmp_path):
    """Последние страницы могут лежать в буфере — close() обязан их записать."""
    storage = SQLiteStorage(tmp_path / "pages.db", batch_size=100)
    crawler = AsyncCrawler(storage=storage)
    await crawler.fetch_and_parse_many([url("/ok"), url("/page")])
    assert await storage.count() == 0

    await crawler.close()

    assert await storage.count() == 2
    assert storage.closed is True


async def test_crawler_can_leave_the_storage_open(url, tmp_path):
    """Хранилище переживает краулер, если им пользуются и после обхода."""
    storage = JSONStorage(tmp_path / "pages.jsonl", lines=True, batch_size=100)
    crawler = AsyncCrawler(storage=storage, close_storage=False)
    await crawler.fetch_and_parse_many([url("/ok")])

    await crawler.close()

    assert storage.closed is False
    assert len(await storage.load()) == 1  # буфер всё равно досылается
    await storage.close()


# --- устойчивость к сбоям записи -------------------------------------------


async def test_broken_storage_does_not_break_the_crawl(url):
    broken = Broken()

    async with AsyncCrawler(max_concurrent=2, storage=broken) as crawler:
        pages = await crawler.crawl([url("/site/")], max_pages=50, same_domain_only=True)
        stats = crawler.get_crawl_stats()["storage"]

    assert len(crawler.processed_urls) > 1  # обход прошёл целиком
    assert all(page["url"] for page in pages)
    assert stats["failed"] == len(crawler.processed_urls)
    assert stats["saved"] == 0
    assert "места" in stats["last_error"]


async def test_one_broken_format_does_not_cost_the_data(url, tmp_path):
    storage = MultiStorage([JSONStorage(tmp_path / "pages.json"), Broken()])

    async with AsyncCrawler(max_concurrent=2, storage=storage) as crawler:
        await crawler.crawl([url("/site/")], max_pages=50, same_domain_only=True)

    saved = json.loads((tmp_path / "pages.json").read_text(encoding="utf-8"))
    assert {item["url"] for item in saved} == set(crawler.processed_urls)


# --- несколько форматов в обходе -------------------------------------------


async def test_crawl_writes_three_formats_at_once(url, tmp_path):
    storage = MultiStorage(
        [
            JSONStorage(tmp_path / "pages.json"),
            CSVStorage(tmp_path / "pages.csv"),
            SQLiteStorage(tmp_path / "pages.db", batch_size=5),
        ]
    )

    async with AsyncCrawler(max_concurrent=2, storage=storage) as crawler:
        await crawler.crawl([url("/site/")], max_pages=50, same_domain_only=True)
        expected = set(crawler.processed_urls)

    from_json = await JSONStorage(tmp_path / "pages.json").load()
    from_csv = await CSVStorage(tmp_path / "pages.csv").load()
    from_db = await SQLiteStorage(tmp_path / "pages.db").load()
    assert {row["url"] for row in from_json} == expected
    assert {row["url"] for row in from_csv} == expected
    assert {row["url"] for row in from_db} == expected


async def test_second_crawl_updates_the_same_rows(url, tmp_path):
    """Повторный обход обновляет страницы в базе, а не копит дубликаты."""
    database = tmp_path / "pages.db"

    for _ in range(2):
        storage = SQLiteStorage(database, batch_size=5)
        async with AsyncCrawler(max_concurrent=2, storage=storage) as crawler:
            await crawler.crawl([url("/site/")], max_pages=50, same_domain_only=True)
            expected = len(crawler.processed_urls)

    assert await SQLiteStorage(database).count() == expected
