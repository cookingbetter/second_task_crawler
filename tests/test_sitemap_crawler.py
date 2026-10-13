"""Тесты карты сайта в связке с краулером и сервером (День 7)."""

from __future__ import annotations

from aiohttp.test_utils import TestServer

from src.crawler import AsyncCrawler
from src.parser import HTMLParser
from src.robots import parse_robots
from src.sitemap import SITEMAP_AUTO
from tests.conftest import (
    SITEMAP_BOMB_PATH,
    SITEMAP_BOMB_URLS,
    SITEMAP_BROKEN_PATH,
    SITEMAP_EMPTY_PATH,
    SITEMAP_GZ_PATH,
    SITEMAP_INDEX_PATH,
    SITEMAP_LOOP_PATH,
    SITEMAP_ORDER,
    SITEMAP_PARTS,
    SITEMAP_PATH,
)


# --- загрузка карты через краулер -------------------------------------------


async def test_fetch_sitemap_through_crawler(url):
    async with AsyncCrawler(parser=HTMLParser()) as crawler:
        urls = await crawler.sitemap.fetch_sitemap(url(SITEMAP_PATH))

    assert url("/site/a") in urls
    assert url("/site/b1") in urls  # относительный <loc> достроился
    assert all("logo.png" not in item for item in urls)


async def test_sitemap_duplicates_counted(url):
    async with AsyncCrawler(parser=HTMLParser()) as crawler:
        await crawler.sitemap.fetch_sitemap(url(SITEMAP_PATH))
        stats = crawler.sitemap.get_stats()

    assert stats["files"] == 1
    assert stats["duplicates"] == 1
    assert stats["errors"] == 0


async def test_index_map_collects_parts_and_counts_missing(url, hits):
    async with AsyncCrawler(parser=HTMLParser()) as crawler:
        urls = await crawler.sitemap.fetch_sitemap(url(SITEMAP_INDEX_PATH))
        stats = crawler.sitemap.get_stats()

    assert sorted(urls) == sorted([url("/site/a"), url("/site/b")])
    assert (stats["indexes"], stats["files"]) == (1, 3)
    assert stats["errors"] == 1  # третья карта индекса отсутствует
    assert all(hits[part] == 1 for part in SITEMAP_PARTS)


async def test_self_referencing_map_requested_once(url, hits):
    async with AsyncCrawler(parser=HTMLParser()) as crawler:
        assert await crawler.sitemap.fetch_sitemap(url(SITEMAP_LOOP_PATH)) == []

    assert hits[SITEMAP_LOOP_PATH] == 1


async def test_broken_map_is_error_not_crash(url):
    async with AsyncCrawler(parser=HTMLParser()) as crawler:
        urls = await crawler.sitemap.fetch_sitemap(url(SITEMAP_BROKEN_PATH))
        stats = crawler.sitemap.get_stats()

    # Обрывок XML parser чинит сам и отдаёт то, что успел прочитать: важно, что
    # обход не падает, а файл учтён — как разобранный или как ошибка.
    assert urls in ([], [url("/site/a")])
    assert stats["files"] + stats["errors"] == 1


async def test_gzipped_map_is_unpacked(url):
    """`sitemap.xml.gz` разрешён стандартом, и крупные сайты отдают только его."""
    async with AsyncCrawler(parser=HTMLParser()) as crawler:
        urls = await crawler.sitemap.fetch_sitemap(url(SITEMAP_GZ_PATH))
        stats = crawler.sitemap.get_stats()

    assert urls == [url("/site/a1")]
    assert (stats["files"], stats["errors"]) == (1, 0)


async def test_unpacked_map_is_capped_by_limit(url, monkeypatch, caplog):
    """Повторяющийся XML сжимается в сотни раз, поэтому лимит нужен и после распаковки.

    Сначала карта читается целиком, а потом — с уменьшенным лимитом: адресов
    становится заметно меньше, то есть распаковка остановилась на лимите, а не
    развернула архив в память полностью.
    """
    async with AsyncCrawler(parser=HTMLParser()) as crawler:
        full = await crawler.sitemap.fetch_sitemap(url(SITEMAP_BOMB_PATH))

    monkeypatch.setattr("src.crawler.SITEMAP_MAX_BYTES", 50_000)
    async with AsyncCrawler(parser=HTMLParser()) as crawler:
        with caplog.at_level("WARNING"):
            capped = await crawler.sitemap.fetch_sitemap(url(SITEMAP_BOMB_PATH))

    assert len(full) == SITEMAP_BOMB_URLS
    assert len(capped) < SITEMAP_BOMB_URLS // 2
    assert "обрезан при распаковке" in caplog.text


async def test_empty_map_gives_no_urls(url):
    async with AsyncCrawler(parser=HTMLParser()) as crawler:
        assert await crawler.sitemap.fetch_sitemap(url(SITEMAP_EMPTY_PATH)) == []
        assert crawler.sitemap.get_stats()["errors"] == 0


# --- поиск карты и стартовые адреса -----------------------------------------


async def test_discover_sitemaps_reads_robots(url, robots_txt):
    robots_txt(f"User-agent: *\nSitemap: {url(SITEMAP_INDEX_PATH)}\n")

    async with AsyncCrawler(parser=HTMLParser()) as crawler:
        found = await crawler.discover_sitemaps(url("/site/"))

    assert found == [url(SITEMAP_INDEX_PATH)]


async def test_relative_sitemap_directive_resolved(url, robots_txt):
    """Относительный `Sitemap:` встречается в живых robots.txt — не теряем его."""
    robots_txt(f"User-agent: *\nSitemap: {SITEMAP_PATH}\n")

    async with AsyncCrawler(parser=HTMLParser()) as crawler:
        found = await crawler.discover_sitemaps(url("/site/"))
        seeds = await crawler.seeds_from_sitemaps([SITEMAP_AUTO], base_urls=[url("/site/")])

    assert found == [url(SITEMAP_PATH)]
    assert seeds[0] == url(SITEMAP_ORDER[0])


async def test_discover_sitemaps_falls_back_to_default_path(url, robots_txt):
    robots_txt("User-agent: *\nDisallow:\n")

    async with AsyncCrawler(parser=HTMLParser()) as crawler:
        found = await crawler.discover_sitemaps(url("/site/"))

    assert found == [url("/sitemap.xml")]


async def test_robots_sitemap_does_not_split_group():
    """`Sitemap:` между двумя `User-agent` не разрывает одну группу на две."""
    rules = parse_robots(
        "User-agent: A\n"
        "Sitemap: https://example.com/sitemap.xml\n"
        "User-agent: B\n"
        "Disallow: /both\n"
    )

    assert rules.sitemaps == ("https://example.com/sitemap.xml",)
    assert rules.decide("/both", "A")[0] is False
    assert rules.decide("/both", "B")[0] is False


async def test_seeds_sorted_by_priority(url):
    async with AsyncCrawler(parser=HTMLParser()) as crawler:
        seeds = await crawler.seeds_from_sitemaps([url(SITEMAP_PATH)])

    expected = [url(path) for path in SITEMAP_ORDER]
    assert seeds[: len(expected)] == expected


async def test_seeds_limit_keeps_most_important(url):
    async with AsyncCrawler(parser=HTMLParser()) as crawler:
        seeds = await crawler.seeds_from_sitemaps([url(SITEMAP_PATH)], limit=2)

    assert seeds == [url(SITEMAP_ORDER[0]), url(SITEMAP_ORDER[1])]


async def test_seeds_auto_uses_robots(url, robots_txt):
    robots_txt(f"User-agent: *\nSitemap: {url(SITEMAP_PATH)}\n")

    async with AsyncCrawler(parser=HTMLParser()) as crawler:
        seeds = await crawler.seeds_from_sitemaps(
            [SITEMAP_AUTO], base_urls=[url("/site/")]
        )

    assert seeds[0] == url(SITEMAP_ORDER[0])


async def test_seeds_without_map_are_empty_not_error(url, robots_txt):
    """Карты по указанному адресу нет — пустой список, а не исключение."""
    robots_txt(f"User-agent: *\nSitemap: {url('/nowhere.xml')}\n")

    async with AsyncCrawler(parser=HTMLParser()) as crawler:
        seeds = await crawler.seeds_from_sitemaps(
            [SITEMAP_AUTO], base_urls=[url("/site/")]
        )

    assert seeds == []
    assert crawler.sitemap.get_stats()["errors"] == 1


async def test_crawl_from_sitemap_seeds(url, server: TestServer, hits):
    async with AsyncCrawler(parser=HTMLParser(), max_depth=0) as crawler:
        seeds = await crawler.seeds_from_sitemaps([url(SITEMAP_PATH)], limit=2)
        pages = await crawler.crawl(seeds, max_pages=2)

    # Порядок страниц задаёт не очередь, а то, кто первым ответил.
    assert sorted(page["url"] for page in pages) == sorted(seeds)
    assert hits["/site/b"] == 1


async def test_second_run_does_not_count_first(url):
    """Второй прогон не должен показывать карты и адреса первого."""
    async with AsyncCrawler(parser=HTMLParser()) as crawler:
        await crawler.seeds_from_sitemaps([url(SITEMAP_PATH)])
        first = crawler.sitemap.get_stats()
        await crawler.seeds_from_sitemaps([url(SITEMAP_PATH)])
        second = crawler.sitemap.get_stats()

    assert (first["files"], first["urls"]) == (1, 5)
    assert (second["files"], second["urls"]) == (1, 5)


async def test_sitemap_stats_in_crawl_stats(url):
    async with AsyncCrawler(parser=HTMLParser()) as crawler:
        before = crawler.get_crawl_stats()["sitemap"]
        await crawler.seeds_from_sitemaps([url(SITEMAP_PATH)])
        after = crawler.get_crawl_stats()["sitemap"]

    assert before["enabled"] is False
    assert after["enabled"] is True
    assert after["files"] == 1


async def test_sitemap_request_respects_rate_limit(url):
    """Карта — обычный запрос к сайту: лимит скорости на неё тоже действует."""
    async with AsyncCrawler(
        parser=HTMLParser(), requests_per_second=50
    ) as crawler:
        await crawler.sitemap.fetch_sitemap(url(SITEMAP_INDEX_PATH))
        stats = crawler.rate_limiter.get_stats()

    assert stats["requests"] >= 4
