"""Тесты разбора и обхода карт сайта (День 7)."""

from __future__ import annotations

import asyncio

import pytest

from src.sitemap import (
    DEFAULT_PRIORITY,
    KIND_INDEX,
    KIND_UNKNOWN,
    KIND_URLSET,
    SitemapEntry,
    SitemapParser,
    SitemapResponse,
    parse_sitemap,
    sort_by_priority,
)

URLSET = """<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"
        xmlns:image="http://www.google.com/schemas/sitemap-image/1.1">
  <url>
    <loc>https://example.com/first</loc>
    <lastmod>2026-01-02</lastmod>
    <changefreq>daily</changefreq>
    <priority>0.8</priority>
    <image:image><image:loc>https://example.com/logo.png</image:loc></image:image>
  </url>
  <url><loc>https://example.com/second</loc></url>
</urlset>
"""

INDEX = """<?xml version="1.0" encoding="UTF-8"?>
<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <sitemap><loc>https://example.com/map-1.xml</loc></sitemap>
  <sitemap><loc>https://example.com/map-2.xml</loc></sitemap>
</sitemapindex>
"""

MAP_URL = "https://example.com/sitemap.xml"


def fake_fetcher(
    pages: dict[str, str],
    *,
    calls: list[str] | None = None,
    status: int | None = 200,
    error: str | None = None,
):
    """Загрузчик карт без сети: отдаёт тело по адресу, иначе 404."""

    async def _fetch(url: str) -> SitemapResponse:
        if calls is not None:
            calls.append(url)
        if url not in pages:
            return SitemapResponse(status=404, text="")
        return SitemapResponse(status=status, text=pages[url], error=error)

    return _fetch


# --- разбор файла -----------------------------------------------------------


def test_parse_urlset_reads_hints():
    document = parse_sitemap(URLSET, MAP_URL)

    assert document.kind == KIND_URLSET
    first = document.entries[0]
    assert first.loc == "https://example.com/first"
    assert (first.lastmod, first.changefreq, first.priority) == ("2026-01-02", "daily", 0.8)
    assert first.source == MAP_URL


def test_parse_ignores_image_loc():
    """`<image:loc>` лежит внутри `<url>`, но это картинка, а не страница."""
    document = parse_sitemap(URLSET, MAP_URL)

    assert [entry.loc for entry in document.entries] == [
        "https://example.com/first",
        "https://example.com/second",
    ]


def test_entry_without_priority_weighs_default():
    document = parse_sitemap(URLSET, MAP_URL)

    assert document.entries[1].priority is None
    assert document.entries[1].weight == DEFAULT_PRIORITY


def test_parse_index_returns_sitemaps_not_entries():
    document = parse_sitemap(INDEX, MAP_URL)

    assert document.kind == KIND_INDEX
    assert document.entries == []
    assert document.sitemaps == [
        "https://example.com/map-1.xml",
        "https://example.com/map-2.xml",
    ]


@pytest.mark.parametrize(
    "text",
    ["", "   ", "не xml вовсе", "<html><body><a href='/a'>нет</a></body></html>"],
)
def test_parse_without_root_tag_is_unknown(text: str):
    assert parse_sitemap(text, MAP_URL).kind == KIND_UNKNOWN


def test_parse_survives_broken_xml():
    document = parse_sitemap("<urlset><url><loc>/a</loc", MAP_URL)

    # Разбор не падает; что именно удалось вытащить — дело парсера.
    assert document.kind == KIND_URLSET


def test_relative_loc_resolves_against_map():
    document = parse_sitemap(
        "<urlset><url><loc>page/one</loc></url></urlset>",
        "https://example.com/maps/sitemap.xml",
    )

    assert document.entries[0].loc == "https://example.com/maps/page/one"


@pytest.mark.parametrize("loc", ["mailto:a@example.com", "javascript:void(0)", "ftp://host/f"])
def test_unfetchable_loc_dropped(loc: str):
    document = parse_sitemap(f"<urlset><url><loc>{loc}</loc></url></urlset>", MAP_URL)

    assert document.entries == []


@pytest.mark.parametrize("value", ["", "очень важно", "1.5", "-0.2"])
def test_bad_priority_becomes_none(value: str):
    document = parse_sitemap(
        f"<urlset><url><loc>https://example.com/a</loc>"
        f"<priority>{value}</priority></url></urlset>",
        MAP_URL,
    )

    assert document.entries[0].priority is None


def test_sort_by_priority_is_stable():
    entries = [
        SitemapEntry(loc="/a", priority=0.1),
        SitemapEntry(loc="/b"),          # 0.5 по умолчанию
        SitemapEntry(loc="/c", priority=0.9),
        SitemapEntry(loc="/d"),          # тоже 0.5, порядок карты сохраняется
    ]

    assert [entry.loc for entry in sort_by_priority(entries)] == ["/c", "/b", "/d", "/a"]


# --- обход карт -------------------------------------------------------------


async def test_fetch_sitemap_returns_urls():
    parser = SitemapParser(fake_fetcher({MAP_URL: URLSET}))

    urls = await parser.fetch_sitemap(MAP_URL)

    assert urls == ["https://example.com/first", "https://example.com/second"]
    assert parser.get_stats()["files"] == 1


async def test_index_expands_recursively():
    pages = {
        MAP_URL: INDEX,
        "https://example.com/map-1.xml": URLSET,
        "https://example.com/map-2.xml": (
            "<urlset><url><loc>https://example.com/third</loc></url></urlset>"
        ),
    }
    parser = SitemapParser(fake_fetcher(pages))

    urls = await parser.fetch_sitemap(MAP_URL)

    assert urls == [
        "https://example.com/first",
        "https://example.com/second",
        "https://example.com/third",
    ]
    stats = parser.get_stats()
    assert (stats["files"], stats["indexes"], stats["errors"]) == (3, 1, 0)


async def test_duplicates_counted_once():
    text = (
        "<urlset>"
        "<url><loc>https://example.com/a</loc></url>"
        "<url><loc>https://example.com/a?</loc></url>"
        "<url><loc>https://example.com/b</loc></url>"
        "</urlset>"
    )
    parser = SitemapParser(fake_fetcher({MAP_URL: text}))

    urls = await parser.fetch_sitemap(MAP_URL)

    assert urls == ["https://example.com/a", "https://example.com/b"]
    assert parser.get_stats()["duplicates"] == 1


async def test_self_referencing_index_loads_once():
    """Карта, ссылающаяся на себя, останавливается множеством посещённых."""
    calls: list[str] = []
    text = f"<sitemapindex><sitemap><loc>{MAP_URL}</loc></sitemap></sitemapindex>"
    parser = SitemapParser(fake_fetcher({MAP_URL: text}, calls=calls))

    assert await parser.fetch_sitemap(MAP_URL) == []
    assert calls == [MAP_URL]


async def test_max_sitemaps_limits_files():
    pages = {
        MAP_URL: (
            "<sitemapindex>"
            "<sitemap><loc>https://example.com/m1.xml</loc></sitemap>"
            "<sitemap><loc>https://example.com/m2.xml</loc></sitemap>"
            "<sitemap><loc>https://example.com/m3.xml</loc></sitemap>"
            "</sitemapindex>"
        ),
    }
    for number in (1, 2, 3):
        pages[f"https://example.com/m{number}.xml"] = (
            f"<urlset><url><loc>https://example.com/p{number}</loc></url></urlset>"
        )
    calls: list[str] = []
    parser = SitemapParser(fake_fetcher(pages, calls=calls), max_sitemaps=2)

    urls = await parser.fetch_sitemap(MAP_URL)

    assert urls == ["https://example.com/p1"]
    assert len(calls) == 2
    assert parser.get_stats()["truncated"] is True


async def test_max_urls_limits_entries():
    text = "<urlset>" + "".join(
        f"<url><loc>https://example.com/p{n}</loc></url>" for n in range(5)
    ) + "</urlset>"
    parser = SitemapParser(fake_fetcher({MAP_URL: text}), max_urls=2)

    urls = await parser.fetch_sitemap(MAP_URL)

    assert urls == ["https://example.com/p0", "https://example.com/p1"]
    assert parser.get_stats()["truncated"] is True


async def test_max_depth_stops_nested_indexes():
    pages = {
        MAP_URL: "<sitemapindex><sitemap><loc>https://example.com/l1.xml</loc>"
        "</sitemap></sitemapindex>",
        "https://example.com/l1.xml": (
            "<sitemapindex><sitemap><loc>https://example.com/l2.xml</loc>"
            "</sitemap></sitemapindex>"
        ),
        "https://example.com/l2.xml": (
            "<urlset><url><loc>https://example.com/deep</loc></url></urlset>"
        ),
    }
    parser = SitemapParser(fake_fetcher(pages), max_depth=1)

    assert await parser.fetch_sitemap(MAP_URL) == []
    stats = parser.get_stats()
    assert (stats["truncated"], stats["depth_reached"]) == (True, 1)


async def test_missing_map_counts_error():
    parser = SitemapParser(fake_fetcher({}))

    assert await parser.fetch_sitemap(MAP_URL) == []
    assert parser.get_stats()["errors"] == 1


async def test_file_without_root_counts_error():
    parser = SitemapParser(fake_fetcher({MAP_URL: "<html>не карта</html>"}))

    assert await parser.fetch_sitemap(MAP_URL) == []
    assert parser.get_stats()["errors"] == 1


async def test_fetcher_exception_does_not_break_walk():
    async def _fetch(url: str) -> SitemapResponse:
        if url == MAP_URL:
            return SitemapResponse(
                status=200,
                text=f"<sitemapindex><sitemap><loc>{MAP_URL}-broken</loc></sitemap>"
                f"<sitemap><loc>{MAP_URL}-ok</loc></sitemap></sitemapindex>",
            )
        if url.endswith("broken"):
            raise RuntimeError("сеть упала")
        return SitemapResponse(
            status=200,
            text="<urlset><url><loc>https://example.com/ok</loc></url></urlset>",
        )

    parser = SitemapParser(_fetch)

    assert await parser.fetch_sitemap(MAP_URL) == ["https://example.com/ok"]
    assert parser.get_stats()["errors"] == 1


async def test_fetcher_cancellation_propagates():
    async def _fetch(url: str) -> SitemapResponse:
        raise asyncio.CancelledError

    parser = SitemapParser(_fetch)

    with pytest.raises(asyncio.CancelledError):
        await parser.fetch_sitemap(MAP_URL)


async def test_fetch_sitemaps_keeps_order_without_duplicates():
    pages = {
        "https://example.com/m1.xml": (
            "<urlset><url><loc>https://example.com/a</loc></url>"
            "<url><loc>https://example.com/shared</loc></url></urlset>"
        ),
        "https://example.com/m2.xml": (
            "<urlset><url><loc>https://example.com/shared</loc></url>"
            "<url><loc>https://example.com/b</loc></url></urlset>"
        ),
    }
    parser = SitemapParser(fake_fetcher(pages))

    urls = await parser.fetch_sitemaps(list(pages))

    assert urls == [
        "https://example.com/a",
        "https://example.com/shared",
        "https://example.com/b",
    ]


async def test_reset_stats_clears_counters():
    parser = SitemapParser(fake_fetcher({MAP_URL: URLSET}))
    await parser.fetch_sitemap(MAP_URL)

    parser.reset_stats()

    stats = parser.get_stats()
    assert (stats["files"], stats["urls"], stats["errors"]) == (0, 0, 0)


async def test_summary_log_matches_stats(caplog):
    """Итоговая строка в логе и сводка прогона должны сходиться.

    Недоступная карта — это попытка, а не разобранный файл, и в обоих местах
    она должна попасть в ошибки.
    """
    pages = {
        MAP_URL: (
            f"<sitemapindex><sitemap><loc>{MAP_URL}-ok</loc></sitemap>"
            f"<sitemap><loc>{MAP_URL}-missing</loc></sitemap></sitemapindex>"
        ),
        f"{MAP_URL}-ok": URLSET,
    }
    parser = SitemapParser(fake_fetcher(pages))

    with caplog.at_level("INFO"):
        await parser.fetch_sitemap(MAP_URL)

    stats = parser.get_stats()
    assert (stats["files"], stats["indexes"], stats["errors"]) == (2, 1, 1)
    assert (
        f"файлов {stats['files']} (индексов {stats['indexes']}),"
        f" адресов {stats['urls']}, ошибок {stats['errors']}"
    ) in caplog.text


async def test_summary_log_counts_only_current_call(caplog):
    """Счётчики накапливаются до `reset_stats`, а строка в логе — про один вызов."""
    parser = SitemapParser(fake_fetcher({MAP_URL: URLSET}))
    await parser.fetch_sitemap(MAP_URL)

    caplog.clear()
    with caplog.at_level("INFO"):
        await parser.fetch_sitemap(MAP_URL)

    assert parser.get_stats()["files"] == 2
    assert "файлов 1 (индексов 0), адресов 2, ошибок 0" in caplog.text


@pytest.mark.parametrize(
    "kwargs",
    [{"max_depth": -1}, {"max_urls": 0}, {"max_sitemaps": 0}],
)
def test_bad_limits_rejected(kwargs: dict[str, int]):
    with pytest.raises(ValueError):
        SitemapParser(fake_fetcher({}), **kwargs)
