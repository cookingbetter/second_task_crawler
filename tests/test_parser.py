"""Тесты парсинга HTML и извлечения данных (День 2)."""

from __future__ import annotations

import logging
import subprocess
import sys
import warnings
from pathlib import Path

import pytest
from bs4 import MarkupResemblesLocatorWarning

from src.crawler import AsyncCrawler
from src.models import ERROR_HTTP
from src.parser import FALLBACK_PARSER, HTMLParser
from tests.conftest import EXTERNAL_LINK, PAGE_BODY

BASE_URL = "https://site.test/dir/page"
REPO_ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture
def parser() -> HTMLParser:
    return HTMLParser()


@pytest.fixture
def soup(parser: HTMLParser):
    return parser.make_soup(PAGE_BODY)


@pytest.fixture
async def page(parser: HTMLParser) -> dict:
    return await parser.parse_html(PAGE_BODY, BASE_URL)


async def test_parse_valid_html(page):
    assert page["url"] == BASE_URL
    assert page["title"] == "Тестовая страница"
    assert page["warnings"] == []
    assert page["text_length"] == len(page["text"])
    assert "Основной текст страницы." in page["text"]


async def test_text_skips_scripts_and_hidden_tags(page):
    assert "мусор из script" not in page["text"]
    assert "display: none" not in page["text"]
    assert "включите javascript" not in page["text"]


async def test_text_excludes_head_content(page):
    """Основной текст — это body, а не title с мета-тегами из head."""
    assert page["title"] == "Тестовая страница"
    assert "Тестовая страница" not in page["text"]
    assert "Страница для проверки парсера" not in page["text"]
    assert page["text"].startswith("Главный заголовок")


async def test_metadata_fields(page):
    metadata = page["metadata"]

    assert metadata["description"] == "Страница для проверки парсера"
    assert metadata["keywords"] == ["краулер", "парсинг", "тесты"]
    assert metadata["lang"] == "ru"
    assert metadata["canonical"] == "/page"
    assert metadata["og_title"] == "OG заголовок"


async def test_relative_links_become_absolute(page):
    assert page["links"] == [
        "https://site.test/about",
        "https://site.test/dir/nested/page.html",
        EXTERNAL_LINK,
    ]
    assert page["links_count"] == 3


def test_links_skip_fragments_and_unsupported_schemes(parser, soup):
    links = parser.extract_links(soup, BASE_URL)

    assert BASE_URL not in links  # href="#section" — якорь этой же страницы
    assert not [link for link in links if "mailto" in link or "javascript" in link]
    assert "#" not in "".join(links)


def test_same_domain_only_drops_external(parser, soup):
    same_domain = parser.extract_links(soup, BASE_URL, same_domain_only=True)
    all_links = parser.extract_links(soup, BASE_URL, same_domain_only=False)

    assert EXTERNAL_LINK in all_links
    assert EXTERNAL_LINK not in same_domain
    assert all(link.startswith("https://site.test/") for link in same_domain)


def test_same_domain_only_from_constructor(soup):
    strict = HTMLParser(same_domain_only=True)

    assert EXTERNAL_LINK not in strict.extract_links(soup, BASE_URL)


def test_extract_text_with_selector(parser, soup):
    text = parser.extract_text(soup, ".content")

    assert "Подзаголовок" in text
    assert "Главный заголовок" not in text


def test_unmatched_selector_falls_back_to_document(parser, soup, caplog):
    with caplog.at_level(logging.WARNING, logger="src.parser"):
        text = parser.extract_text(soup, ".no-such-block")

    assert "Главный заголовок" in text
    assert "ничего не нашёл" in caplog.text


def test_nested_selector_matches_are_not_duplicated(parser):
    """Селектор, совпавший и с родителем, и с потомком, не удваивает текст."""
    soup = parser.make_soup('<div class="c"><div class="c"><p>Текст</p></div></div>')

    assert parser.extract_text(soup, ".c") == "Текст"


def test_extract_images(parser, soup):
    images = parser.extract_images(soup, BASE_URL)

    assert images == [
        {"src": "https://site.test/static/logo.png", "alt": "Логотип"},
        {"src": "https://site.test/dir/photo.jpg", "alt": ""},
    ]


def test_extract_headings(parser, soup):
    assert parser.extract_headings(soup) == {
        "h1": ["Главный заголовок"],
        "h2": ["Подзаголовок"],
        "h3": ["Мелкий заголовок"],
    }


def test_extract_tables(parser, soup):
    assert parser.extract_tables(soup) == [
        {
            "headers": ["Город", "Страниц"],
            "rows": [["Москва", "12"], ["Казань", "7"]],
        }
    ]


def test_nested_table_does_not_pollute_outer(parser):
    """Строки и ячейки вложенной таблицы не подмешиваются во внешнюю."""
    soup = parser.make_soup(
        "<table><tr><th>A</th><th>B</th></tr>"
        "<tr><td>внешняя</td><td>"
        "<table><tr><td>внутренняя</td></tr></table>"
        "</td></tr></table>"
    )

    assert parser.extract_tables(soup) == [
        {"headers": ["A", "B"], "rows": [["внешняя", "внутренняя"]]},
        {"headers": [], "rows": [["внутренняя"]]},
    ]


def test_extract_lists(parser, soup):
    assert parser.extract_lists(soup) == [
        {"type": "ul", "items": ["первый", "второй"]},
        {"type": "ol", "items": ["шаг 1", "шаг 2", "шаг 3"]},
    ]


async def test_broken_html_does_not_break_parsing(parser, url):
    async with AsyncCrawler() as crawler:
        result = await crawler.fetch_one(url("/broken"))
    page = await parser.parse_html(result.text or "", BASE_URL)

    assert page["warnings"] == []
    assert "Текст без закрытия" in page["text"]
    assert page["links"] == ["https://site.test/broken/link"]
    assert page["lists"] == [{"type": "ul", "items": ["пункт"]}]


@pytest.mark.parametrize("html", ["", "   ", "совсем не разметка"])
async def test_degenerate_input_returns_empty_structure(parser, html):
    page = await parser.parse_html(html, BASE_URL)

    assert page["title"] == ""
    assert page["links"] == []
    assert page["images"] == []
    assert page["headings"] == {"h1": [], "h2": [], "h3": []}
    assert page["warnings"] == []


async def test_partial_result_when_extractor_fails(parser, monkeypatch):
    def boom(*args, **kwargs):
        raise ValueError("экстрактор сломался")

    monkeypatch.setattr(HTMLParser, "extract_links", boom)
    page = await parser.parse_html(PAGE_BODY, BASE_URL)

    assert page["links"] == []
    assert page["links_count"] == 0
    # Остальные поля должны быть заполнены: частичный результат, а не отказ.
    assert page["title"] == "Тестовая страница"
    assert page["images_count"] == 2
    assert any(note.startswith("links:") for note in page["warnings"])


def test_unknown_parser_falls_back(caplog):
    with caplog.at_level(logging.WARNING, logger="src.parser"):
        parser = HTMLParser("no-such-parser")

    assert parser.parser == FALLBACK_PARSER
    assert "недоступен" in caplog.text


@pytest.mark.parametrize("parser_name", ["lxml", FALLBACK_PARSER])
def test_title_with_inline_markup(parser_name):
    """Разметка внутри <title> не должна обнулять заголовок."""
    parser = HTMLParser(parser_name)
    soup = parser.make_soup("<html><head><title>Как <b>жить</b></title></head></html>")

    assert parser.extract_metadata(soup)["title"]


def test_base_href_changes_resolution_base(parser):
    """<base href> задаёт базу для всех относительных ссылок страницы."""
    soup = parser.make_soup(
        '<html><head><base href="https://cdn.site.test/v2/"></head>'
        '<body><a href="docs/a.html">a</a><img src="pic.png" alt=""></body></html>'
    )

    assert parser.extract_links(soup, BASE_URL) == [
        "https://cdn.site.test/v2/docs/a.html"
    ]
    assert parser.extract_images(soup, BASE_URL) == [
        {"src": "https://cdn.site.test/v2/pic.png", "alt": ""}
    ]


def test_inline_data_image_is_kept(parser):
    """data:-картинка — это само изображение, а не ссылка: её не отбрасываем."""
    src = "data:image/gif;base64,R0lGOD"
    soup = parser.make_soup(f'<img src="{src}" alt="инлайн">')

    assert parser.extract_images(soup, BASE_URL) == [{"src": src, "alt": "инлайн"}]


def test_locator_warning_is_silenced_in_real_process():
    """Мусорный ответ не должен печатать предупреждение bs4 на каждой странице.

    Проверяем подпроцессом: pytest оборачивает сбор тестов в
    `warnings.catch_warnings`, поэтому модульный фильтр внутри тестов уже может
    быть откатан и проверка в текущем процессе была бы недостоверной.
    """
    code = "from src.parser import HTMLParser; HTMLParser().make_soup('http://junk')"
    proc = subprocess.run(
        [sys.executable, "-W", "default", "-c", code],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )

    assert "MarkupResemblesLocatorWarning" not in proc.stderr


def test_junk_response_survives_warnings_as_errors(parser):
    """Политика -W error не должна превращать мусорный ответ в потерю страницы."""
    junk = "http://junk-response"

    with warnings.catch_warnings():
        warnings.simplefilter("error", MarkupResemblesLocatorWarning)
        soup = parser.make_soup(junk)

    assert parser.extract_text(soup) == junk


async def test_fetch_and_parse_resolves_links_against_server(url):
    async with AsyncCrawler() as crawler:
        page = await crawler.fetch_and_parse(url("/page"))

    assert page["ok"] is True
    assert page["status"] == 200
    assert page["title"] == "Тестовая страница"
    assert url("/about") in page["links"]
    assert url("/nested/page.html") in page["links"]
    assert page["images"][0]["src"] == url("/static/logo.png")
    assert page["elapsed"] > 0


async def test_fetch_and_parse_keeps_shape_on_http_error(url):
    async with AsyncCrawler() as crawler:
        page = await crawler.fetch_and_parse(url("/status/404"))

    assert page["ok"] is False
    assert page["error_type"] == ERROR_HTTP
    assert page["status"] == 404
    assert page["links"] == []
    assert page["title"] == ""
    assert page["text_length"] == 0


async def test_fetch_and_parse_many_collects_stats(url):
    urls = [url("/page"), url("/page"), url("/empty"), url("/status/500")]

    async with AsyncCrawler(max_concurrent=3) as crawler:
        pages = await crawler.fetch_and_parse_many(urls)
        stats = crawler.stats

    assert len(pages) == 3  # дубль отброшен до запуска задач
    assert stats.total == 3
    assert stats.succeeded == 2
    assert stats.failed == 1
    assert [page["ok"] for page in pages] == [True, True, False]
