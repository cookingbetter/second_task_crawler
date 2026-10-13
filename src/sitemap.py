"""Загрузка и разбор sitemap.xml, включая индексные карты (День 7).

Карта сайта — самый дешёвый источник адресов: сайт сам перечисляет страницы,
которые хочет отдать, и обходить его по ссылкам ради того же набора незачем.
"""

from __future__ import annotations

import asyncio
import logging
import warnings
from collections import deque
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urljoin, urlsplit

from bs4 import BeautifulSoup, FeatureNotFound, XMLParsedAsHTMLWarning

from src.filters import normalize_url

logger = logging.getLogger(__name__)

XML_PARSER = "lxml-xml"
# Откат на случай сборки без lxml: html.parser разберёт теги карты, хотя и
# считает документ HTML — отсюда фильтр предупреждения ниже.
FALLBACK_PARSER = "html.parser"
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

# Корневые теги двух видов карт: обычной и индексной.
TAG_URLSET = "urlset"
TAG_INDEX = "sitemapindex"

KIND_URLSET = "urlset"
KIND_INDEX = "index"
KIND_UNKNOWN = "unknown"

# Пределы разворачивания. Индексы вкладывают друг в друга редко, зато карта,
# ссылающаяся сама на себя, встречается, и остановить обход должны лимиты, а не
# терпение пользователя.
MAX_DEPTH = 3
MAX_URLS = 50_000
MAX_SITEMAPS = 50

# Приоритет по умолчанию из спецификации sitemaps.org: запись без <priority>
# считается средней по важности.
DEFAULT_PRIORITY = 0.5

ALLOWED_SCHEMES = ("http", "https")

# Значение вместо адреса карты, означающее «найди её сам»: по robots.txt, а
# если там её нет — по стандартному /sitemap.xml.
SITEMAP_AUTO = "auto"


@dataclass(slots=True)
class SitemapResponse:
    """Ответ на запрос карты сайта — ровно то, что нужно парсеру."""

    status: int | None = None
    text: str = ""
    error: str | None = None


Fetcher = Callable[[str], Awaitable[SitemapResponse]]


@dataclass(slots=True, frozen=True)
class SitemapEntry:
    """Одна запись `<url>`: адрес и подсказки сайта о нём."""

    loc: str
    lastmod: str | None = None
    changefreq: str | None = None
    priority: float | None = None
    # Из какой карты пришла запись: у индексных карт это важно для отчёта.
    source: str = ""

    @property
    def weight(self) -> float:
        """Приоритет для сортировки, с подстановкой значения по умолчанию."""
        return DEFAULT_PRIORITY if self.priority is None else self.priority


@dataclass(slots=True)
class SitemapDocument:
    """Разобранный файл: либо записи страниц, либо ссылки на другие карты."""

    kind: str = KIND_UNKNOWN
    entries: list[SitemapEntry] = field(default_factory=list)
    sitemaps: list[str] = field(default_factory=list)


def _make_soup(text: str) -> BeautifulSoup:
    """Строит дерево XML, откатываясь на встроенный парсер без lxml."""
    try:
        return BeautifulSoup(text, XML_PARSER)
    except FeatureNotFound:
        logger.warning(
            "Парсер %r недоступен, карта разбирается как %s", XML_PARSER, FALLBACK_PARSER
        )
        return BeautifulSoup(text, FALLBACK_PARSER)


def _child_text(tag: Any, name: str) -> str:
    """Текст дочернего тега без учёта namespace карты.

    Только прямые дети и только теги без префикса: внутри `<url>` лежит ещё и
    `<image:loc>`, и картинка не должна попасть в список страниц.
    """
    for child in tag.find_all(name, recursive=False):
        if not child.prefix:
            return child.get_text(strip=True)
    return ""


def _absolute(loc: str, base_url: str) -> str:
    """Приводит адрес из карты к абсолютному и каноничному виду.

    Спецификация требует абсолютных адресов, но относительные в картах
    встречаются, и достроить их по адресу самой карты дешевле, чем потерять.
    """
    if not loc:
        return ""
    absolute = urljoin(base_url, loc) if base_url else loc
    if urlsplit(absolute).scheme not in ALLOWED_SCHEMES:
        logger.debug("Адрес из карты сайта не загрузить: %s", loc)
        return ""
    return normalize_url(absolute)


def _priority(value: str, url: str) -> float | None:
    """Разбирает `<priority>`; мусор и значения вне 0..1 отбрасываются."""
    if not value:
        return None
    try:
        priority = float(value)
    except ValueError:
        logger.debug("Нечисловой priority %r в карте %s", value, url)
        return None
    if not 0.0 <= priority <= 1.0:
        logger.debug("priority вне диапазона 0..1: %r в карте %s", value, url)
        return None
    return priority


def parse_sitemap(text: str, base_url: str = "") -> SitemapDocument:
    """Разбирает карту сайта или индекс карт.

    Вид файла определяется корневым тегом, а не содержимым: по спецификации
    `<urlset>` и `<sitemapindex>` смешивать нельзя, и угадывать тут нечего.
    Файл, в котором нет ни того, ни другого корня, возвращается с
    `kind=unknown` — решение, считать это ошибкой, принимает вызывающий.
    """
    soup = _make_soup(text or "")
    root = soup.find([TAG_URLSET, TAG_INDEX])
    if root is None:
        return SitemapDocument()

    if root.name == TAG_INDEX:
        sitemaps = []
        for item in root.find_all("sitemap"):
            if loc := _absolute(_child_text(item, "loc"), base_url):
                sitemaps.append(loc)
        return SitemapDocument(kind=KIND_INDEX, sitemaps=sitemaps)

    entries = []
    for item in root.find_all("url"):
        loc = _absolute(_child_text(item, "loc"), base_url)
        if not loc:
            continue
        entries.append(
            SitemapEntry(
                loc=loc,
                lastmod=_child_text(item, "lastmod") or None,
                changefreq=_child_text(item, "changefreq") or None,
                priority=_priority(_child_text(item, "priority"), base_url),
                source=base_url,
            )
        )
    return SitemapDocument(kind=KIND_URLSET, entries=entries)


def sort_by_priority(entries: Iterable[SitemapEntry]) -> list[SitemapEntry]:
    """Сортирует записи по убыванию приоритета.

    Сортировка устойчивая, поэтому внутри одного приоритета сохраняется порядок
    карты: сайт перечисляет страницы не случайно.
    """
    return sorted(entries, key=lambda entry: -entry.weight)


class SitemapParser:
    """Собирает адреса страниц из карт сайта, разворачивая индексные карты.

    Загрузку делает не сам: `fetcher` передаётся снаружи, как и у
    `RobotsParser`, поэтому класс не знает ни про aiohttp, ни про сессию
    краулера, а тесты обходятся без сети.

    Обход индексов — в ширину, с множеством уже загруженных адресов и тремя
    лимитами (глубина, число файлов, число URL): карта, ссылающаяся сама на
    себя или на миллион страниц, не должна останавливать работу.
    """

    def __init__(
        self,
        fetcher: Fetcher,
        *,
        max_depth: int = MAX_DEPTH,
        max_urls: int = MAX_URLS,
        max_sitemaps: int = MAX_SITEMAPS,
    ) -> None:
        if max_depth < 0:
            raise ValueError("max_depth должен быть >= 0")
        if max_urls < 1:
            raise ValueError("max_urls должен быть >= 1")
        if max_sitemaps < 1:
            raise ValueError("max_sitemaps должен быть >= 1")

        self._fetcher = fetcher
        self.max_depth = max_depth
        self.max_urls = max_urls
        self.max_sitemaps = max_sitemaps

        self.files = 0
        self.indexes = 0
        self.urls = 0
        self.duplicates = 0
        self.errors = 0
        self.depth_reached = 0
        self.truncated = False

    async def fetch_sitemap(self, sitemap_url: str) -> list[str]:
        """Загружает карту сайта и отдаёт адреса всех страниц из неё."""
        return [entry.loc for entry in await self.fetch_entries([sitemap_url])]

    async def fetch_sitemaps(self, sitemap_urls: Iterable[str]) -> list[str]:
        """То же для нескольких карт: адреса без повторов, в порядке карт."""
        return [entry.loc for entry in await self.fetch_entries(sitemap_urls)]

    async def fetch_entries(
        self, sitemap_urls: str | Iterable[str]
    ) -> list[SitemapEntry]:
        """Записи карт целиком: с `lastmod`, `changefreq` и `priority`."""
        sources = [sitemap_urls] if isinstance(sitemap_urls, str) else list(sitemap_urls)
        pending: deque[tuple[str, int]] = deque((url, 0) for url in sources)
        visited: set[str] = set()
        seen_locs: set[str] = set()
        entries: list[SitemapEntry] = []
        # Счётчики живут до `reset_stats`, а лог описывает один вызов: разницу
        # «до и после» можно сравнивать со сводкой прогона, а `len(visited)` —
        # нет, там ещё и попытки, которые кончились ошибкой.
        files_before, indexes_before = self.files, self.indexes
        errors_before = self.errors

        while pending:
            url, depth = pending.popleft()
            key = normalize_url(url)
            if key in visited:
                logger.debug("Карту уже загружали, пропуск: %s", url)
                continue
            if len(visited) >= self.max_sitemaps:
                self._truncate(f"файлов карты больше {self.max_sitemaps}")
                break
            visited.add(key)

            document = await self._load(url)
            self.depth_reached = max(self.depth_reached, depth)

            if document.kind == KIND_INDEX:
                self.indexes += 1
                if depth >= self.max_depth:
                    logger.warning(
                        "Индекс карт глубже %d не разворачивается: %s",
                        self.max_depth,
                        url,
                    )
                    self._truncate(f"вложенность индексов больше {self.max_depth}")
                    continue
                pending.extend((child, depth + 1) for child in document.sitemaps)
                continue

            for entry in document.entries:
                if entry.loc in seen_locs:
                    self.duplicates += 1
                    continue
                if len(entries) >= self.max_urls:
                    self._truncate(f"адресов больше {self.max_urls}")
                    pending.clear()
                    break
                seen_locs.add(entry.loc)
                entries.append(entry)

        self.urls += len(entries)
        logger.info(
            "Карты сайта разобраны: файлов %d (индексов %d), адресов %d, ошибок %d",
            self.files - files_before,
            self.indexes - indexes_before,
            len(entries),
            self.errors - errors_before,
        )
        return entries

    def reset_stats(self) -> None:
        """Обнуляет счётчики: второй обход не должен показывать цифры первого."""
        self.files = 0
        self.indexes = 0
        self.urls = 0
        self.duplicates = 0
        self.errors = 0
        self.depth_reached = 0
        self.truncated = False

    def get_stats(self) -> dict[str, Any]:
        """Сколько карт загружено и что из них вышло."""
        return {
            "files": self.files,
            "indexes": self.indexes,
            "urls": self.urls,
            "duplicates": self.duplicates,
            "errors": self.errors,
            "depth_reached": self.depth_reached,
            "truncated": self.truncated,
            "max_depth": self.max_depth,
            "max_urls": self.max_urls,
        }

    # --- внутреннее ---------------------------------------------------------

    def _truncate(self, reason: str) -> None:
        self.truncated = True
        logger.warning("Разбор карт сайта остановлен: %s", reason)

    async def _load(self, url: str) -> SitemapDocument:
        """Загружает и разбирает один файл; любая беда даёт пустой документ."""
        try:
            response = await self._fetcher(url)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # падение загрузчика не должно рвать обход
            return self._failed(url, f"{type(exc).__name__}: {exc}")

        if response.error is not None:
            return self._failed(url, response.error)
        if response.status is not None and response.status >= 400:
            return self._failed(url, f"статус {response.status}")

        # Разбор синхронный и целиком CPU-bound, а карты бывают на десятки
        # мегабайт: в потоке он не держит event loop.
        document = await asyncio.to_thread(parse_sitemap, response.text, url)
        if document.kind == KIND_UNKNOWN:
            return self._failed(url, "в файле нет ни <urlset>, ни <sitemapindex>")

        self.files += 1
        logger.info(
            "Карта сайта разобрана: %s (%s, записей %d)",
            url,
            document.kind,
            len(document.entries) or len(document.sitemaps),
        )
        return document

    def _failed(self, url: str, reason: str) -> SitemapDocument:
        self.errors += 1
        logger.warning("Карта сайта не прочитана: %s — %s", url, reason)
        return SitemapDocument()
