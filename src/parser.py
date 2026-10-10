"""Парсинг HTML и извлечение структурированных данных."""

from __future__ import annotations

import asyncio
import logging
import re
import warnings
from collections.abc import Callable
from html import escape
from typing import Any
from urllib.parse import urldefrag, urljoin, urlparse

from bs4 import BeautifulSoup, FeatureNotFound, MarkupResemblesLocatorWarning
from bs4.element import NavigableString, Tag

logger = logging.getLogger(__name__)

NOISE_TAGS = frozenset({"script", "style", "noscript", "template"})
HEADING_TAGS = ("h1", "h2", "h3")
ALLOWED_SCHEMES = frozenset({"http", "https"})
FALLBACK_PARSER = "html.parser"

_WHITESPACE = re.compile(r"\s+")

# bs4 принимает короткий ответ без тегов за путь к файлу или URL и предупреждает
# об этом. Для краулера это обычный мусорный ответ, а не повод шуметь на каждой
# странице. Фильтр ставится один раз при импорте: `warnings.catch_warnings` для
# этого не годится — он правит глобальный список фильтров, а разбор идёт в
# нескольких потоках, и потоки затирали бы фильтры друг друга.
warnings.filterwarnings("ignore", category=MarkupResemblesLocatorWarning)


def empty_page(url: str) -> dict[str, Any]:
    """Пустой результат парсинга — одна и та же форма словаря при любой ошибке."""
    return {
        "url": url,
        "title": "",
        "text": "",
        "text_length": 0,
        "links": [],
        "links_count": 0,
        "images": [],
        "images_count": 0,
        "headings": {tag: [] for tag in HEADING_TAGS},
        "tables": [],
        "lists": [],
        "metadata": {},
        "warnings": [],
    }


def _clean(text: str) -> str:
    """Сжимает любые пробельные последовательности до одного пробела."""
    return _WHITESPACE.sub(" ", text).strip()


class HTMLParser:
    """Извлекает из HTML текст, ссылки, метаданные и прочие структуры.

    Ни один метод не поднимает исключение наружу: битая разметка — норма для
    краулера, поэтому сбой одного извлечения даёт частичный результат и запись
    в `warnings`, а не падение всего запуска.
    """

    def __init__(
        self,
        parser: str = "lxml",
        *,
        same_domain_only: bool = False,
        text_selector: str | None = None,
    ) -> None:
        self.parser = self._resolve_parser(parser)
        self.same_domain_only = same_domain_only
        self.text_selector = text_selector

    @staticmethod
    def _resolve_parser(name: str) -> str:
        """Проверяет доступность парсера, откатываясь на встроенный."""
        if name == FALLBACK_PARSER:
            return name
        try:
            BeautifulSoup("", name)
        except FeatureNotFound:
            logger.warning(
                "Парсер %r недоступен, используется %s", name, FALLBACK_PARSER
            )
            return FALLBACK_PARSER
        return name

    async def parse_html(self, html: str, url: str) -> dict[str, Any]:
        """Разбирает HTML и возвращает словарь с извлечёнными данными.

        Разбор синхронный и целиком CPU-bound, поэтому уходит в поток: иначе
        большая страница блокировала бы event loop, пока остальные запросы
        ждут сеть.
        """
        return await asyncio.to_thread(self._parse_sync, html or "", url)

    def make_soup(self, html: str) -> BeautifulSoup:
        """Строит дерево из разметки любого качества."""
        try:
            return BeautifulSoup(html, self.parser)
        except MarkupResemblesLocatorWarning:
            # Приложение могло объявить предупреждения ошибками (-W error), и
            # тогда модульный фильтр не спасает. Терять страницу из-за этого
            # незачем: bs4 ругается только на короткий текст без тегов, то есть
            # разметки здесь и нет — оборачиваем ответ в минимальный документ.
            logger.debug("bs4 принял ответ за ссылку или путь, беру его как текст")
            return BeautifulSoup(
                f"<html><body>{escape(html)}</body></html>", self.parser
            )

    def _parse_sync(self, html: str, url: str) -> dict[str, Any]:
        page = empty_page(url)
        notes: list[str] = page["warnings"]

        try:
            soup = self.make_soup(html)
        except Exception as exc:  # разметка, которую не осилил даже lxml
            logger.warning("Не удалось разобрать HTML %s: %s", url, exc)
            notes.append(f"soup: {type(exc).__name__}: {exc}")
            return page

        metadata = self._safe("metadata", notes, {}, self.extract_metadata, soup)
        text = self._safe("text", notes, "", self.extract_text, soup)
        links = self._safe("links", notes, [], self.extract_links, soup, url)
        images = self._safe("images", notes, [], self.extract_images, soup, url)
        headings = self._safe(
            "headings", notes, page["headings"], self.extract_headings, soup
        )

        page.update(
            title=metadata.get("title", ""),
            text=text,
            text_length=len(text),
            links=links,
            links_count=len(links),
            images=images,
            images_count=len(images),
            headings=headings,
            tables=self._safe("tables", notes, [], self.extract_tables, soup),
            lists=self._safe("lists", notes, [], self.extract_lists, soup),
            metadata=metadata,
        )
        logger.debug(
            "Разобрано %s: текст %d симв., ссылок %d, изображений %d",
            url,
            page["text_length"],
            page["links_count"],
            page["images_count"],
        )
        return page

    def _safe(
        self,
        field: str,
        notes: list[str],
        default: Any,
        func: Callable[..., Any],
        *args: Any,
    ) -> Any:
        """Выполняет извлечение, превращая исключение в предупреждение."""
        try:
            return func(*args)
        except Exception as exc:
            logger.warning("Ошибка извлечения %s: %s: %s", field, type(exc).__name__, exc)
            notes.append(f"{field}: {type(exc).__name__}: {exc}")
            return default

    def extract_links(
        self,
        soup: BeautifulSoup,
        base_url: str,
        *,
        same_domain_only: bool | None = None,
    ) -> list[str]:
        """Собирает абсолютные http(s)-ссылки без повторов и фрагментов."""
        if same_domain_only is None:
            same_domain_only = self.same_domain_only
        base_url = self._document_base(soup, base_url)
        base_host = urlparse(base_url).netloc.lower()

        links: dict[str, None] = {}
        skipped = 0
        for tag in soup.find_all("a", href=True):
            href = tag.get("href")
            if not isinstance(href, str) or not href.strip():
                continue
            href = href.strip()
            if href.startswith("#"):  # ссылка на якорь внутри этой же страницы
                continue

            absolute = self._absolute_url(base_url, href)
            if absolute is None:
                skipped += 1
                continue
            if same_domain_only and urlparse(absolute).netloc.lower() != base_host:
                skipped += 1
                continue
            links[absolute] = None

        if skipped:
            logger.debug("Отброшено %d ссылок на %s", skipped, base_url)
        return list(links)

    def _document_base(self, soup: BeautifulSoup, url: str) -> str:
        """Учитывает `<base href>`: он задаёт базу для всех ссылок страницы."""
        tag = soup.find("base", href=True)
        href = tag.get("href") if isinstance(tag, Tag) else None
        if not isinstance(href, str) or not href.strip():
            return url

        base = self._absolute_url(url, href.strip())
        if base is None:
            logger.debug("Непригодный <base href=%r>, база остаётся %s", href, url)
            return url

        logger.debug("Ссылки разрешаются относительно <base href>: %s", base)
        return base

    @staticmethod
    def _absolute_url(base_url: str, href: str) -> str | None:
        """Приводит ссылку к абсолютной, возвращая None для непригодных."""
        try:
            absolute, _fragment = urldefrag(urljoin(base_url, href))
            parsed = urlparse(absolute)
        except ValueError:  # например, битый IPv6-литерал в href
            return None

        # mailto:, javascript:, tel:, data: и прочее краулеру не загрузить.
        if parsed.scheme not in ALLOWED_SCHEMES or not parsed.netloc:
            return None
        return absolute

    def extract_text(self, soup: BeautifulSoup, selector: str | None = None) -> str:
        """Собирает видимый текст документа или блока, заданного селектором.

        Дерево не изменяется: вместо удаления шумных тегов пропускаются строки,
        лежащие внутри них, — иначе остальные извлечения работали бы с
        искалеченным деревом.
        """
        if selector is None:
            selector = self.text_selector

        roots = self._text_roots(soup, selector)
        chunks = [
            str(string)
            for root in roots
            for string in root.strings
            if not self._inside_noise(string)
        ]
        return _clean(" ".join(chunks))

    def _text_roots(self, soup: BeautifulSoup, selector: str | None) -> list[Tag]:
        """Выбирает узлы, из которых берётся текст."""
        if selector:
            found = soup.select(selector)
            if found:
                # Если селектор совпал и с родителем, и с его потомком, текст
                # потомка попал бы в результат дважды.
                return [
                    node
                    for node in found
                    if not any(parent in found for parent in node.parents)
                ]
            logger.warning("Селектор %r ничего не нашёл, беру весь документ", selector)

        # Основной текст живёт в body: иначе в него попадает title из head.
        # На фрагментах разметки html.parser body не создаёт — тогда берём всё.
        return [soup.body or soup]

    @staticmethod
    def _inside_noise(string: NavigableString) -> bool:
        return any(parent.name in NOISE_TAGS for parent in string.parents)

    def extract_metadata(self, soup: BeautifulSoup) -> dict[str, Any]:
        """Достаёт title, description, keywords и ещё пару полезных полей."""
        meta = self._meta_tags(soup)
        data: dict[str, Any] = {}

        # get_text, а не .string: при любой разметке внутри <title> (а html.parser
        # разбирает её как теги) .string равен None, и заголовок терялся бы.
        title = soup.title.get_text(" ", strip=True) if soup.title else ""
        title = _clean(title) or meta.get("og:title", "")
        if title:
            data["title"] = title

        description = meta.get("description") or meta.get("og:description", "")
        if description:
            data["description"] = _clean(description)

        keywords = [
            word.strip()
            for word in meta.get("keywords", "").split(",")
            if word.strip()
        ]
        if keywords:
            data["keywords"] = keywords

        html_tag = soup.find("html")
        lang = html_tag.get("lang") if isinstance(html_tag, Tag) else None
        if isinstance(lang, str) and lang.strip():
            data["lang"] = lang.strip()

        canonical = soup.find("link", rel="canonical")
        href = canonical.get("href") if isinstance(canonical, Tag) else None
        if isinstance(href, str) and href.strip():
            data["canonical"] = href.strip()

        for key, field in (("og:title", "og_title"), ("og:description", "og_description")):
            if meta.get(key):
                data[field] = _clean(meta[key])

        return data

    @staticmethod
    def _meta_tags(soup: BeautifulSoup) -> dict[str, str]:
        """Складывает все <meta> в словарь за один проход по дереву."""
        found: dict[str, str] = {}
        for tag in soup.find_all("meta"):
            key = tag.get("name") or tag.get("property") or tag.get("http-equiv")
            content = tag.get("content")
            if not isinstance(key, str) or not isinstance(content, str):
                continue
            if key.strip() and content.strip():
                found.setdefault(key.strip().lower(), content.strip())
        return found

    def extract_images(
        self, soup: BeautifulSoup, base_url: str
    ) -> list[dict[str, str]]:
        """Собирает изображения с абсолютным src и текстовой альтернативой."""
        base_url = self._document_base(soup, base_url)

        images: list[dict[str, str]] = []
        for tag in soup.find_all("img"):
            src = tag.get("src")
            if not isinstance(src, str) or not src.strip():
                continue
            src = src.strip()

            if src.startswith("data:"):
                # Инлайн-картинка: это уже само изображение, разрешать нечего.
                absolute: str | None = src
            else:
                absolute = self._absolute_url(base_url, src)
            if absolute is None:
                logger.debug("Пропущен src изображения %r на %s", src, base_url)
                continue

            alt = tag.get("alt")
            images.append(
                {
                    "src": absolute,
                    "alt": _clean(alt) if isinstance(alt, str) else "",
                }
            )
        return images

    def extract_headings(self, soup: BeautifulSoup) -> dict[str, list[str]]:
        """Возвращает тексты заголовков h1-h3, сгруппированные по уровню."""
        return {
            name: [
                text
                for tag in soup.find_all(name)
                if (text := _clean(tag.get_text(" ", strip=True)))
            ]
            for name in HEADING_TAGS
        }

    def extract_tables(self, soup: BeautifulSoup) -> list[dict[str, Any]]:
        """Разбирает таблицы: первая строка из `th` считается заголовком.

        Вложенная таблица попадает в результат отдельным элементом, а её строки
        и ячейки не подмешиваются во внешнюю: `find_all` рекурсивен, поэтому
        строки фильтруются по ближайшей таблице, а ячейки берутся только прямые.
        """
        tables: list[dict[str, Any]] = []
        for table in soup.find_all("table"):
            headers: list[str] = []
            rows: list[list[str]] = []

            own_rows = [
                row for row in table.find_all("tr") if row.find_parent("table") is table
            ]
            for row in own_rows:
                cells = row.find_all(["th", "td"], recursive=False)
                values = [_clean(cell.get_text(" ", strip=True)) for cell in cells]
                if not values:
                    continue
                if not headers and not rows and all(c.name == "th" for c in cells):
                    headers = values
                    continue
                rows.append(values)

            if headers or rows:
                tables.append({"headers": headers, "rows": rows})
        return tables

    def extract_lists(self, soup: BeautifulSoup) -> list[dict[str, Any]]:
        """Собирает ul/ol; вложенные списки попадают в результат отдельно."""
        lists: list[dict[str, Any]] = []
        for tag in soup.find_all(["ul", "ol"]):
            items = [
                text
                for item in tag.find_all("li", recursive=False)
                if (text := _clean(item.get_text(" ", strip=True)))
            ]
            if items:
                lists.append({"type": tag.name, "items": items})
        return lists
