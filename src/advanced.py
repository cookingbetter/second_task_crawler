"""Финальная интеграция: один класс на все семь дней (День 7).

`AsyncCrawler` умеет загружать, разбирать и обходить; `AdvancedCrawler` решает,
что обходить и что с результатом делать: читает конфигурацию, берёт адреса из
карты сайта, пишет данные в хранилища и собирает отчёты. Это композиция, а не
наследование: краулер остаётся библиотекой, которую можно взять без конфигов и
отчётов.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable
from datetime import datetime
from pathlib import Path
from types import TracebackType
from typing import Any

from src.config import CrawlerConfig
from src.crawler import AsyncCrawler
from src.parser import HTMLParser
from src.stats import CrawlerStats
from src.storage import DataStorage
from src.utils import setup_logging

logger = logging.getLogger(__name__)


class AdvancedCrawler:
    """Краулер, собранный по конфигурации, со статистикой и отчётами."""

    def __init__(
        self,
        config: CrawlerConfig,
        *,
        storage: DataStorage | None = None,
        crawler: AsyncCrawler | None = None,
    ) -> None:
        self.config = config
        # Хранилище создаётся по конфигурации, но его можно передать готовым —
        # так тесты подставляют своё, не трогая файлы.
        self.storage = storage if storage is not None else config.build_storage()
        self.crawler = crawler or AsyncCrawler(
            parser=HTMLParser(),
            storage=self.storage,
            **config.to_crawler_kwargs(),
        )
        self.pages: list[dict[str, Any]] = []
        self.stats = CrawlerStats(title=self._title())
        self.started_at: datetime | None = None

    @classmethod
    def from_config(
        cls, path: str | Path, *, configure_logging: bool = True
    ) -> AdvancedCrawler:
        """Создаёт краулер по файлу YAML или JSON.

        Логирование настраивается здесь же: `from_config` — точка входа
        приложения, и уровень с файлом логов заданы в том же конфиге. Прямой
        конструктор корневой логгер не трогает: библиотеке это не положено.
        """
        config = CrawlerConfig.from_file(path)
        if configure_logging:
            setup_logging(
                config.log_level,
                config.log_file,
                max_bytes=config.log_max_bytes,
                backups=config.log_backups,
            )
        return cls(config)

    async def crawl(
        self,
        start_urls: Iterable[str] | None = None,
        *,
        on_progress: Callable[[dict[str, Any]], None] | None = None,
        progress_interval: float | None = None,
    ) -> list[dict[str, Any]]:
        """Обходит сайт и собирает статистику прогона.

        Адреса берутся из аргумента, из карты сайта или из конфигурации — в
        этом порядке. Буфер хранилища досылается сразу после обхода: иначе
        статистика показывала бы «записей 0», пока последние страницы лежат в
        памяти.
        """
        urls = list(start_urls) if start_urls else await self.resolve_start_urls()
        if not urls:
            raise ValueError("нет стартовых URL: ни в конфигурации, ни в карте сайта")

        self.started_at = datetime.now()
        interval = (
            self.config.progress_interval if progress_interval is None else progress_interval
        )
        self.pages = await self.crawler.crawl(
            urls,
            on_progress=on_progress,
            progress_interval=interval,
            **self.config.to_crawl_kwargs(),
        )
        if self.storage is not None:
            await self.storage.flush()

        self.stats = CrawlerStats.from_crawler(
            self.crawler,
            self.pages,
            started_at=self.started_at,
            title=self._title(),
        )
        self.stats.settings = self.config.to_dict()
        return self.pages

    async def resolve_start_urls(self) -> list[str]:
        """Стартовые адреса: из карты сайта, если её попросили, иначе из конфига."""
        if not self.config.sitemaps:
            return list(self.config.start_urls)

        urls = await self.crawler.seeds_from_sitemaps(
            self.config.sitemaps,
            base_urls=self.config.start_urls,
            limit=self.config.max_pages,
        )
        if urls:
            logger.info("Из карт сайта получено стартовых URL: %d", len(urls))
            return urls

        # Карта могла не найтись или оказаться пустой — это не повод
        # заканчивать работу, если адреса есть в конфигурации.
        logger.warning("Карты сайта не дали адресов, беру start_urls из конфигурации")
        return list(self.config.start_urls)

    def get_stats(self) -> dict[str, Any]:
        """Сводка прогона словарём: `total_pages`, `successful`, `failed` и прочее."""
        return self.stats.get_stats()

    def export_to_json(self, filename: str | Path | None = None) -> Path:
        """Сохраняет статистику в JSON (по умолчанию — путь из конфигурации)."""
        return self.stats.export_to_json(self._target(filename, self.config.report_json))

    def export_to_html_report(self, filename: str | Path | None = None) -> Path:
        """Сохраняет HTML-отчёт (по умолчанию — путь из конфигурации)."""
        return self.stats.export_to_html_report(
            self._target(filename, self.config.report_html)
        )

    def export_reports(self) -> list[Path]:
        """Пишет те отчёты, пути к которым заданы в конфигурации."""
        written: list[Path] = []
        if self.config.report_json:
            written.append(self.export_to_json())
        if self.config.report_html:
            written.append(self.export_to_html_report())
        return written

    async def close(self) -> None:
        """Закрывает краулер, а вместе с ним хранилище и сессию."""
        await self.crawler.close()

    async def __aenter__(self) -> AdvancedCrawler:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.close()

    # --- внутреннее ---------------------------------------------------------

    def _title(self) -> str:
        first = self.config.start_urls[0] if self.config.start_urls else ""
        source = first or (self.config.sitemaps[0] if self.config.sitemaps else "")
        return f"Отчёт краулера: {source}" if source else "Отчёт краулера"

    @staticmethod
    def _target(filename: str | Path | None, fallback: str | None) -> Path:
        target = filename or fallback
        if not target:
            raise ValueError(
                "не указан файл отчёта: передайте путь или задайте его в конфигурации"
            )
        return Path(target)
