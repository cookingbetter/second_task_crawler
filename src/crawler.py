"""Базовый асинхронный HTTP-клиент на aiohttp."""

from __future__ import annotations

import asyncio
import logging
import ssl
import time
from types import TracebackType
from typing import Any

import aiohttp
import certifi

from src.models import (
    ERROR_HTTP,
    ERROR_NETWORK,
    ERROR_TIMEOUT,
    ERROR_UNKNOWN,
    CrawlStats,
    FetchResult,
)
from src.parser import HTMLParser, empty_page

logger = logging.getLogger(__name__)

CHUNK_SIZE = 64 * 1024


def _default_ssl_context() -> ssl.SSLContext:
    """SSL-контекст на корневых сертификатах certifi.

    Системное хранилище CA на части машин (особенно в свежем venv на macOS)
    недоступно, и тогда любой https-запрос падает с CERTIFICATE_VERIFY_FAILED.
    """
    return ssl.create_default_context(cafile=certifi.where())


class AsyncCrawler:
    """Загружает страницы параллельно, ограничивая конкурентность семафором.

    Сессия создаётся лениво, при первом запросе: так объект можно создать вне
    работающего event loop, а connector привяжется к тому циклу, в котором
    реально выполняются запросы.
    """

    def __init__(
        self,
        max_concurrent: int = 10,
        *,
        timeout_total: float = 15.0,
        timeout_connect: float = 5.0,
        timeout_read: float = 10.0,
        max_body_bytes: int = 5_000_000,
        user_agent: str = "AsyncCrawler/1.0 (+https://example.com/bot)",
        ssl_context: ssl.SSLContext | None = None,
        parser: HTMLParser | None = None,
    ) -> None:
        if max_concurrent < 1:
            raise ValueError("max_concurrent должен быть >= 1")

        self.max_concurrent = max_concurrent
        self.max_body_bytes = max_body_bytes
        self.user_agent = user_agent
        self.parser = parser or HTMLParser()
        self._ssl_context = ssl_context or _default_ssl_context()
        self._timeout = aiohttp.ClientTimeout(
            total=timeout_total,
            connect=timeout_connect,
            sock_read=timeout_read,
        )
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._session: aiohttp.ClientSession | None = None
        self._closed = False
        self.results: list[FetchResult] = []
        self.stats = CrawlStats()

    async def __aenter__(self) -> AsyncCrawler:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.close()

    @property
    def closed(self) -> bool:
        return self._closed

    def _get_session(self) -> aiohttp.ClientSession:
        """Возвращает сессию с пулом соединений, создавая её при первом вызове."""
        if self._closed:
            raise RuntimeError("Краулер уже закрыт, создайте новый экземпляр")

        if self._session is None or self._session.closed:
            connector = aiohttp.TCPConnector(
                limit=self.max_concurrent * 2,
                limit_per_host=self.max_concurrent,
                ttl_dns_cache=300,
                ssl=self._ssl_context,
            )
            self._session = aiohttp.ClientSession(
                connector=connector,
                timeout=self._timeout,
                headers={"User-Agent": self.user_agent},
            )
            logger.debug(
                "Создана сессия: limit=%d, limit_per_host=%d",
                self.max_concurrent * 2,
                self.max_concurrent,
            )

        return self._session

    async def fetch_one(self, url: str) -> FetchResult:
        """Загружает один URL и всегда возвращает результат, не поднимая исключений."""
        session = self._get_session()

        async with self._semaphore:
            # Время измеряется после семафора: ожидание в очереди — не время запроса.
            started = time.perf_counter()
            logger.info("Загрузка начата: %s", url)
            try:
                async with session.get(url, raise_for_status=True) as response:
                    body, truncated = await self._read_body(response, url)
                    text = self._decode_body(body, response, url)
                    elapsed = time.perf_counter() - started
                    logger.info(
                        "Загрузка завершена: %s (status=%d, %d байт, %.2f с)",
                        url,
                        response.status,
                        len(body),
                        elapsed,
                    )
                    return FetchResult(
                        url=url,
                        status=response.status,
                        text=text,
                        elapsed=elapsed,
                        truncated=truncated,
                        final_url=str(response.url),
                    )
            except asyncio.CancelledError:
                raise
            except asyncio.TimeoutError:
                return self._failure(url, started, ERROR_TIMEOUT, "превышен таймаут")
            except aiohttp.ClientResponseError as exc:
                return self._failure(
                    url,
                    started,
                    ERROR_HTTP,
                    f"HTTP {exc.status}: {exc.message}",
                    status=exc.status,
                )
            except aiohttp.ClientError as exc:
                return self._failure(
                    url, started, ERROR_NETWORK, f"{type(exc).__name__}: {exc}"
                )
            except Exception as exc:  # защита от падения всего запуска
                return self._failure(
                    url, started, ERROR_UNKNOWN, f"{type(exc).__name__}: {exc}"
                )

    async def _read_body(
        self, response: aiohttp.ClientResponse, url: str
    ) -> tuple[bytes, bool]:
        """Читает тело чанками до `max_body_bytes`.

        `response.content.read(n)` здесь не подходит: он отдаёт только то, что уже
        лежит в буфере, поэтому ответ из нескольких TCP-чанков молча обрезался бы.
        """
        chunks: list[bytes] = []
        size = 0
        truncated = False

        async for chunk in response.content.iter_chunked(CHUNK_SIZE):
            chunks.append(chunk)
            size += len(chunk)
            if size >= self.max_body_bytes:
                truncated = True
                break

        body = b"".join(chunks)[: self.max_body_bytes]
        if truncated:
            logger.warning(
                "Тело ответа обрезано до %d байт: %s", self.max_body_bytes, url
            )
        return body, truncated

    def _decode_body(
        self, body: bytes, response: aiohttp.ClientResponse, url: str
    ) -> str:
        """Декодирует тело по charset из заголовка.

        `response.get_encoding()` для этого не годится: без charset в Content-Type
        он требует уже прочитанного `response._body` и бросает RuntimeError.
        """
        encoding = response.charset or "utf-8"
        try:
            return body.decode(encoding, errors="replace")
        except LookupError:
            logger.debug(
                "Неизвестная кодировка %r, декодирую как utf-8: %s", encoding, url
            )
            return body.decode("utf-8", errors="replace")

    def _failure(
        self,
        url: str,
        started: float,
        error_type: str,
        message: str,
        *,
        status: int | None = None,
    ) -> FetchResult:
        elapsed = time.perf_counter() - started
        logger.warning("Ошибка загрузки %s [%s]: %s", url, error_type, message)
        return FetchResult(
            url=url,
            status=status,
            elapsed=elapsed,
            error=message,
            error_type=error_type,
        )

    async def fetch_url(self, url: str) -> str:
        """Загружает страницу и возвращает её текст, поднимая исключение при ошибке."""
        result = await self.fetch_one(url)
        if not result.ok:
            raise FetchError(result)
        return result.text or ""

    def _unique(self, urls: list[str]) -> list[str]:
        """Убирает повторы, сохраняя порядок: один URL незачем грузить дважды."""
        unique = list(dict.fromkeys(urls))
        if len(unique) != len(urls):
            logger.debug(
                "Пропущено %d повторяющихся URL из %d", len(urls) - len(unique), len(urls)
            )
        return unique

    def _unexpected(self, url: str, exc: BaseException) -> FetchResult:
        """Оборачивает исключение, просочившееся из `gather`, в результат."""
        logger.warning(
            "Непредвиденное исключение для %s: %s", url, exc, exc_info=exc
        )
        return FetchResult(
            url=url,
            error=f"{type(exc).__name__}: {exc}",
            error_type=ERROR_UNKNOWN,
        )

    async def fetch_urls(self, urls: list[str]) -> dict[str, str]:
        """Загружает список URL параллельно; в словарь попадают только успешные."""
        urls = self._unique(urls)
        started = time.perf_counter()
        raw = await asyncio.gather(
            *(self.fetch_one(url) for url in urls),
            return_exceptions=True,
        )
        elapsed = time.perf_counter() - started

        results: list[FetchResult] = []
        for url, item in zip(urls, raw, strict=True):
            if isinstance(item, FetchResult):
                results.append(item)
            elif isinstance(item, BaseException):
                results.append(self._unexpected(url, item))

        self.results = results
        self.stats = CrawlStats.from_results(results, elapsed)
        logger.info(
            "Параллельная загрузка: %d/%d успешно за %.2f с",
            self.stats.succeeded,
            self.stats.total,
            elapsed,
        )
        return {result.url: result.text or "" for result in results if result.ok}

    async def fetch_urls_sequential(self, urls: list[str]) -> list[FetchResult]:
        """Последовательная загрузка — нужна только для сравнения в демо и тестах."""
        urls = self._unique(urls)
        started = time.perf_counter()
        results = [await self.fetch_one(url) for url in urls]
        elapsed = time.perf_counter() - started

        self.results = results
        self.stats = CrawlStats.from_results(results, elapsed)
        logger.info(
            "Последовательная загрузка: %d/%d успешно за %.2f с",
            self.stats.succeeded,
            self.stats.total,
            elapsed,
        )
        return results

    async def fetch_and_parse(self, url: str) -> dict[str, Any]:
        """Загружает страницу и сразу разбирает её HTML.

        Возвращает словарь одной и той же формы и при успехе, и при ошибке
        загрузки: вызывающему коду не нужно разбираться с двумя типами ответа.
        """
        _result, page = await self._fetch_and_parse(url)
        return page

    async def _fetch_and_parse(self, url: str) -> tuple[FetchResult, dict[str, Any]]:
        """Возвращает и `FetchResult` (для статистики), и разобранную страницу."""
        result = await self.fetch_one(url)

        if result.ok and result.text is not None:
            # База для относительных ссылок — URL после редиректов.
            page = await self.parser.parse_html(result.text, result.final_url or url)
        else:
            page = empty_page(url)

        return result, self._merge_result(page, result)

    @staticmethod
    def _merge_result(page: dict[str, Any], result: FetchResult) -> dict[str, Any]:
        """Дополняет разобранную страницу данными о самой загрузке."""
        page.update(
            url=result.url,
            final_url=result.final_url,
            status=result.status,
            elapsed=result.elapsed,
            truncated=result.truncated,
            ok=result.ok,
            error=result.error,
            error_type=result.error_type,
        )
        return page

    async def fetch_and_parse_many(self, urls: list[str]) -> list[dict[str, Any]]:
        """Параллельно загружает и разбирает список URL, обновляя статистику."""
        urls = self._unique(urls)
        started = time.perf_counter()
        raw = await asyncio.gather(
            *(self._fetch_and_parse(url) for url in urls),
            return_exceptions=True,
        )
        elapsed = time.perf_counter() - started

        results: list[FetchResult] = []
        pages: list[dict[str, Any]] = []
        for url, item in zip(urls, raw, strict=True):
            if isinstance(item, BaseException):
                result = self._unexpected(url, item)
                page = self._merge_result(empty_page(url), result)
            else:
                result, page = item
            results.append(result)
            pages.append(page)

        self.results = results
        self.stats = CrawlStats.from_results(results, elapsed)
        logger.info(
            "Загрузка с парсингом: %d/%d успешно за %.2f с",
            self.stats.succeeded,
            self.stats.total,
            elapsed,
        )
        return pages

    async def close(self) -> None:
        """Закрывает сессию; повторный вызов безопасен."""
        self._closed = True
        if self._session is not None and not self._session.closed:
            await self._session.close()
            # Даём коннектору завершить закрытие транспортов до выхода из loop.
            await asyncio.sleep(0)
            logger.debug("Сессия закрыта")
        self._session = None


class FetchError(RuntimeError):
    """Ошибка загрузки, поднимаемая `fetch_url` и несущая исходный `FetchResult`."""

    def __init__(self, result: FetchResult) -> None:
        super().__init__(f"{result.url}: [{result.error_type}] {result.error}")
        self.result = result
