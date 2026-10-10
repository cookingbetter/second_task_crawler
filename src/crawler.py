"""Базовый асинхронный HTTP-клиент на aiohttp."""

from __future__ import annotations

import asyncio
import logging
import ssl
import time
from collections import Counter
from collections.abc import Callable
from types import TracebackType
from typing import Any

import aiohttp
import certifi

from src.filters import UrlFilter, normalize_url
from src.models import (
    ERROR_HTTP,
    ERROR_NETWORK,
    ERROR_TIMEOUT,
    ERROR_UNKNOWN,
    CrawlStats,
    FetchResult,
)
from src.parser import HTMLParser, empty_page
from src.queue import CrawlerQueue
from src.semaphore import SemaphoreManager

logger = logging.getLogger(__name__)

CHUNK_SIZE = 64 * 1024

SKIP_DEPTH = "depth"
SKIP_FILTERED = "filtered"
SKIP_DUPLICATE = "duplicate"
SKIP_CLOSED = "closed"


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
        max_depth: int = 2,
        max_per_domain: int | None = None,
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
        if max_depth < 0:
            raise ValueError("max_depth должен быть >= 0")

        self.max_concurrent = max_concurrent
        self.max_depth = max_depth
        self.max_body_bytes = max_body_bytes
        self.user_agent = user_agent
        self.parser = parser or HTMLParser()
        self._ssl_context = ssl_context or _default_ssl_context()
        self._timeout = aiohttp.ClientTimeout(
            total=timeout_total,
            connect=timeout_connect,
            sock_read=timeout_read,
        )
        self.semaphores = SemaphoreManager(max_concurrent, max_per_domain)
        self._session: aiohttp.ClientSession | None = None
        self._closed = False
        self.results: list[FetchResult] = []
        self.stats = CrawlStats()

        # Состояние обхода (день 3). `visited_urls` — URL, которые действительно
        # забрали из очереди и загрузили; за дедупликацию отвечает очередь,
        # её `seen` — надмножество этого набора.
        self.queue = CrawlerQueue()
        self.visited_urls: set[str] = set()
        self.failed_urls: dict[str, str] = {}
        self.processed_urls: dict[str, dict[str, Any]] = {}
        self.skipped: Counter[str] = Counter()
        # Адреса, с которых страницы реально отдали (после редиректов).
        self._final_urls: set[str] = set()
        self._pages_started = 0

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
                # Пул соединений не должен быть уже лимита семафора на домен,
                # иначе он, а не семафор, определял бы конкурентность.
                limit_per_host=self.semaphores.max_per_domain,
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
                self.semaphores.max_per_domain,
            )

        return self._session

    async def fetch_one(self, url: str) -> FetchResult:
        """Загружает один URL и всегда возвращает результат, не поднимая исключений."""
        session = self._get_session()

        async with self.semaphores.acquire(url):
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

    async def crawl(
        self,
        start_urls: list[str],
        max_pages: int = 100,
        *,
        max_depth: int | None = None,
        same_domain_only: bool = False,
        include_patterns: list[str] | None = None,
        exclude_patterns: list[str] | None = None,
        on_progress: Callable[[dict[str, Any]], None] | None = None,
        progress_interval: float = 0.5,
    ) -> list[dict[str, Any]]:
        """Обходит сайт от стартовых URL, добавляя найденные ссылки в очередь.

        Работу делает пул из `max_concurrent` воркеров: каждый берёт URL из
        очереди, загружает, разбирает и ставит найденные ссылки обратно. Такая
        схема, в отличие от обхода «волнами по уровням», не оставляет воркеры
        простаивать в конце уровня.

        Возвращает список страниц в порядке завершения загрузки — включая те,
        что не загрузились (у них `ok=False`).
        """
        if max_pages < 1:
            raise ValueError("max_pages должен быть >= 1")

        if self._closed:
            raise RuntimeError("Краулер уже закрыт, создайте новый экземпляр")

        depth_limit = self.max_depth if max_depth is None else max_depth
        # Нормализация до фильтра: иначе `https://Example.com` и
        # `https://example.com/` задали бы разные «разрешённые» домены.
        seeds = [normalize_url(url) for url in self._unique(start_urls)]
        url_filter = UrlFilter.build(
            seeds,
            same_domain_only=same_domain_only,
            include_patterns=include_patterns,
            exclude_patterns=exclude_patterns,
        )

        self.queue = CrawlerQueue()
        self.visited_urls = set()
        self.failed_urls = {}
        self.processed_urls = {}
        self.skipped = Counter()
        self._final_urls = set()
        self.results = []
        self._pages_started = 0
        # Семафоры переиспользуются между обходами, но их пики — нет: иначе
        # второй обход показывал бы статистику первого.
        self.semaphores.reset_stats()
        pages: list[dict[str, Any]] = []

        for url in seeds:
            # Стартовые URL не проходят фильтр по домену: именно они его и задают.
            self.queue.add_url(url, priority=0, depth=0)

        logger.info(
            "Обход начат: %d стартовых URL, max_pages=%d, max_depth=%d, "
            "воркеров %d",
            len(self.queue.seen),
            max_pages,
            depth_limit,
            self.max_concurrent,
        )

        started = time.perf_counter()
        if on_progress is not None:
            # Стартовая строка сразу: на быстром сайте обход может закончиться
            # раньше первого тика, и прогресс не показался бы вовсе.
            self._report_progress(on_progress)
        ticker = self._start_progress_ticker(on_progress, progress_interval)
        workers = [
            asyncio.create_task(
                self._crawl_worker(pages, url_filter, max_pages, depth_limit, number),
                name=f"crawl-worker-{number}",
            )
            for number in range(self.max_concurrent)
        ]
        try:
            await asyncio.gather(*workers)
        except BaseException:
            # gather не отменяет соседей сам, а оставлять воркеры работать в
            # фоне после выхода из crawl нельзя.
            self.queue.close()
            for worker in workers:
                worker.cancel()
            await asyncio.gather(*workers, return_exceptions=True)
            raise
        finally:
            await self._stop_progress_ticker(ticker, on_progress)

        self.stats = CrawlStats.from_results(
            self.results, time.perf_counter() - started
        )
        logger.info(
            "Обход завершён: страниц %d, ошибок %d, в очереди осталось %d, %.2f с",
            len(self.processed_urls),
            len(self.failed_urls),
            len(self.queue),
            self.stats.elapsed,
        )
        return pages

    async def _crawl_worker(
        self,
        pages: list[dict[str, Any]],
        url_filter: UrlFilter,
        max_pages: int,
        max_depth: int,
        number: int,
    ) -> None:
        """Тянет URL из очереди, пока она не опустеет или не закроется."""
        queue = self.queue
        while True:
            url = await queue.get_next()
            if url is None:
                logger.debug("Воркер %d: работы больше нет", number)
                return

            if url in self._final_urls:
                # Эту страницу уже загружали под другим адресом: о редиректе
                # становится известно только после загрузки, и ссылка на цель
                # могла попасть в очередь раньше.
                logger.debug("URL ведёт на уже загруженную страницу: %s", url)
                self.skipped[SKIP_DUPLICATE] += 1
                queue.mark_skipped(url)
                continue

            # Проверка и инкремент без await между ними, поэтому лимит страниц
            # соблюдается точно, без перебора на число воркеров.
            if self._pages_started >= max_pages:
                logger.info(
                    "Достигнут лимит %d страниц, обход останавливается", max_pages
                )
                queue.mark_skipped(url)
                queue.close()
                return
            self._pages_started += 1

            depth = queue.depth_of(url)
            marked = False
            try:
                try:
                    result, page = await self._fetch_and_parse(url)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:  # падение воркера не должно рвать обход
                    result = self._unexpected(url, exc)
                    page = self._merge_result(empty_page(url), result)

                page["depth"] = depth
                self.visited_urls.add(url)
                self.results.append(result)
                pages.append(page)

                if result.ok:
                    self.processed_urls[url] = page
                    self._remember_redirect(url, result)
                    # Ссылки добавляются до mark_*: иначе последний работающий
                    # воркер на мгновение создал бы «очередь пуста и все
                    # свободны», и остальные вышли бы, не дождавшись новых URL.
                    self._enqueue_links(page, url_filter, depth, max_depth)
                    queue.mark_processed(url)
                else:
                    error = result.error or "неизвестная ошибка"
                    self.failed_urls[url] = error
                    queue.mark_failed(url, error)
                marked = True
            finally:
                if not marked:
                    # Иначе in_progress остался бы завышенным, и остальные
                    # воркеры ждали бы работу, которой больше не будет.
                    queue.mark_skipped(url)

    def _remember_redirect(self, url: str, result: FetchResult) -> None:
        """Запоминает адрес, с которого реально отдали страницу.

        Нужно для редиректов: ссылку на цель не поставят в очередь, а если она
        попала туда раньше — воркер её пропустит. Неизбежным остаётся только
        случай, когда редирект и его цель загружаются одновременно: до ответа
        сервера об этом знать нельзя.
        """
        final = normalize_url(result.final_url or url)
        self._final_urls.add(final)
        if final != url and self.queue.add_alias(final):
            logger.debug("Редирект %s -> %s, цель не будет загружена повторно", url, final)

    def _enqueue_links(
        self,
        page: dict[str, Any],
        url_filter: UrlFilter,
        depth: int,
        max_depth: int,
    ) -> None:
        """Ставит в очередь ссылки страницы, отсекая глубину, фильтр и дубликаты."""
        next_depth = depth + 1
        links: list[str] = page.get("links") or []

        if self.queue.closed:
            # Лимит страниц исчерпан: очередь больше ничего не принимает, и
            # списывать эти ссылки на дубликаты было бы неправдой.
            self.skipped[SKIP_CLOSED] += len(links)
            return

        if next_depth > max_depth:
            if links:
                self.skipped[SKIP_DEPTH] += len(links)
                logger.debug(
                    "Глубина %d исчерпана, %d ссылок не добавлено: %s",
                    max_depth,
                    len(links),
                    page.get("url"),
                )
            return

        added = 0
        for raw_link in links:
            link = normalize_url(raw_link)
            if (reason := url_filter.reject_reason(link)) is not None:
                self.skipped[SKIP_FILTERED] += 1
                logger.debug("Ссылка отклонена (%s): %s", reason, link)
                continue
            # priority = -глубина: мелкие уровни разбираются раньше, обход
            # получается почти BFS, но воркеры не ждут конца уровня.
            if self.queue.add_url(link, priority=-next_depth, depth=next_depth):
                added += 1
            else:
                self.skipped[SKIP_DUPLICATE] += 1

        if added:
            logger.debug(
                "Добавлено %d ссылок с глубины %d: %s", added, next_depth, page.get("url")
            )

    def _start_progress_ticker(
        self,
        on_progress: Callable[[dict[str, Any]], None] | None,
        interval: float,
    ) -> asyncio.Task[None] | None:
        """Запускает задачу, которая периодически отдаёт статистику наружу."""
        if on_progress is None or interval <= 0:
            return None

        async def tick() -> None:
            while True:
                await asyncio.sleep(interval)
                self._report_progress(on_progress)

        return asyncio.create_task(tick(), name="crawl-progress")

    async def _stop_progress_ticker(
        self,
        ticker: asyncio.Task[None] | None,
        on_progress: Callable[[dict[str, Any]], None] | None,
    ) -> None:
        if ticker is not None:
            ticker.cancel()
            # Дождаться отмены, чтобы задача не пережила сам обход.
            await asyncio.gather(ticker, return_exceptions=True)
        if on_progress is not None:
            self._report_progress(on_progress)  # финальные цифры

    def _report_progress(
        self, on_progress: Callable[[dict[str, Any]], None]
    ) -> None:
        """Вызывает callback прогресса, не позволяя ему сорвать обход."""
        try:
            on_progress(self.get_crawl_stats())
        except Exception as exc:
            logger.warning("Callback прогресса упал: %s: %s", type(exc).__name__, exc)

    def get_crawl_stats(self) -> dict[str, Any]:
        """Сводная статистика обхода: очередь, семафоры и причины пропусков."""
        stats = self.queue.get_stats()
        stats.update(
            visited=len(self.visited_urls),
            pages_started=self._pages_started,
            skipped=dict(self.skipped),
            skipped_total=sum(self.skipped.values()),
            semaphores=self.semaphores.get_stats(),
        )
        return stats

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
