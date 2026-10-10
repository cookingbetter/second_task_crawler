"""Базовый асинхронный HTTP-клиент на aiohttp."""

from __future__ import annotations

import asyncio
import itertools
import logging
import ssl
import time
from collections import Counter
from collections.abc import Callable, Iterable
from types import TracebackType
from typing import Any

import aiohttp
import certifi

from src.filters import UrlFilter, normalize_url
from src.models import (
    ERROR_HTTP,
    ERROR_NETWORK,
    ERROR_ROBOTS,
    ERROR_TIMEOUT,
    ERROR_UNKNOWN,
    CrawlStats,
    FetchResult,
)
from src.parser import HTMLParser, empty_page
from src.queue import CrawlerQueue
from src.ratelimit import RateLimiter
from src.robots import RobotsParser, RobotsResponse
from src.semaphore import SemaphoreManager

logger = logging.getLogger(__name__)

CHUNK_SIZE = 64 * 1024
DEFAULT_USER_AGENT = "AsyncCrawler/1.0 (+https://example.com/bot)"
# robots.txt больше этого размера — уже не правила обхода, а чья-то ошибка.
ROBOTS_MAX_BYTES = 500_000
# Коды, означающие «сайту сейчас тяжело»: на них задержка домена растёт.
OVERLOAD_STATUSES = frozenset({408, 429})

SKIP_DEPTH = "depth"
SKIP_FILTERED = "filtered"
SKIP_DUPLICATE = "duplicate"
SKIP_CLOSED = "closed"
SKIP_ROBOTS = "robots"


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
        user_agent: str = DEFAULT_USER_AGENT,
        user_agents: Iterable[str] | None = None,
        requests_per_second: float | None = None,
        per_domain_rate: bool = True,
        min_delay: float = 0.0,
        jitter: float = 0.0,
        backoff_factor: float = 2.0,
        max_backoff: float = 60.0,
        respect_robots: bool = False,
        robots_allow_on_error: bool = True,
        rate_limiter: RateLimiter | None = None,
        robots: RobotsParser | None = None,
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

        # Вежливость (день 4). По умолчанию выключена: лимит скорости и
        # robots.txt включаются явно, иначе простая загрузка списка URL
        # неожиданно стала бы медленной.
        if rate_limiter is not None and (requests_per_second or min_delay or jitter):
            logger.warning(
                "Передан готовый rate_limiter: настройки requests_per_second, "
                "min_delay, jitter и backoff не применяются"
            )
        if robots is not None and not robots_allow_on_error:
            logger.warning(
                "Передан готовый RobotsParser: robots_allow_on_error не применяется"
            )
        self.rate_limiter = rate_limiter or RateLimiter(
            requests_per_second,
            per_domain_rate,
            min_delay=min_delay,
            jitter=jitter,
            backoff_factor=backoff_factor,
            max_backoff=max_backoff,
        )
        self.respect_robots = respect_robots
        self.robots = robots or RobotsParser(
            self._fetch_robots_txt,
            user_agent=user_agent,
            allow_on_error=robots_allow_on_error,
        )
        # Ротация агентов: пустой список означает «всегда основной агент».
        self.user_agents = tuple(user_agents or ())
        self._ua_cycle = itertools.cycle(self.user_agents) if self.user_agents else None
        # URL -> правило robots.txt, из-за которого его не стали загружать.
        self.blocked_urls: dict[str, str] = {}

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

    async def fetch_one(
        self,
        url: str,
        *,
        user_agent: str | None = None,
        robots_checked: bool = False,
    ) -> FetchResult:
        """Загружает один URL и всегда возвращает результат, не поднимая исключений.

        Порядок проверок: robots.txt -> лимит скорости -> семафоры -> запрос.
        Лимитер стоит перед семафорами намеренно: ожидающая задача не должна
        держать слот конкурентности и мешать запросам к другим доменам.

        `user_agent` переопределяет агента для этого запроса (иначе берётся
        следующий по ротации), а `robots_checked` говорит, что вызывающий уже
        спросил разрешения — чтобы одна страница не считалась в статистике
        robots.txt дважды.
        """
        session = self._get_session()
        agent = user_agent or self._next_user_agent()

        if self.respect_robots:
            if not robots_checked:
                allowed, rule = await self.robots.allows(url, agent)
                if not allowed:
                    return self._blocked(url, rule, agent)
            # Правила домена уже в кэше, осталось учесть Crawl-delay из них.
            self._apply_crawl_delay(url, agent)

        waited = await self.rate_limiter.acquire(url)

        async with self.semaphores.acquire(url):
            # Время измеряется после семафора: ожидание в очереди — не время запроса.
            started = time.perf_counter()
            logger.info("Загрузка начата: %s", url)
            try:
                async with session.get(
                    url, raise_for_status=True, headers={"User-Agent": agent}
                ) as response:
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
                    # Сайт отвечает — можно постепенно вернуться к обычному темпу.
                    self.rate_limiter.reward(url)
                    return FetchResult(
                        url=url,
                        status=response.status,
                        text=text,
                        elapsed=elapsed,
                        truncated=truncated,
                        final_url=str(response.url),
                        waited=waited,
                        user_agent=agent,
                    )
            except asyncio.CancelledError:
                raise
            except asyncio.TimeoutError:
                return self._failure(
                    url,
                    started,
                    ERROR_TIMEOUT,
                    "превышен таймаут",
                    waited=waited,
                    user_agent=agent,
                )
            except aiohttp.ClientResponseError as exc:
                return self._failure(
                    url,
                    started,
                    ERROR_HTTP,
                    f"HTTP {exc.status}: {exc.message}",
                    status=exc.status,
                    waited=waited,
                    user_agent=agent,
                )
            except aiohttp.ClientError as exc:
                return self._failure(
                    url,
                    started,
                    ERROR_NETWORK,
                    f"{type(exc).__name__}: {exc}",
                    waited=waited,
                    user_agent=agent,
                )
            except Exception as exc:  # защита от падения всего запуска
                return self._failure(
                    url,
                    started,
                    ERROR_UNKNOWN,
                    f"{type(exc).__name__}: {exc}",
                    waited=waited,
                    user_agent=agent,
                )

    def _next_user_agent(self) -> str:
        """Следующий агент по кругу или основной, если ротация не настроена."""
        return self.user_agent if self._ua_cycle is None else next(self._ua_cycle)

    def _apply_crawl_delay(self, url: str, user_agent: str) -> None:
        """Переносит `Crawl-delay` из robots.txt в лимитер скорости."""
        delay = self.robots.get_crawl_delay(user_agent, url=url)
        if delay:
            self.rate_limiter.set_crawl_delay(url, delay)

    def _blocked(self, url: str, rule: str | None, user_agent: str) -> FetchResult:
        """Результат для URL, запрещённого robots.txt: запроса в сеть не было."""
        reason = "robots.txt запрещает загрузку"
        if rule:
            reason = f"{reason} ({rule})"
        return FetchResult(
            url=url, error=reason, error_type=ERROR_ROBOTS, user_agent=user_agent
        )

    async def _fetch_robots_txt(self, robots_url: str) -> RobotsResponse:
        """Загружает robots.txt для `RobotsParser`.

        Запрос идёт мимо проверки robots.txt (иначе получилась бы рекурсия), но
        через лимит скорости и семафоры: сам robots.txt — тоже запрос к сайту.
        """
        session = self._get_session()
        await self.rate_limiter.acquire(robots_url)

        async with self.semaphores.acquire(robots_url):
            try:
                async with session.get(
                    robots_url, headers={"User-Agent": self.user_agent}
                ) as response:
                    if response.status >= 400:
                        return RobotsResponse(status=response.status)

                    chunks: list[bytes] = []
                    size = 0
                    async for chunk in response.content.iter_chunked(CHUNK_SIZE):
                        chunks.append(chunk)
                        size += len(chunk)
                        if size >= ROBOTS_MAX_BYTES:
                            logger.warning("robots.txt обрезан: %s", robots_url)
                            break
                    body = b"".join(chunks)[:ROBOTS_MAX_BYTES]
                    return RobotsResponse(
                        status=response.status,
                        text=self._decode_body(body, response, robots_url),
                    )
            except asyncio.CancelledError:
                raise
            except asyncio.TimeoutError:
                return RobotsResponse(error="превышен таймаут")
            except aiohttp.ClientError as exc:
                return RobotsResponse(error=f"{type(exc).__name__}: {exc}")

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
        waited: float = 0.0,
        user_agent: str | None = None,
    ) -> FetchResult:
        elapsed = time.perf_counter() - started
        logger.warning("Ошибка загрузки %s [%s]: %s", url, error_type, message)
        if self._overloaded(error_type, status):
            self.rate_limiter.penalize(url)
        return FetchResult(
            url=url,
            status=status,
            elapsed=elapsed,
            error=message,
            error_type=error_type,
            waited=waited,
            user_agent=user_agent,
        )

    @staticmethod
    def _overloaded(error_type: str, status: int | None) -> bool:
        """Похожа ли ошибка на перегрузку сайта, а не на нашу собственную.

        Замедляться стоит от таймаутов, сетевых сбоев, 5xx и явных «слишком
        много запросов». Обычный 404 к скорости отношения не имеет: из-за битых
        ссылок обход тормозил бы без причины.
        """
        if error_type in (ERROR_TIMEOUT, ERROR_NETWORK, ERROR_UNKNOWN):
            return True
        return status is not None and (status >= 500 or status in OVERLOAD_STATUSES)

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

    async def _fetch_and_parse(
        self,
        url: str,
        *,
        user_agent: str | None = None,
        robots_checked: bool = False,
    ) -> tuple[FetchResult, dict[str, Any]]:
        """Возвращает и `FetchResult` (для статистики), и разобранную страницу."""
        result = await self.fetch_one(
            url, user_agent=user_agent, robots_checked=robots_checked
        )

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
            waited=result.waited,
            user_agent=result.user_agent,
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
        self.blocked_urls = {}
        self.skipped = Counter()
        self._final_urls = set()
        self.results = []
        self._pages_started = 0
        # Семафоры, лимитер и парсер robots.txt переиспользуются между обходами,
        # но их счётчики — нет: иначе второй обход показывал бы статистику
        # первого. Разобранные правила и шаг задержек при этом сохраняются.
        self.semaphores.reset_stats()
        self.rate_limiter.reset_stats()
        self.robots.reset_stats()
        pages: list[dict[str, Any]] = []

        for url in seeds:
            # Стартовые URL не проходят фильтр по домену: именно они его и задают.
            self.queue.add_url(url, priority=0, depth=0)

        logger.info(
            "Обход начат: %d стартовых URL, max_pages=%d, max_depth=%d, "
            "воркеров %d, лимит %s зап/с, robots.txt %s",
            len(self.queue.seen),
            max_pages,
            depth_limit,
            self.max_concurrent,
            self.rate_limiter.requests_per_second or "без лимита",
            "учитывается" if self.respect_robots else "игнорируется",
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

            # Весь путь после get_next идёт под try/finally: на любом выходе
            # взятый URL нужно отпустить, иначе in_progress останется
            # завышенным и остальные воркеры будут ждать работу, которой нет.
            marked = False
            try:
                if url in self._final_urls:
                    # Эту страницу уже загружали под другим адресом: о редиректе
                    # становится известно только после загрузки, и ссылка на цель
                    # могла попасть в очередь раньше.
                    logger.debug("URL ведёт на уже загруженную страницу: %s", url)
                    self.skipped[SKIP_DUPLICATE] += 1
                    continue

                if self._pages_started >= max_pages:
                    # Бюджет исчерпан — спрашивать robots.txt не о чем: иначе
                    # URL, который никто не собирался грузить, попал бы в
                    # статистику проверок и, возможно, в blocked_urls.
                    logger.info(
                        "Достигнут лимит %d страниц, обход останавливается", max_pages
                    )
                    queue.close()
                    return

                user_agent = self._next_user_agent()
                if self.respect_robots:
                    # Проверка до расхода бюджета: запрещённый URL не должен
                    # тратить лимит страниц и попадать в неудачные загрузки.
                    allowed, rule = await self.robots.allows(url, user_agent)
                    if not allowed:
                        self.skipped[SKIP_ROBOTS] += 1
                        self.blocked_urls[url] = rule or "robots.txt"
                        continue

                    if self._pages_started >= max_pages:
                        # Пока спрашивали разрешение, бюджет могли израсходовать
                        # другие воркеры.
                        queue.close()
                        return

                # Проверка и инкремент без await между ними, поэтому лимит
                # страниц соблюдается точно, без перебора на число воркеров.
                self._pages_started += 1

                depth = queue.depth_of(url)
                try:
                    result, page = await self._fetch_and_parse(
                        url, user_agent=user_agent, robots_checked=self.respect_robots
                    )
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
        """Сводная статистика обхода: очередь, семафоры, вежливость, пропуски."""
        stats = self.queue.get_stats()
        stats.update(
            visited=len(self.visited_urls),
            pages_started=self._pages_started,
            skipped=dict(self.skipped),
            skipped_total=sum(self.skipped.values()),
            semaphores=self.semaphores.get_stats(),
            rate_limit=self.rate_limiter.get_stats(),
            robots={**self.robots.get_stats(), "enabled": self.respect_robots},
            blocked=len(self.blocked_urls),
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
