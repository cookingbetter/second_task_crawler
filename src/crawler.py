"""Базовый асинхронный HTTP-клиент на aiohttp."""

from __future__ import annotations

import asyncio
import gzip
import io
import itertools
import logging
import ssl
import time
from collections import Counter
from collections.abc import Callable, Iterable
from datetime import datetime
from types import TracebackType
from typing import Any
from urllib.parse import urljoin

import aiohttp
import certifi

from src.circuit import CircuitBreaker
from src.errors import (
    CircuitOpenError,
    CrawlerError,
    NetworkError,
    ParseError,
    PermanentError,
    RobotsBlockedError,
    TransientError,
    from_exception,
)
from src.filters import UrlFilter, normalize_url
from src.models import (
    ERROR_ROBOTS,
    ERROR_UNKNOWN,
    CrawlStats,
    FetchResult,
)
from src.parser import HTMLParser, empty_page
from src.queue import CrawlerQueue
from src.ratelimit import RateLimiter
from src.retry import RetryOutcome, RetryStrategy
from src.robots import RobotsParser, RobotsResponse
from src.sitemap import (
    SITEMAP_AUTO,
    SitemapEntry,
    SitemapParser,
    SitemapResponse,
    sort_by_priority,
)
from src.storage import DataStorage
from src.semaphore import SemaphoreManager

logger = logging.getLogger(__name__)

CHUNK_SIZE = 64 * 1024
DEFAULT_USER_AGENT = "AsyncCrawler/1.0 (+https://example.com/bot)"
# robots.txt больше этого размера — уже не правила обхода, а чья-то ошибка.
ROBOTS_MAX_BYTES = 500_000
# Карта сайта бывает на десятки мегабайт, но у неё есть предел по стандарту —
# 50 МБ в распакованном виде; столько в память краулера мы не берём.
SITEMAP_MAX_BYTES = 10_000_000
# Где карта лежит, если robots.txt о ней не сказал.
DEFAULT_SITEMAP_PATH = "/sitemap.xml"
# Первые байты gzip-архива: по ним видно сжатую карту (sitemap.xml.gz).
GZIP_MAGIC = b"\x1f\x8b"
# Классы ошибок, которые повтор не исправит: такие URL попадают в отчёт
# постоянными.
PERMANENT_CLASSES = frozenset({PermanentError.__name__, ParseError.__name__})

SKIP_DEPTH = "depth"
SKIP_FILTERED = "filtered"
SKIP_DUPLICATE = "duplicate"
SKIP_CLOSED = "closed"
SKIP_ROBOTS = "robots"


def _fallback(value: Any, default: Any) -> Any:
    """Значение аргумента или значение по умолчанию, если его не передавали.

    `None` здесь означает именно «не передавали»: иначе нельзя отличить
    `max_retries=2`, написанное руками, от значения по умолчанию, и
    предупреждение о проигнорированных настройках врало бы.
    """
    return default if value is None else value


def _warn_ignored(what: str, **args: Any) -> None:
    """Сообщает, что скалярные настройки не применились к готовому объекту."""
    ignored = sorted(name for name, value in args.items() if value is not None)
    if ignored:
        logger.warning(
            "Передан готовый %s: настройки %s не применяются",
            what,
            ", ".join(ignored),
        )


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
        # Значения по умолчанию подставляются ниже: None означает «не
        # передавали», и только так видно, какие настройки игнорирует готовая
        # RetryStrategy или CircuitBreaker.
        max_retries: int | None = None,
        retry_base_delay: float | None = None,
        retry_backoff_factor: float | None = None,
        retry_max_delay: float | None = None,
        retry_jitter: float | None = None,
        timeout_growth: float = 1.5,
        timeout_max_total: float = 60.0,
        circuit_breaker: bool = False,
        circuit_threshold: int | None = None,
        circuit_window: float | None = None,
        circuit_recovery: float | None = None,
        storage: DataStorage | None = None,
        storage_save_failed: bool = False,
        close_storage: bool = True,
        retry_strategy: RetryStrategy | None = None,
        breaker: CircuitBreaker | None = None,
        rate_limiter: RateLimiter | None = None,
        robots: RobotsParser | None = None,
        sitemap: SitemapParser | None = None,
        ssl_context: ssl.SSLContext | None = None,
        parser: HTMLParser | None = None,
    ) -> None:
        if max_concurrent < 1:
            raise ValueError("max_concurrent должен быть >= 1")
        if max_depth < 0:
            raise ValueError("max_depth должен быть >= 0")
        if timeout_growth < 1:
            raise ValueError("timeout_growth должен быть >= 1")

        self.max_concurrent = max_concurrent
        self.max_depth = max_depth
        self.max_body_bytes = max_body_bytes
        self.user_agent = user_agent
        self.parser = parser or HTMLParser()
        self._ssl_context = ssl_context or _default_ssl_context()
        self.timeout_total = timeout_total
        self.timeout_connect = timeout_connect
        self.timeout_read = timeout_read
        self.timeout_growth = timeout_growth
        self.timeout_max_total = timeout_max_total
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
        # Карты сайта (день 7). Сам по себе парсер ничего не загружает: он
        # работает только когда стартовые URL просят взять из sitemap.
        self.sitemap = sitemap or SitemapParser(self._fetch_sitemap)

        # Повторы (день 5). Включены по умолчанию и умеренно: временная ошибка
        # не должна стоить страницы, но и долбить сайт краулер не станет.
        if retry_strategy is not None:
            _warn_ignored(
                "RetryStrategy",
                max_retries=max_retries,
                retry_base_delay=retry_base_delay,
                retry_backoff_factor=retry_backoff_factor,
                retry_max_delay=retry_max_delay,
                retry_jitter=retry_jitter,
            )
        self.retry_strategy = retry_strategy or RetryStrategy(
            _fallback(max_retries, 2),
            _fallback(retry_backoff_factor, 2.0),
            base_delay=_fallback(retry_base_delay, 0.5),
            max_delay=_fallback(retry_max_delay, 30.0),
            jitter=_fallback(retry_jitter, 0.0),
        )
        # Circuit breaker по умолчанию выключен: он отказывает в загрузке без
        # обращения в сеть, и такое решение краулер не должен принимать молча.
        # Готовый объект включает его сам — передавать его «на всякий случай»
        # никто не будет.
        if breaker is not None:
            _warn_ignored(
                "CircuitBreaker",
                circuit_threshold=circuit_threshold,
                circuit_window=circuit_window,
                circuit_recovery=circuit_recovery,
            )
        self.circuit: CircuitBreaker | None = breaker
        if self.circuit is None and circuit_breaker:
            self.circuit = CircuitBreaker(
                _fallback(circuit_threshold, 5),
                _fallback(circuit_window, 60.0),
                _fallback(circuit_recovery, 30.0),
            )
        # Сохранение (день 6). Хранилища нет по умолчанию: писать файлы — не то
        # решение, которое краулер принимает за пользователя.
        self.storage = storage
        # Неудачные страницы в данные не идут: сохранять нечего, а причины
        # сбоев и так лежат в отчёте об ошибках дня 5.
        self.storage_save_failed = storage_save_failed
        # Хранилище, переданное снаружи, обычно и закрывать нужно вместе с
        # краулером: пример из задания заканчивается одним `crawler.close()`.
        self.close_storage = close_storage

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
        self._max_pages = 0

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

        Порядок проверок: robots.txt -> повторы -> circuit breaker -> лимит
        скорости -> семафоры -> запрос. Лимитер стоит перед семафорами
        намеренно: ожидающая задача не должна держать слот конкурентности и
        мешать запросам к другим доменам.

        robots.txt спрашивается один раз, вне цикла повторов: его решение от
        попытки к попытке не меняется.

        `user_agent` переопределяет агента для этого запроса (иначе берётся
        следующий по ротации), а `robots_checked` говорит, что вызывающий уже
        спросил разрешения — чтобы одна страница не считалась в статистике
        robots.txt дважды.
        """
        self._get_session()  # «краулер закрыт» видно до первой попытки
        agent = user_agent or self._next_user_agent()

        if self.respect_robots:
            if not robots_checked:
                allowed, rule = await self.robots.allows(url, agent)
                if not allowed:
                    return self._blocked(url, rule, agent)
            # Правила домена уже в кэше, осталось учесть Crawl-delay из них.
            self._apply_crawl_delay(url, agent)

        outcome = RetryOutcome()
        try:
            result = await self.retry_strategy.execute_with_retry(
                self._fetch_attempt,
                url,
                agent,
                label=url,
                attempt_kwarg="attempt",
                outcome=outcome,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            return self._failure(url, from_exception(exc), agent, outcome)

        result.attempts = outcome.attempts
        result.retry_waited = outcome.waited
        return result

    async def _fetch_attempt(
        self, url: str, agent: str, *, attempt: int = 1
    ) -> FetchResult:
        """Одна попытка загрузки: результат или типизированная ошибка.

        В отличие от `fetch_one`, поднимает исключения из
        [errors.py](errors.py): решение о повторе принимает `RetryStrategy`, и
        ей нужен класс ошибки, а не поле в результате.
        """
        session = self._get_session()
        if self.circuit is not None and not self.circuit.allow(url):
            # Домен отключён: сеть не трогаем вовсе, повторять тоже нечего.
            raise CircuitOpenError(
                "домен отключён circuit breaker'ом "
                f"(состояние {self.circuit.state_of(url)})"
            )

        waited = await self.rate_limiter.acquire(url)
        timeout = self.timeout_for_attempt(attempt)

        async with self.semaphores.acquire(url):
            # Время измеряется после семафора: ожидание в очереди — не время запроса.
            started = time.perf_counter()
            logger.info("Загрузка начата: %s (попытка %d)", url, attempt)
            try:
                async with session.get(
                    url,
                    raise_for_status=True,
                    headers={"User-Agent": agent},
                    timeout=timeout,
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
                    if self.circuit is not None:
                        self.circuit.record_success(url)
                    return FetchResult(
                        url=url,
                        status=response.status,
                        text=text,
                        elapsed=elapsed,
                        truncated=truncated,
                        final_url=str(response.url),
                        waited=waited,
                        user_agent=agent,
                        attempts=attempt,
                        content_type=response.content_type,
                        fetched_at=datetime.now(),
                    )
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # защита от падения всего запуска
                error = from_exception(exc)
                error.attempts = attempt
                error.elapsed = time.perf_counter() - started
                error.waited = waited
                logger.warning(
                    "Попытка %d не удалась: %s [%s] %s (%.2f с)",
                    attempt,
                    url,
                    error.error_type,
                    error,
                    error.elapsed,
                )
                if self.circuit is not None:
                    self.circuit.record_failure(url, error)
                if error is exc:
                    raise
                raise error from exc

    def timeout_for_attempt(self, attempt: int = 1) -> aiohttp.ClientTimeout:
        """Таймауты попытки: с каждым повтором они растут.

        Медленный сервер чаще перегружен, чем мёртв, и повтор с тем же
        таймаутом упал бы по той же причине. Потолок `timeout_max_total`
        не даёт одному URL висеть минутами, но и урезать исходный таймаут не
        может: иначе у краулера с `timeout_total` больше потолка повтор получал
        бы меньше времени, чем первая попытка.
        """
        if attempt <= 1 or self.timeout_growth <= 1.0:
            return self._timeout

        growth = self.timeout_growth ** (attempt - 1)

        def scale(value: float | None) -> float | None:
            if not value:
                return value
            return max(value, min(value * growth, self.timeout_max_total))

        return aiohttp.ClientTimeout(
            total=scale(self.timeout_total),
            connect=scale(self.timeout_connect),
            sock_read=scale(self.timeout_read),
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
            url=url,
            error=reason,
            error_type=ERROR_ROBOTS,
            # Класс нужен, чтобы сознательный отказ не попал в отчёт
            # «неизвестной ошибкой» рядом с настоящими сбоями.
            error_class=RobotsBlockedError.__name__,
            user_agent=user_agent,
        )

    async def _fetch_robots_txt(self, robots_url: str) -> RobotsResponse:
        """Загружает robots.txt для `RobotsParser`."""
        status, text, error = await self._fetch_plain(
            robots_url, ROBOTS_MAX_BYTES, "robots.txt"
        )
        return RobotsResponse(status=status, text=text, error=error)

    async def _fetch_sitemap(self, sitemap_url: str) -> SitemapResponse:
        """Загружает карту сайта для `SitemapParser`."""
        status, text, error = await self._fetch_plain(
            sitemap_url, SITEMAP_MAX_BYTES, "карта сайта", gunzip=True
        )
        return SitemapResponse(status=status, text=text, error=error)

    async def _fetch_plain(
        self, url: str, max_bytes: int, what: str, *, gunzip: bool = False
    ) -> tuple[int | None, str, str | None]:
        """Простая загрузка служебного файла: статус, текст и ошибка.

        Служебные файлы (robots.txt, карты сайта) идут мимо проверки robots.txt
        — иначе для robots.txt получилась бы рекурсия, — но через лимит
        скорости и семафоры: это такие же запросы к сайту. Повторов и разбора
        ошибок тут нет: решение, что делать с недоступным файлом, принимает тот,
        кто его просил.

        `gunzip` распаковывает тело, если оно сжато: `sitemap.xml.gz` разрешён
        спецификацией, и крупные сайты отдают карту только так. Сжатие
        проверяется по самим байтам, а не по имени файла или заголовку:
        встречается и `.gz` без заголовка, и заголовок без `.gz`.
        """
        session = self._get_session()
        await self.rate_limiter.acquire(url)

        async with self.semaphores.acquire(url):
            try:
                async with session.get(
                    url, headers={"User-Agent": self.user_agent}
                ) as response:
                    if response.status >= 400:
                        return response.status, "", None

                    chunks: list[bytes] = []
                    size = 0
                    async for chunk in response.content.iter_chunked(CHUNK_SIZE):
                        chunks.append(chunk)
                        size += len(chunk)
                        if size >= max_bytes:
                            logger.warning("%s обрезан: %s", what, url)
                            break
                    body = b"".join(chunks)[:max_bytes]
                    if gunzip and body[:2] == GZIP_MAGIC:
                        try:
                            with gzip.GzipFile(fileobj=io.BytesIO(body)) as packed:
                                # Читаем ровно до лимита, а не целиком:
                                # повторяющийся XML сжимается в сотни раз, и
                                # разрешённые 10 МБ архива развернулись бы в
                                # гигабайты в памяти воркера.
                                body = packed.read(max_bytes)
                        except (OSError, EOFError) as exc:
                            # Обычно это обрезанный по max_bytes архив: часть
                            # данных есть, но распаковать её нечем.
                            return response.status, "", f"не распаковался: {exc}"
                        if len(body) >= max_bytes:
                            logger.warning("%s обрезан при распаковке: %s", what, url)
                    return (
                        response.status,
                        self._decode_body(body, response, url),
                        None,
                    )
            except asyncio.CancelledError:
                raise
            except asyncio.TimeoutError:
                return None, "", "превышен таймаут"
            except aiohttp.ClientError as exc:
                return None, "", f"{type(exc).__name__}: {exc}"

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
        error: CrawlerError,
        user_agent: str | None,
        outcome: RetryOutcome,
    ) -> FetchResult:
        """Итоговый результат URL, который загрузить не удалось.

        Штраф лимитеру ставится здесь, один раз на URL, а не на каждой попытке:
        паузы между попытками — дело `RetryStrategy`, и двойное наказание
        разогнало бы задержку домена в геометрической прогрессии.
        """
        logger.warning(
            "Загрузка не удалась: %s [%s] %s (попыток %d, ожидание повторов %.2f с)",
            url,
            error.error_class,
            error,
            outcome.attempts,
            outcome.waited,
        )
        if self._overloaded(error):
            self.rate_limiter.penalize(url)
        return FetchResult(
            url=url,
            status=error.status,
            elapsed=error.elapsed,
            error=error.message,
            error_type=error.error_type,
            error_class=error.error_class,
            waited=error.waited,
            user_agent=user_agent,
            attempts=outcome.attempts or error.attempts,
            retry_waited=outcome.waited,
            fetched_at=datetime.now(),
        )

    @staticmethod
    def _overloaded(error: CrawlerError) -> bool:
        """Похожа ли ошибка на перегрузку сайта, а не на нашу собственную.

        Замедляться стоит от таймаутов, сетевых сбоев, 5xx и явных «слишком
        много запросов» — всё это временные ошибки. Обычный 404 к скорости
        отношения не имеет: из-за битых ссылок обход тормозил бы без причины, а
        отказ circuit breaker'а и вовсе не был запросом.
        """
        if isinstance(error, (TransientError, NetworkError)):
            return True
        return error.error_type == ERROR_UNKNOWN

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
        error = from_exception(exc)
        return FetchResult(
            url=url,
            status=error.status,
            error=error.message,
            error_type=error.error_type,
            error_class=error.error_class,
            attempts=error.attempts,
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
        depth: int = 0,
    ) -> tuple[FetchResult, dict[str, Any]]:
        """Возвращает и `FetchResult` (для статистики), и разобранную страницу.

        Здесь же страница уходит в хранилище: через этот метод проходят все
        разобранные страницы — и обход, и `fetch_and_parse_many`, — поэтому один
        хук гарантирует, что каждая сохранится ровно один раз. Глубина нужна
        именно поэтому: записать её в страницу позже, в воркере, было бы уже
        поздно.
        """
        result = await self.fetch_one(
            url, user_agent=user_agent, robots_checked=robots_checked
        )

        page = empty_page(url)
        if result.ok and result.text is not None:
            try:
                # База для относительных ссылок — URL после редиректов.
                page = await self.parser.parse_html(
                    result.text, result.final_url or url
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                # Парсер сам кривую разметку переживает и отдаёт частичный
                # результат; сюда попадает только отказ самого разбора. Повтор
                # не поможет: второй раз придёт тот же HTML.
                self._parse_failed(result, ParseError(f"{type(exc).__name__}: {exc}"))
                page = empty_page(url)

        page = self._merge_result(page, result)
        page["depth"] = depth
        await self._store(page)
        return result, page

    async def _unexpected_page(
        self, url: str, exc: BaseException, depth: int = 0
    ) -> tuple[FetchResult, dict[str, Any]]:
        """Результат и страница для сбоя, которого мы не предусмотрели.

        Сюда попадает только то, что пробилось мимо всей обработки ошибок, —
        такая страница тоже должна выглядеть как обычная неудачная и дойти до
        хранилища, если неудачные просили сохранять.
        """
        result = self._unexpected(url, exc)
        page = self._merge_result(empty_page(url), result)
        page["depth"] = depth
        await self._store(page)
        return result, page

    async def _store(self, page: dict[str, Any]) -> None:
        """Отдаёт страницу хранилищу, если оно подключено.

        Исключений не ждём и не ловим: `DataStorage.save` сам переживает свои
        сбои, пишет их в лог и считает — обход из-за заполненного диска
        останавливаться не должен.
        """
        if self.storage is None:
            return
        if not page.get("ok") and not self.storage_save_failed:
            return
        await self.storage.save(page)

    @staticmethod
    def _parse_failed(result: FetchResult, error: ParseError) -> None:
        """Переводит загруженный результат в ошибку разбора, сохранив статус."""
        logger.warning("Не удалось разобрать страницу %s: %s", result.url, error)
        result.text = None
        result.error = error.message
        result.error_type = error.error_type
        result.error_class = error.error_class

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
            error_class=result.error_class,
            waited=result.waited,
            user_agent=result.user_agent,
            attempts=result.attempts,
            retry_waited=result.retry_waited,
            content_type=result.content_type,
            fetched_at=result.fetched_at,
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
                result, page = await self._unexpected_page(url, item)
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

    async def discover_sitemaps(self, url: str) -> list[str]:
        """Где у сайта карта: из robots.txt, иначе по стандартному адресу.

        robots.txt загружается, даже когда `respect_robots` выключен: мы
        спрашиваем не разрешение, а адрес карты, и другого стандартного способа
        его узнать нет.
        """
        found = await self.robots.sitemaps(url)
        if found:
            # Стандарт требует в директиве абсолютный адрес, но относительный
            # встречается — достроить его по адресу сайта дешевле, чем потерять
            # карту целиком. Так же поступает и разбор <loc> в самой карте.
            found = [urljoin(url, item) for item in found]
            logger.info("Карты сайта из robots.txt (%d): %s", len(found), found[0])
            return found

        fallback = urljoin(url, DEFAULT_SITEMAP_PATH)
        logger.info("В robots.txt карт нет, пробую %s", fallback)
        return [fallback]

    async def seeds_from_sitemaps(
        self,
        sitemap_urls: Iterable[str],
        *,
        base_urls: Iterable[str] = (),
        limit: int | None = None,
    ) -> list[str]:
        """Стартовые URL из карт сайта, самые важные — первыми.

        Вместо адреса карты можно передать `auto` — тогда карта ищется по
        `base_urls` через `discover_sitemaps`.

        Порядок задаёт `<priority>`: очередь обхода при равном приоритете
        работает как FIFO, поэтому отсортированный список сохранит свой порядок
        и менять сигнатуру `crawl()` не требуется.

        Счётчики карты сбрасываются здесь, а не в `crawl()`, как у остальных
        механизмов: карту читают до начала обхода, и сброс в `crawl()` стёр бы
        её цифры из отчёта. Зато второй прогон не покажет файлы первого.
        """
        self.sitemap.reset_stats()
        sources: list[str] = []
        for item in sitemap_urls:
            if item != SITEMAP_AUTO:
                sources.append(item)
                continue
            for base in base_urls:
                sources.extend(await self.discover_sitemaps(base))

        entries: list[SitemapEntry] = await self.sitemap.fetch_entries(
            dict.fromkeys(sources)  # без повторов, порядок сохранён
        )
        urls = [entry.loc for entry in sort_by_priority(entries)]
        if limit is not None and len(urls) > limit:
            logger.info("Из карты сайта взяты первые %d адресов из %d", limit, len(urls))
            urls = urls[:limit]
        return urls

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
        # Предел этого обхода нужен не воркерам, а статистике: без него
        # невозможно посчитать ни процент выполнения, ни остаток времени.
        self._max_pages = max_pages
        # Семафоры, лимитер и парсер robots.txt переиспользуются между обходами,
        # но их счётчики — нет: иначе второй обход показывал бы статистику
        # первого. Разобранные правила и шаг задержек при этом сохраняются.
        self.semaphores.reset_stats()
        self.rate_limiter.reset_stats()
        self.robots.reset_stats()
        self.retry_strategy.reset_stats()
        if self.circuit is not None:
            self.circuit.reset_stats()
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
                        url,
                        user_agent=user_agent,
                        robots_checked=self.respect_robots,
                        depth=depth,
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as exc:  # падение воркера не должно рвать обход
                    result, page = await self._unexpected_page(url, exc, depth)

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

    def _progress_estimate(self, stats: dict[str, Any]) -> dict[str, Any]:
        """Процент выполнения и оценка остатка времени.

        Остаток считается как `min(лимит страниц - сделано, в очереди + в
        работе)`: обход заканчивается по тому пределу, который наступит раньше,
        и честная оценка — меньшая из двух. Поэтому процент может расти
        рывками: когда ссылок находится меньше, чем позволяет лимит, работы
        внезапно остаётся меньше.
        """
        done = stats.get("processed", 0) + stats.get("failed", 0)
        by_limit = max(self._max_pages - done, 0) if self._max_pages else None
        by_queue = stats.get("pending", 0) + stats.get("in_progress", 0)
        remaining = by_queue if by_limit is None else min(by_limit, by_queue)

        total = done + remaining
        rate = stats.get("pages_per_sec", 0.0)
        return {
            "max_pages": self._max_pages or None,
            "remaining": remaining,
            "percent": done / total * 100 if total else 100.0,
            # Без скорости оценивать нечего: в начале обхода её ещё нет.
            "eta": remaining / rate if rate > 0 and remaining else 0.0,
        }

    def get_crawl_stats(self) -> dict[str, Any]:
        """Сводная статистика обхода: очередь, семафоры, вежливость, ошибки."""
        stats = self.queue.get_stats()
        stats.update(
            **self._progress_estimate(stats),
            visited=len(self.visited_urls),
            pages_started=self._pages_started,
            skipped=dict(self.skipped),
            skipped_total=sum(self.skipped.values()),
            semaphores=self.semaphores.get_stats(),
            rate_limit=self.rate_limiter.get_stats(),
            robots={**self.robots.get_stats(), "enabled": self.respect_robots},
            blocked=len(self.blocked_urls),
            retry=self.retry_strategy.get_stats(),
            circuit=(
                self.circuit.get_stats()
                if self.circuit is not None
                else {"enabled": False}
            ),
            storage=(
                {**self.storage.get_stats(), "enabled": True}
                if self.storage is not None
                else {"enabled": False}
            ),
            # Карты сайта попадают в статистику только если их читали: обход по
            # ссылкам ничего о них не знает.
            sitemap={
                **self.sitemap.get_stats(),
                "enabled": bool(self.sitemap.files or self.sitemap.errors),
            },
            errors=self.get_error_report(),
        )
        return stats

    def get_error_report(self, *, max_urls: int = 50) -> dict[str, Any]:
        """Отчёт об ошибках последнего запуска: типы, повторы, постоянные URL.

        Считается из `self.results`, без отдельного состояния: результаты и так
        хранят всё нужное — класс ошибки, число попыток и ожидание повторов.
        """
        by_type: Counter[str] = Counter()
        by_class: Counter[str] = Counter()
        permanent: list[dict[str, Any]] = []
        retried = 0
        retry_waited = 0.0
        failed = 0

        for result in self.results:
            if result.attempts > 1:
                retried += 1
                retry_waited += result.retry_waited
            if result.ok:
                continue
            failed += 1
            by_type[result.error_type or ERROR_UNKNOWN] += 1
            by_class[result.error_class or ERROR_UNKNOWN] += 1
            if result.error_class in PERMANENT_CLASSES:
                permanent.append(
                    {
                        "url": result.url,
                        "status": result.status,
                        "error": result.error,
                        "error_class": result.error_class,
                        "attempts": result.attempts,
                    }
                )

        retry_stats = self.retry_strategy.get_stats()
        return {
            "total": len(self.results),
            "failed": failed,
            "by_type": dict(by_type),
            "by_class": dict(by_class),
            # URL, которым потребовался хотя бы один повтор, и сколько времени
            # эти паузы заняли.
            "retried_urls": retried,
            "retry_waited": retry_waited,
            "retry_success": retry_stats["retry_success"],
            "avg_retry_wait": retry_stats["avg_wait"],
            "permanent_count": len(permanent),
            "permanent_urls": permanent[:max_urls],
        }

    async def close(self) -> None:
        """Закрывает хранилище и сессию; повторный вызов безопасен."""
        self._closed = True
        if self.storage is not None:
            # Сначала данные: в буфере хранилища могут лежать последние
            # страницы, и терять их из-за порядка закрытия нельзя. Если
            # закрывать хранилище не просили, буфер всё равно досылается.
            if self.close_storage:
                await self.storage.close()
            else:
                await self.storage.flush()
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
