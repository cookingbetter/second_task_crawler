"""Тесты повторов и обработки ошибок в краулере (День 5)."""

from __future__ import annotations

from typing import Any

import pytest

from src.circuit import CircuitBreaker
from src.crawler import AsyncCrawler
from src.errors import (
    CircuitOpenError,
    CrawlerTimeoutError,
    NetworkError,
    ParseError,
    PermanentError,
    RobotsBlockedError,
    ServerError,
)
from src.models import ERROR_CIRCUIT, ERROR_PARSE, ERROR_ROBOTS, ERROR_TIMEOUT
from src.parser import HTMLParser
from src.retry import RetryStrategy
from src.utils import format_results_table

UNREACHABLE_URL = "http://this-domain-does-not-exist-12345.invalid/"


class Clock:
    """Часы для circuit breaker: восстановление в тесте не ждут по-настоящему."""

    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class BrokenParser(HTMLParser):
    """Парсер, который не справляется: так проверяется ошибка разбора."""

    async def parse_html(self, html: str, url: str) -> dict[str, Any]:
        raise RuntimeError("дерево не построилось")


def fast_retries(**kwargs) -> dict[str, Any]:
    """Настройки повторов без настоящих пауз: тесты проверяют логику, не время."""
    kwargs.setdefault("max_retries", 2)
    kwargs.setdefault("retry_base_delay", 0.0)
    kwargs.setdefault("retry_max_delay", 0.0)
    return kwargs


# --- временные ошибки повторяются ------------------------------------------


async def test_transient_error_is_retried_until_success(url, flaky_hits):
    target = url("/flaky?key=t1&fail=2&status=503")

    # max_retries 3 даёт 5xx два повтора: своя политика у них на один меньше.
    async with AsyncCrawler(**fast_retries(max_retries=3)) as crawler:
        result = await crawler.fetch_one(target)

    assert result.ok
    assert result.attempts == 3
    assert flaky_hits["t1"] == 3
    assert crawler.retry_strategy.get_stats()["retry_success"] == 1


async def test_rate_limit_status_is_retried(url, flaky_hits):
    target = url("/flaky?key=t2&fail=1&status=429&retry_after=0")

    async with AsyncCrawler(**fast_retries()) as crawler:
        result = await crawler.fetch_one(target)

    assert result.ok
    assert result.attempts == 2
    assert crawler.retry_strategy.get_stats()["by_class"] == {"RateLimitError": 1}


async def test_timeout_is_retried(url, flaky_hits):
    """Первая попытка не успевает, вторая получает ответ сразу."""
    target = url("/flaky?key=t3&fail=1&status=0&delay=1.0")

    async with AsyncCrawler(
        timeout_total=5.0, timeout_read=0.3, **fast_retries(max_retries=1)
    ) as crawler:
        result = await crawler.fetch_one(target)

    assert result.ok
    assert result.attempts == 2
    assert crawler.retry_strategy.get_stats()["by_error_type"] == {ERROR_TIMEOUT: 1}


async def test_retries_run_out_and_the_error_stays(url, flaky_hits):
    target = url("/flaky?key=t4&fail=10&status=503")

    async with AsyncCrawler(**fast_retries()) as crawler:
        result = await crawler.fetch_one(target)

    assert result.ok is False
    assert result.error_class == ServerError.__name__
    assert result.status == 503
    # 5xx повторяется на один раз меньше обычного: max_retries 2 -> 1 повтор.
    assert result.attempts == 2
    assert flaky_hits["t4"] == 2
    assert crawler.retry_strategy.get_stats()["exhausted"] == 1


async def test_network_error_is_retried():
    async with AsyncCrawler(**fast_retries()) as crawler:
        result = await crawler.fetch_one(UNREACHABLE_URL)

    assert result.ok is False
    assert result.error_class == NetworkError.__name__
    assert result.attempts == 3


# --- постоянные ошибки не повторяются --------------------------------------


@pytest.mark.parametrize("code", [404, 403, 401])
async def test_permanent_statuses_are_not_retried(url, code, hits):
    async with AsyncCrawler(**fast_retries()) as crawler:
        result = await crawler.fetch_one(url(f"/status/{code}"))

    assert result.ok is False
    assert result.attempts == 1
    assert result.error_class == PermanentError.__name__
    assert crawler.retry_strategy.get_stats()["not_retried"] == 1


async def test_retries_can_be_switched_off(url, flaky_hits):
    target = url("/flaky?key=t5&fail=1&status=503")

    async with AsyncCrawler(max_retries=0) as crawler:
        result = await crawler.fetch_one(target)

    assert result.ok is False
    assert result.attempts == 1
    assert flaky_hits["t5"] == 1
    assert crawler.retry_strategy.enabled is False


# --- таймауты --------------------------------------------------------------


def test_timeout_grows_with_attempts_and_stops_at_the_cap():
    crawler = AsyncCrawler(
        timeout_total=10.0,
        timeout_connect=4.0,
        timeout_read=6.0,
        timeout_growth=2.0,
        timeout_max_total=20.0,
    )

    first = crawler.timeout_for_attempt(1)
    second = crawler.timeout_for_attempt(2)
    far = crawler.timeout_for_attempt(5)

    assert (first.total, first.connect, first.sock_read) == (10.0, 4.0, 6.0)
    assert (second.total, second.connect, second.sock_read) == (20.0, 8.0, 12.0)
    assert far.total == 20.0  # потолок не даёт расти бесконечно


def test_timeouts_stay_flat_without_growth():
    crawler = AsyncCrawler(timeout_total=10.0, timeout_growth=1.0)

    assert crawler.timeout_for_attempt(3) is crawler._timeout


def test_timeout_never_shrinks_on_retry():
    """Потолок ограничивает рост, но не может урезать исходный таймаут."""
    crawler = AsyncCrawler(
        timeout_total=120.0, timeout_read=60.0, timeout_max_total=60.0
    )

    second = crawler.timeout_for_attempt(2)

    assert second.total == 120.0
    assert second.sock_read == 60.0


# --- взаимодействие с лимитером --------------------------------------------


async def test_domain_is_penalized_once_per_url(url):
    """Паузы между попытками — дело повторов, штраф домену ставится один раз."""
    async with AsyncCrawler(max_backoff=1.0, **fast_retries()) as crawler:
        await crawler.fetch_one(url("/status/503"))

    assert crawler.rate_limiter.get_stats()["penalties"] == 1
    assert crawler.rate_limiter.backoff_for(url("/")) > 0


async def test_successful_retry_does_not_penalize(url, flaky_hits):
    async with AsyncCrawler(**fast_retries()) as crawler:
        result = await crawler.fetch_one(url("/flaky?key=t6&fail=1&status=503"))

    assert result.ok
    assert crawler.rate_limiter.get_stats()["penalties"] == 0


# --- ошибки разбора --------------------------------------------------------


async def test_parse_failure_is_permanent(url):
    async with AsyncCrawler(parser=BrokenParser(), **fast_retries()) as crawler:
        page = await crawler.fetch_and_parse(url("/page"))

    assert page["ok"] is False
    assert page["error_type"] == ERROR_PARSE
    assert page["error_class"] == ParseError.__name__
    assert page["status"] == 200  # загрузка-то прошла
    assert page["attempts"] == 1  # второй разбор того же HTML не поможет


# --- circuit breaker -------------------------------------------------------


async def test_breaker_stops_requests_to_a_broken_domain(url, hits):
    async with AsyncCrawler(
        max_retries=0, circuit_breaker=True, circuit_threshold=2
    ) as crawler:
        await crawler.fetch_one(url("/status/500"))
        await crawler.fetch_one(url("/status/500"))
        refused = await crawler.fetch_one(url("/counted"))

    assert refused.ok is False
    assert refused.error_type == ERROR_CIRCUIT
    assert refused.error_class == CircuitOpenError.__name__
    assert hits["/counted"] == 0  # запроса в сеть не было
    assert crawler.circuit.get_stats()["refused"] == 1


async def test_breaker_lets_the_domain_back_after_recovery(url, hits):
    clock = Clock()
    breaker = CircuitBreaker(
        failure_threshold=2, window=60.0, recovery_time=30.0, clock=clock
    )

    async with AsyncCrawler(max_retries=0, breaker=breaker) as crawler:
        await crawler.fetch_one(url("/status/500"))
        await crawler.fetch_one(url("/status/500"))
        assert (await crawler.fetch_one(url("/counted"))).ok is False

        clock.advance(31.0)
        recovered = await crawler.fetch_one(url("/counted"))

    assert recovered.ok
    assert hits["/counted"] == 1
    assert breaker.get_stats()["recoveries"] == 1


async def test_breaker_ignores_broken_links(url):
    """Сайт с битыми ссылками работает: 404 не должен его отключать."""
    async with AsyncCrawler(
        max_retries=0, circuit_breaker=True, circuit_threshold=2
    ) as crawler:
        for _ in range(4):
            await crawler.fetch_one(url("/status/404"))
        result = await crawler.fetch_one(url("/ok"))

    assert result.ok
    assert crawler.circuit.get_stats()["trips"] == 0


async def test_breaker_is_off_by_default():
    crawler = AsyncCrawler()

    assert crawler.circuit is None
    assert crawler.get_crawl_stats()["circuit"] == {"enabled": False}


# --- отчёт об ошибках ------------------------------------------------------


async def test_error_report_sums_up_the_batch(url, flaky_hits):
    urls = [
        url("/ok"),
        url("/status/404"),
        url("/flaky?key=t7&fail=1&status=503"),
    ]

    async with AsyncCrawler(max_concurrent=3, **fast_retries()) as crawler:
        await crawler.fetch_urls(urls)
        report = crawler.get_error_report()

    assert report["total"] == 3
    assert report["failed"] == 1
    assert report["by_class"] == {PermanentError.__name__: 1}
    assert report["retried_urls"] == 1
    assert report["retry_success"] == 1
    assert report["permanent_count"] == 1
    assert report["permanent_urls"][0]["url"] == url("/status/404")


async def test_crawl_stats_include_retries_and_errors(url):
    async with AsyncCrawler(max_concurrent=2, max_depth=1, **fast_retries()) as crawler:
        await crawler.crawl([url("/site/")], max_pages=50, same_domain_only=True)
        stats = crawler.get_crawl_stats()

    missing = url("/site/missing")
    assert stats["retry"]["enabled"] is True
    assert stats["errors"]["failed"] == 1
    assert stats["errors"]["permanent_count"] == 1
    assert stats["errors"]["permanent_urls"][0]["url"] == missing
    assert crawler.failed_urls[missing]


async def test_attempts_column_appears_only_with_retries(url, flaky_hits):
    async with AsyncCrawler(**fast_retries()) as crawler:
        await crawler.fetch_urls([url("/ok")])
        without = format_results_table(crawler.results)

        await crawler.fetch_urls([url("/flaky?key=t8&fail=1&status=503")])
        with_retries = format_results_table(crawler.results)

    assert "Попыток" not in without
    assert "Попыток" in with_retries


async def test_robots_refusal_keeps_its_own_class(url, robots_txt):
    """Запрет robots.txt — решение краулера, а не неизвестный сбой."""
    robots_txt("User-agent: *\nDisallow: /site/a1\n")

    async with AsyncCrawler(respect_robots=True, **fast_retries()) as crawler:
        await crawler.fetch_urls([url("/site/a1")])
        report = crawler.get_error_report()
        result = crawler.results[0]

    assert result.ok is False
    assert result.error_type == ERROR_ROBOTS
    assert result.error_class == RobotsBlockedError.__name__
    assert report["by_class"] == {RobotsBlockedError.__name__: 1}


@pytest.mark.parametrize(
    "ignored",
    [
        {"max_retries": 1},
        {"retry_backoff_factor": 3.0},
        {"retry_max_delay": 1.0},
        {"retry_jitter": 0.5},
    ],
)
async def test_custom_retry_strategy_warns_about_ignored_scalars(caplog, ignored):
    with caplog.at_level("WARNING"):
        crawler = AsyncCrawler(retry_strategy=RetryStrategy(max_retries=5), **ignored)

    assert crawler.retry_strategy.max_retries == 5
    assert "RetryStrategy" in caplog.text
    assert next(iter(ignored)) in caplog.text


async def test_custom_breaker_warns_about_ignored_scalars(caplog):
    breaker = CircuitBreaker(failure_threshold=2)

    with caplog.at_level("WARNING"):
        crawler = AsyncCrawler(breaker=breaker, circuit_recovery=1.0)

    assert crawler.circuit is breaker
    assert "CircuitBreaker" in caplog.text
    assert "circuit_recovery" in caplog.text


async def test_default_settings_warn_about_nothing(caplog):
    """Предупреждение не должно срабатывать на значения по умолчанию."""
    with caplog.at_level("WARNING"):
        AsyncCrawler(retry_strategy=RetryStrategy(), breaker=CircuitBreaker())

    assert "не применяются" not in caplog.text


async def test_timeout_error_class_is_reported(url):
    async with AsyncCrawler(
        timeout_total=5.0, timeout_read=0.2, **fast_retries(max_retries=0)
    ) as crawler:
        result = await crawler.fetch_one(url("/slow?delay=1.0"))

    assert result.ok is False
    assert result.error_class == CrawlerTimeoutError.__name__
    assert result.error_type == ERROR_TIMEOUT
